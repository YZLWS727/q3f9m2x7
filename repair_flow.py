# -*- coding: utf-8 -*-
"""8/19 智谱成品 360 条“AI解析超时暂存”定向补打（硅基流动主打 + DeepSeek 兜底）。

流程：S3 GET 成品 → 解析暂存条目 → SiliconFlow Qwen3-8B 补打（a3 FINAL 提示词）
      → 缺失/不合格条目 DeepSeek deepseek-v4-flash 逐条兜底 → 原地回写
      → 本地校验 → S3 PUT + 回读 SHA256 校验。
环境变量：SILICONFLOW_API_KEY / DEEPSEEK_API_KEY / S3_ENDPOINT / S3_BUCKET / S3_ACCESS_KEY / S3_SECRET_KEY
用法：python repair_flow.py --file "20260819周三全网宏观信息流-智谱.md" [--out-dir ./output] [--dry-run]
"""
import hashlib
import hmac
import json
import os
import random
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from repair_tags import TAGS_DEF, VALID_TAGS

SF_BASE = "https://api.siliconflow.cn/v1"
SF_MODEL = "Qwen/Qwen3-8B"
DS_BASE = "https://api.deepseek.com/chat/completions"
DS_MODEL = "deepseek-v4-flash"
BATCH_SIZE = 10
CALL_TIMEOUT = 180
MAX_BATCH_RETRY = 6
MAX_ITEM_RETRY = 3
PLACEHOLDER = "AI解析超时暂存"


def log(*args):
    print(*args, flush=True)


# ---------------- S3 SigV4 ----------------

def _sign(key, msg):
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def _s3_request(method, path, body=None, content_type=None):
    endpoint = os.environ["S3_ENDPOINT"].rstrip("/")
    ak = os.environ["S3_ACCESS_KEY"].strip()
    sk = os.environ["S3_SECRET_KEY"].strip()
    region = os.environ.get("S3_REGION", "cn-north-1")
    host = urllib.parse.urlparse(endpoint).netloc
    payload = body or b""
    payload_hash = hashlib.sha256(payload).hexdigest()
    now = datetime.now(timezone.utc)
    amzdate = now.strftime("%Y%m%dT%H%M%SZ")
    datestamp = now.strftime("%Y%m%d")
    headers = {
        "host": host,
        "x-amz-content-sha256": payload_hash,
        "x-amz-date": amzdate,
    }
    if content_type:
        headers["content-type"] = content_type
    canonical_headers = "".join(f"{k}:{v}\n" for k, v in headers.items())
    signed_headers = ";".join(headers.keys())
    canonical_request = "\n".join([method, path, "", canonical_headers, signed_headers, payload_hash])
    scope = f"{datestamp}/{region}/s3/aws4_request"
    sts = "\n".join(["AWS4-HMAC-SHA256", amzdate, scope, hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()])
    kd = _sign(("AWS4" + sk).encode(), datestamp)
    kr = _sign(kd, region)
    ks = _sign(kr, "s3")
    ksg = _sign(ks, "aws4_request")
    sig = hmac.new(ksg, sts.encode("utf-8"), hashlib.sha256).hexdigest()
    auth = f"AWS4-HMAC-SHA256 Credential={ak}/{scope}, SignedHeaders={signed_headers}, Signature={sig}"
    req = urllib.request.Request(endpoint + path, data=payload, method=method)
    req.add_header("Authorization", auth)
    for k, v in headers.items():
        req.add_header(k, v)
    req.add_header("User-Agent", "rclone/v1.68.0")
    last = None
    for attempt in range(1, 4):
        try:
            with urllib.request.urlopen(req, timeout=240) as r:
                return r.status, r.read()
        except Exception as e:
            last = repr(e)
            time.sleep(3 * attempt)
    raise RuntimeError("S3 request failed after 3 attempts: %s" % last)


def s3_key_path(key):
    return "/" + os.environ["S3_BUCKET"] + "/" + urllib.parse.quote(key, safe="/")


def s3_get(key):
    st, body = _s3_request("GET", s3_key_path(key))
    if st != 200:
        raise RuntimeError("S3 GET %d" % st)
    return body


def s3_put(key, body):
    st, _ = _s3_request("PUT", s3_key_path(key), body=body, content_type="text/markdown; charset=utf-8")
    if st not in (200, 201):
        raise RuntimeError("S3 PUT %d" % st)


# ---------------- MD 解析 ----------------

def parse_md(text):
    lines = text.splitlines()
    entries = []
    i = 0
    while i < len(lines):
        m = re.match(r"^### (.+)$", lines[i])
        if not m:
            i += 1
            continue
        title = m.group(1).strip()
        start = i
        i += 1
        while i < len(lines) and not lines[i].strip():
            i += 1
        meta_idx = None
        if i < len(lines) and lines[i].startswith("`"):
            meta_idx = i
            i += 1
        while i < len(lines) and not lines[i].strip():
            i += 1
        tags_idx = None
        if i < len(lines) and lines[i].strip().startswith("#"):
            tags_idx = i
            i += 1
        content = []
        while i < len(lines) and not lines[i].strip().startswith("### ") and lines[i].strip() != "---":
            content.append(lines[i])
            i += 1
        entries.append(
            {
                "title": title,
                "block_start": start,
                "meta_idx": meta_idx,
                "tags_idx": tags_idx,
                "content": "\n".join(content).strip(),
            }
        )
    return entries


def extract_id(meta_line):
    m = re.search(r"\[id::\s*([^\]\s]+)", meta_line or "")
    return m.group(1) if m else None


def extract_time(meta_line):
    m = re.match(r"^`([^`]+)`", meta_line or "")
    return m.group(1).strip() if m else ""


def extract_source_tag(tags_line):
    tags = re.findall(r"#[\u4e00-\u9fffA-Za-z0-9_]+", tags_line or "")
    if tags:
        return tags[0]
    return "#新浪24H"


# ---------------- 提示词与模型调用 ----------------

def build_prompt(batch_data):
    tags_context = ", ".join(f"{tag}(释义:{desc})" for tag, desc in TAGS_DEF.items())
    system_prompt = f"""作为宏观金融与认知心理学专家，请分析 JSON 列表中的快讯并赋予标签。
【核心纪律：绝对白名单制】
你输出的所有标签，必须 100% 存在于下方的《预设标签库》中，一字不差。严禁自创标签。
若无吻合标签，强制且仅输出 ["#无法归类等待识别"]。

【多标签硬性纪律（最高优先级）】
1. 正常情况下每条新闻只输出最相关的 4~6 个标签，宁缺毋滥，不要堆砌弱相关标签；
2. 所有标签必须一字不差来自《预设标签库》，绝对禁止自创、改写、合并或新增任何白名单之外的标签；
3. 每条必须输出 4~6 个标签：即使最相关标签不足 4 个，也必须从《预设标签库》中选择中等相关但合理的标签补齐，严禁少于 4 个；
4. 标签上限 6 个，绝不超过；
5. 一条都找不到匹配时，只输出 ["#无法归类等待识别"]；
6. 输出前逐条自检：每条标签数必须在 4~6 之间，不足 4 个立即补足后再输出。

【情绪判定纪律】
sentiment 必须按新闻事实明确判断：
1. 有明确利空信号（重大自然灾害致灾/人员伤亡、地缘冲突升级、重大违约/暴跌/衰退/评级下调/出口下滑等）→ Negative；
2. 有明确利好信号（重大政策利好、显著增长/创新高/评级上调/突破/回升/获批等）→ Positive；
3. 仅当信息确实中性、好坏参半或证据不足时才用 Neutral；有方向性事实必须判断：市场上涨/下跌、政策支持/收紧、数据改善/恶化、评级上调/下调、冲突升级/缓和、灾害损失等 → 分别映射 Positive/Negative，不得因“没提涨跌”就默认中性；不因主体知名度或语气偏向。

《预设标签库》：
{tags_context}

summary_title 为 20~30 字的一句话标题，必须保留主体、关键数字（原文有数字时必须包含）与事件核心，禁止编造原文没有的事实。仅返回纯 JSON 数组，禁止任何 markdown 标记。格式示例：
[ {{"idx": 0, "summary_title": "国内商品期货开盘多数上涨 铂、沪银、乙二醇涨超4%", "tags": ["#美国", "#债务和债券市场", "#金融流动性", "#宏观政策"], "sentiment": "Neutral"}} ]

【输出前强制检查】
①每条标签数必须 4~6 个，不足 4 个立即从《预设标签库》补足；
②sentiment 必须为 Positive/Negative/Neutral 三选一，不得省略；
③summary_title 必须 20~30 字且不得编造原文没有的事实。"""
    user_payload = [{"idx": i, "text": item["content"]} for i, item in enumerate(batch_data)]
    return system_prompt, json.dumps(user_payload, ensure_ascii=False)


def call_provider(provider, api_key, batch_data):
    """provider: sf=硅基流动, ds=DeepSeek。返回 (result_dict, fatal, err_info)。"""
    sys_prompt, user_content = build_prompt(batch_data)
    if provider == "sf":
        url = SF_BASE + "/chat/completions"
        payload = {
            "model": SF_MODEL,
            "messages": [
                {"role": "system", "content": sys_prompt},
                {"role": "user", "content": user_content},
            ],
            "temperature": 0.0,
            "enable_thinking": False,
        }
    else:
        url = DS_BASE
        payload = {
            "model": DS_MODEL,
            "messages": [
                {"role": "system", "content": sys_prompt},
                {"role": "user", "content": user_content},
            ],
            "temperature": 0.0,
            "stream": False,
        }
    req = urllib.request.Request(
        url,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json", "Authorization": "Bearer " + api_key},
        method="POST",
    )
    last_err = None
    for attempt in range(1, MAX_BATCH_RETRY + 1):
        try:
            with urllib.request.urlopen(req, timeout=CALL_TIMEOUT) as r:
                raw = r.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", "replace")[:300]
            if e.code == 400 and ("20012" in body or "Model does not exist" in body):
                log("MODEL_GONE_ALERT", provider, body[:150])
                return None, True, body[:150]
            if e.code in (401, 403):
                return None, False, f"{provider} AUTH_FAILED HTTP {e.code} {body[:150]}"
            last_err = f"HTTP {e.code} {body[:150]}"
            time.sleep(min(60, 3 * (2 ** (attempt - 1))) + random.uniform(0, 3))
            continue
        except Exception as e:
            last_err = repr(e)
            time.sleep(min(60, 3 * (2 ** (attempt - 1))) + random.uniform(0, 3))
            continue
        try:
            content = json.loads(raw)["choices"][0]["message"]["content"]
            clean = content.replace("```json", "").replace("```", "").strip()
            arr = json.loads(clean)
        except Exception as e:
            last_err = "parse fail: " + repr(e)
            time.sleep(min(30, 3 * (2 ** (attempt - 1))))
            continue
        result = {}
        for r in arr if isinstance(arr, list) else []:
            idx = r.get("idx")
            if not isinstance(idx, int) or not (0 <= idx < len(batch_data)):
                continue
            real_id = batch_data[idx]["id"]
            raw_tags = r.get("tags", []) if isinstance(r.get("tags"), list) else []
            seen = set()
            tags = []
            for t in raw_tags:
                if t in VALID_TAGS and t not in seen:
                    seen.add(t)
                    tags.append(t)
            tags = tags[:6]
            if not tags:
                tags = ["#无法归类等待识别"]
            title = (r.get("summary_title") or "").strip().replace("\n", " ")
            sentiment = r.get("sentiment") if r.get("sentiment") in ("Positive", "Negative", "Neutral") else "Neutral"
            if not title or title == PLACEHOLDER:
                continue
            result[real_id] = {"summary_title": title, "tags": tags, "sentiment": sentiment, "_provider": provider}
        return result, False, None
    log("PROVIDER_FAILED", provider, last_err)
    return {}, False, last_err


def item_ok(r):
    return bool(r) and r.get("summary_title", "").strip() and r.get("sentiment") in ("Positive", "Negative", "Neutral")


def title_consistent(r, rich_text):
    """标题-正文一致性守卫：标题与正文至少共享一个内容 2-gram（防 idx 错位）。"""
    t = re.sub(r"\s+", "", (r or {}).get("summary_title", ""))
    if len(t) < 6:
        return True
    tb = {t[i : i + 2] for i in range(len(t) - 1)}
    tb = {b for b in tb if any(c.isalnum() or "\u4e00" <= c <= "\u9fff" for c in b)}
    body = re.sub(r"\s+", "", rich_text or "")
    return any(b in body for b in tb)


def process_batch(bi, batch, sf_key, ds_key):
    res, fatal, err = call_provider("sf", sf_key, batch)
    if fatal:
        with open("MODEL_GONE_ALERT.txt", "w", encoding="utf-8") as f:
            f.write("硅基流动模型不存在，修复中止\n")
        log("MODEL_GONE_ABORT")
        sys.exit(2)
    if err and err.startswith("sf AUTH_FAILED"):
        log("SF_AUTH_FAILED", err)
        sys.exit(2)
    merged = {}
    for it in batch:
        r = (res or {}).get(it["id"])
        if r and item_ok(r) and title_consistent(r, it["content"]):
            merged[it["id"]] = r
    missing = [it for it in batch if it["id"] not in merged]
    if missing:
        log("BATCH %d SF_SINGLE_RETRY n=%d" % (bi, len(missing)))
        for it in missing:
            for attempt in range(1, MAX_ITEM_RETRY + 1):
                r2, f2, e2 = call_provider("sf", sf_key, [it])
                if f2:
                    sys.exit(2)
                r3 = (r2 or {}).get(it["id"])
                if r3 and item_ok(r3) and title_consistent(r3, it["content"]):
                    merged[it["id"]] = r3
                    break
                time.sleep(2 * attempt)
    missing = [it for it in batch if it["id"] not in merged]
    if missing:
        log("BATCH %d DS_FALLBACK n=%d" % (bi, len(missing)))
        for it in missing:
            for attempt in range(1, MAX_ITEM_RETRY + 1):
                r4, f4, e4 = call_provider("ds", ds_key, [it])
                if f4:
                    log("DS_MODEL_GONE", e4)
                    sys.exit(2)
                r5 = (r4 or {}).get(it["id"])
                if r5 and item_ok(r5) and title_consistent(r5, it["content"]):
                    merged[it["id"]] = r5
                    break
                time.sleep(2 * attempt)
    failed = [it["id"] for it in batch if it["id"] not in merged]
    return merged, failed


def label_entries(items):
    ok = {}
    failed = []
    sf_key = os.environ.get("SILICONFLOW_API_KEY", "").strip()
    ds_key = os.environ.get("DEEPSEEK_API_KEY", "").strip()
    if not sf_key:
        log("NO_SILICONFLOW_API_KEY")
        sys.exit(2)
    if not ds_key:
        log("NO_DEEPSEEK_API_KEY")
        sys.exit(2)
    batches = [items[i : i + BATCH_SIZE] for i in range(0, len(items), BATCH_SIZE)]
    total = len(batches)
    for bi, batch in enumerate(batches, 1):
        merged, failed_ids = process_batch(bi, batch, sf_key, ds_key)
        ok.update(merged)
        failed.extend(failed_ids)
        for fid in failed_ids:
            log("ITEM_FAILED", fid)
        log("BATCH_PROGRESS %d/%d ok=%d fail=%d" % (bi, total, len(ok), len(failed)))
        time.sleep(1.0)
    return ok, failed


# ---------------- 回写 ----------------

def patch_md(text, items, results, failed_ids):
    lines = text.splitlines()
    for it in items:
        if it["id"] in failed_ids:
            continue
        r = results[it["id"]]
        lines[it["block_start"]] = "### " + r["summary_title"]
        if it["meta_idx"] is not None:
            lines[it["meta_idx"]] = re.sub(
                r"\[sentiment::\s*(?:Positive|Negative|Neutral)\]",
                "[sentiment:: " + r["sentiment"] + "]",
                lines[it["meta_idx"]],
            )
        if it["tags_idx"] is not None:
            src = extract_source_tag(lines[it["tags_idx"]])
            new_tags = [src] + [t for t in r["tags"] if t != src]
            lines[it["tags_idx"]] = " ".join(new_tags[:6])
    return "\n".join(lines)


def verify(text, expected_total):
    n_timeout = len(re.findall(r"^### AI解析超时暂存\s*$", text, flags=re.M))
    n_entries = len(re.findall(r"^### ", text, flags=re.M))
    ids = re.findall(r"\[id::\s*([^\]\s]+)", text)
    dup = len(ids) - len(set(ids))
    log("VERIFY placeholders=%d entries=%d dup_ids=%d" % (n_timeout, n_entries, dup))
    return n_timeout == 0 and n_entries == expected_total and dup == 0


# ---------------- 主流程 ----------------

def main():
    args = sys.argv[1:]
    target_file = "20260819周三全网宏观信息流-智谱.md"
    out_dir = "./output"
    dry_run = False
    local_src = None
    i = 0
    while i < len(args):
        if args[i] == "--file" and i + 1 < len(args):
            target_file = args[i + 1]
            i += 2
        elif args[i] == "--out-dir" and i + 1 < len(args):
            out_dir = args[i + 1]
            i += 2
        elif args[i] == "--dry-run":
            dry_run = True
            i += 1
        elif args[i] == "--local-src" and i + 1 < len(args):
            local_src = args[i + 1]
            i += 2
        else:
            i += 1

    if dry_run:
        src = local_src or r"D:\obsidian\OB\新浪24H信息流\20260819周三全网宏观信息流-智谱.md"
        text = open(src, encoding="utf-8").read()
    else:
        log("S3_GET", target_file)
        text = s3_get(target_file).decode("utf-8")

    entries = parse_md(text)
    timeouts = [e for e in entries if e["title"] == PLACEHOLDER]
    log("PARSE total=%d timeout=%d" % (len(entries), len(timeouts)))
    if not timeouts:
        log("NO_TIMEOUT_ENTRIES")
        return 0
    if dry_run:
        empty = sum(1 for e in timeouts if not e["content"])
        log("DRY_RUN ok; empty_content=%d" % empty)
        return 0

    items = []
    for e in timeouts:
        meta = text.splitlines()[e["meta_idx"]] if e["meta_idx"] is not None else ""
        tags_line = text.splitlines()[e["tags_idx"]] if e["tags_idx"] is not None else ""
        items.append(
            {
                "id": extract_id(meta),
                "time": extract_time(meta),
                "source_tag": extract_source_tag(tags_line),
                "content": e["content"],
                "block_start": e["block_start"],
                "meta_idx": e["meta_idx"],
                "tags_idx": e["tags_idx"],
            }
        )
    items = [it for it in items if it["id"]]
    log("TO_RELABEL=%d" % len(items))

    results, failed_ids = label_entries(items)
    log("LABEL_DONE ok=%d fail=%d" % (len(results), len(failed_ids)))

    new_text = patch_md(text, items, results, set(failed_ids))
    os.makedirs(out_dir, exist_ok=True)
    out_md = os.path.join(out_dir, target_file)
    with open(out_md, "w", encoding="utf-8", newline="\n") as f:
        f.write(new_text)

    report = {
        "target_file": target_file,
        "total_entries": len(entries),
        "timeout_entries": len(timeouts),
        "repaired": len(results),
        "failed": failed_ids,
        "entries": [],
    }
    for it in items:
        report["entries"].append(
            {
                "id": it["id"],
                "time": it["time"],
                "source_tag": it["source_tag"],
                "new_title": results.get(it["id"], {}).get("summary_title"),
                "new_tags": results.get(it["id"], {}).get("tags"),
                "new_sentiment": results.get(it["id"], {}).get("sentiment"),
                "provider": results.get(it["id"], {}).get("_provider"),
                "failed": it["id"] in failed_ids,
            }
        )
    with open(os.path.join(out_dir, "repair_report.json"), "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=1)

    if not verify(new_text, len(entries)):
        log("VERIFY_FAILED")
        return 3

    body = new_text.encode("utf-8")
    log("UPLOAD size=%d sha256=%s" % (len(body), hashlib.sha256(body).hexdigest()))
    s3_put(target_file, body)
    back = s3_get(target_file)
    ok_sha = hashlib.sha256(back).hexdigest() == hashlib.sha256(body).hexdigest()
    ok_size = len(back) == len(body)
    log("S3_BACK size=%d sha256=%s MATCH=%s" % (len(back), hashlib.sha256(back).hexdigest(), ok_sha and ok_size))
    if not (ok_sha and ok_size):
        return 3
    if failed_ids:
        log("HAS_FAILED_ENTRIES", failed_ids)
        return 3
    log("REPAIR_DONE all=%d repaired=%d" % (len(items), len(results)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
