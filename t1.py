# -*- coding: utf-8 -*-
"""t1: DeepSeek 标签专用兜底（每日 ≤300 条，思考模式，只改标签）
在 a3 成品落地后运行：读分歧清单 → 按标签 Jaccard 取前 100 → DeepSeek 只打标签
→ B 方案只改成品标签行 → 回传主桶 + 复核报告。标题/正文/情绪一律不动。
环境变量：DEEPSEEK_API_KEY / TARGET_DATE / RAW_PATH / OUT_DIR / S3_*（与 a3_label 相同）
          T1_SKIP_S3=1 可跳过 S3（本地冒烟用）
"""
import hashlib
import importlib.util
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("a3label", os.path.join(HERE, "a3_label.py"))
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

DS_BASE = "https://api.deepseek.com"
DS_MODEL = "deepseek-v4-flash"
CAP = 300
TAG_JACCARD_THRESHOLD = 0.3
BATCH_SIZE = 10
WORKERS = 3


def ds_call(sys_prompt, user_content, key, timeout=300):
    payload = {"model": DS_MODEL,
               "input": [{"role": "system", "content": sys_prompt},
                         {"role": "user", "content": user_content}],
               "max_output_tokens": 8192,
               "temperature": 0.0}
    last = None
    for attempt in range(1, 5):
        t0 = time.time()
        try:
            req = urllib.request.Request(DS_BASE + "/v1/responses", method="POST")
            req.add_header("Authorization", "Bearer " + key)
            req.add_header("Content-Type", "application/json")
            req.add_header("Accept", "application/json")
            with urllib.request.urlopen(req, data=json.dumps(payload).encode("utf-8"),
                                        timeout=timeout) as r:
                body = json.loads(r.read().decode("utf-8", "replace"))
            parts = []
            for item in body.get("output", []):
                if item.get("type") == "message":
                    for c in item.get("content", []):
                        if c.get("type") == "output_text":
                            parts.append(c.get("text", ""))
            text = "".join(parts)
            usage = body.get("usage", {})
            return text, usage, time.time() - t0
        except urllib.error.HTTPError as e:
            last = repr(e)
            if e.code in (429, 500, 502, 503, 504):
                time.sleep(min(45, 5 * (2 ** (attempt - 1))))
                continue
            return None, {}, time.time() - t0
        except Exception as e:
            last = repr(e)
            time.sleep(min(45, 5 * (2 ** (attempt - 1))))
    return None, {"err": last}, 0


def build_tag_prompt(batch):
    tag_list = "、".join(f"{tag}（{desc}）" if desc.strip() else tag
                         for tag, desc in mod.TAGS_DEF.items())
    system_prompt = f"""作为宏观金融与认知心理学专家，请分析 JSON 列表中的快讯并只输出【标签】。
【核心纪律：绝对白名单制】
你输出的所有标签，必须 100% 存在于下方的《预设标签库》中，一字不差。严禁自创标签。

【多标签硬性纪律（最高优先级）】
1. 优先输出最相关的 4~6 个标签，允许 3~6 个；宁缺毋滥，绝对禁止为了凑数添加弱相关或泛化标签；
2. 所有标签必须一字不差来自《预设标签库》，绝对禁止自创、改写、合并或新增任何白名单之外的标签；
3. 每条输出 3~6 个标签；只有 1~2 个明确相关标签时允许少于 3 个，绝不拿“金融流动性/宏观政策/行业动态/无法归类等待识别”等当万能填充；
4. 标签上限 6 个，绝不超过；
5. 一条都找不到匹配时，只输出 ["#无法归类等待识别"]；该标签必须独占（唯一），禁止与其他任何标签同时出现；
6. 输出前逐条自检：每条标签数必须在 3~6 之间（优先 4~6）；#无法归类等待识别 只能单独出现；#期权 仅当正文明确出现 期权/看涨期权/看跌期权/认购/认沽/行权/隐含波动率/到期/做市 等期权要素时才使用。

【对齐纪律（最高优先级）】
- 必须严格按输入顺序输出，idx 从 0 到 N-1，每个只用一次，禁止跳号、重复或错位；
- 逐条核对：第 idx 条的标签与 keys 只能依据第 idx 条快讯的正文，不得串到其他快讯；
- keys 输出 2~3 个支持标签判断的正文原文关键词或短语（必须能在该条正文中逐字找到），仅用于对齐校验，不写入成品。

《预设标签库》：
{tag_list}

仅返回纯 JSON 数组，禁止任何 markdown 标记。格式示例：
[ {{"idx": 0, "tags": ["#美国", "#债务和债券市场", "#金融流动性", "#宏观政策"], "keys": ["美联储", "国债"]}} ]

【输出前强制检查】
①每条标签数必须在 3~6 之间（优先 4~6）；不足时允许 1~2 个，但禁止为凑数添加弱相关、泛化或“无法归类等待识别”标签；
②#无法归类等待识别 仅当所有其他标签都不符合时作为唯一标签输出，禁止与其他标签共存；
③#期权 仅在正文明确出现期权要素时使用，禁止作为金融类填充标签；
④idx 必须覆盖 0~N-1 且不重复、不错位；
⑤keys 中每个词必须能在对应第 idx 条快讯正文中逐字找到。"""
    user_payload = [{"idx": i, "text": it["rich_text"]} for i, it in enumerate(batch)]
    return system_prompt, json.dumps(user_payload, ensure_ascii=False)


def parse_ds_content(content, batch_len):
    clean = (content or "").replace("```json", "").replace("```", "").strip()
    try:
        arr = json.loads(clean)
    except Exception:
        return None, "PARSE_FAIL " + clean[:120]
    res = {}
    seen = set()
    for r in arr:
        idx = r.get("idx")
        if not isinstance(idx, int) or not (0 <= idx < batch_len):
            continue
        if idx in seen:
            return None, f"DUP_IDX {idx}"
        seen.add(idx)
        raw_tags = r.get("tags", [])
        if not isinstance(raw_tags, list):
            raw_tags = []
        st = set()
        tags = []
        for t in raw_tags:
            if t in mod.VALID_TAGS_SET and t not in st:
                st.add(t)
                tags.append(t)
        tags = tags[:6]
        if "#无法归类等待识别" in tags and len(tags) > 1:
            tags = [t for t in tags if t != "#无法归类等待识别"]
        if not tags:
            tags = ["#无法归类等待识别"]
        raw_keys = r.get("keys", [])
        keys = []
        if isinstance(raw_keys, list):
            for k in raw_keys:
                if isinstance(k, str):
                    kk = k.strip()
                    if kk and kk not in keys:
                        keys.append(kk[:20])
        r["keys"] = keys[:4]
        r["tags"] = tags
        res[idx] = r
    missing = [i for i in range(batch_len) if i not in res]
    return res, None if not missing else f"MISSING_IDX {missing}"


def tag_ok(r):
    return bool(r) and r.get("tags") and r["tags"] != ["#无法归类等待识别"]


def keys_ok(r, rich_text):
    body = re.sub(r"\s+", "", rich_text or "")
    for k in (r or {}).get("keys", []):
        kk = re.sub(r"\s+", "", k)
        if kk and kk not in body:
            return False
    return True


def process_batch(batch, key):
    sys_prompt, user_content = build_tag_prompt(batch)
    text, usage, dt = ds_call(sys_prompt, user_content, key)
    if text is None:
        time.sleep(3)
        text, usage2, dt2 = ds_call(sys_prompt, user_content, key)
        dt += dt2
        if text is None:
            return {}, {}, dt, usage
        usage = usage2
    res, err = parse_ds_content(text, len(batch))
    ok = {}
    for i, it in enumerate(batch):
        r = (res or {}).get(i)
        if r and tag_ok(r) and keys_ok(r, it["rich_text"]):
            ok[it["id"]] = {"tags": r["tags"]}
    if not ok:
        time.sleep(3)
        text, usage2, dt2 = ds_call(sys_prompt, user_content, key)
        dt += dt2
        if text:
            res, err = parse_ds_content(text, len(batch))
            ok = {}
            for i, it in enumerate(batch):
                r = (res or {}).get(i)
                if r and tag_ok(r) and keys_ok(r, it["rich_text"]):
                    ok[it["id"]] = {"tags": r["tags"]}
    return ok, {}, dt, usage


def severity_rank(ids, cp):
    qmap, gmap = {}, {}
    for brec in cp.get("batches", {}).values():
        ids_b = brec.get("items", [])
        for k, v in (brec.get("qwen_items") or {}).items():
            if int(k) < len(ids_b):
                qmap[ids_b[int(k)]] = v
        for k, v in (brec.get("glm9b_items") or {}).items():
            if int(k) < len(ids_b):
                gmap[ids_b[int(k)]] = v
    scored = []
    for iid in ids:
        q, g = qmap.get(iid), gmap.get(iid)
        if not (q and g):
            continue
        a, b = set(q.get("tags", [])), set(g.get("tags", []))
        j = len(a & b) / max(1, len(a | b))
        if j >= TAG_JACCARD_THRESHOLD:
            continue
        scored.append((j, iid))
    scored.sort(key=lambda x: x[0])
    return [x[1] for x in scored]


def parse_md_blocks(path):
    text = open(path, encoding="utf-8").read()
    blocks = re.split(r"(?=\n### )", text)
    by_id = {}
    for bi, b in enumerate(blocks):
        m = re.search(r"\[id:: ([0-9a-f_]+)\]", b)
        if m:
            by_id[m.group(1)] = bi
    return blocks, by_id


def block_tag_line(block):
    for ln in block.splitlines():
        if ln.startswith("#") and not ln.startswith("###") and "id::" not in ln and "sentiment::" not in ln:
            return ln
    return ""


def patch_block(block, it, ds):
    """B 方案（标签专用）：只替换标签行，标题/情绪一律保留 Qwen3 原值。"""
    lines = block.splitlines()
    src = it["source_tag"]
    tags = [t for t in ds["tags"] if t != src]
    tag_line = " ".join([src] + tags)
    for li, ln in enumerate(lines):
        if ln.startswith("#") and not ln.startswith("###") and "id::" not in ln and "sentiment::" not in ln:
            lines[li] = tag_line
            break
    return "\n".join(lines)


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def main():
    dry = "--dry-run" in sys.argv
    key = os.environ.get("DEEPSEEK_API_KEY", "").strip()
    skip_s3 = os.environ.get("T1_SKIP_S3", "0") == "1"
    raw_path = os.environ.get("RAW_PATH", "")
    target_date = os.environ.get("TARGET_DATE", "").strip()
    out_dir = os.environ.get("OUT_DIR", "./output-a3")
    if not raw_path or not os.path.exists(raw_path):
        print("RAW_NOT_FOUND", raw_path, flush=True)
        return 0
    if not target_date:
        m = re.search(r"(\d{8})", os.path.basename(raw_path))
        target_date = m.group(1) if m else ""
    if not target_date:
        print("TARGET_DATE_MISSING", flush=True)
        return 0
    date_tag = mod.date_tag_from_path(raw_path, target_date)
    md_path = os.path.join(out_dir, f"{date_tag}全网宏观信息流-硅基流动.md")
    dis_path = os.path.join(out_dir, "disagreement.json")
    ckpt_path = os.path.join(out_dir, "checkpoint.json")
    for need in (md_path, dis_path, ckpt_path):
        if not os.path.exists(need):
            print(f"SKIP_MISSING_FILE {need}", flush=True)
            return 0

    ids = json.load(open(dis_path, encoding="utf-8"))
    cp = json.load(open(ckpt_path, encoding="utf-8"))
    pool = severity_rank(ids, cp)
    ordered = pool[:CAP]
    items = {it["id"]: it for it in mod.parse_raw_all(raw_path)}
    pending = [iid for iid in ordered if iid in items]
    _lim_raw = os.environ.get("T1_LIMIT", "").strip()
    if _lim_raw:
        try:
            pending = pending[:max(0, int(_lim_raw))]
        except ValueError:
            pass
    md_sha = sha256_file(md_path)

    if dry:
        print("DRYRUN", json.dumps({"date": date_tag, "disagreement": len(ids),
                                    "tag_pool": len(pool), "cap": len(ordered),
                                    "found": len(pending), "md_sha256": md_sha[:16]},
                                   ensure_ascii=False), flush=True)
        return 0

    # 幂等：同一天同一成品 SHA 已兜底过则跳过
    if not skip_s3:
        try:
            done_bytes = mod.s3_get_optional(f"a3-reports/{date_tag}/deepseek_tag_done.json",
                                             mod.s3_cfg_aux())
            if done_bytes:
                done = json.loads(done_bytes.decode("utf-8", "replace"))
                if done.get("md_sha256") == md_sha:
                    print("SKIP_ALREADY_DONE", date_tag, flush=True)
                    return 0
        except Exception as e:
            print("DONE_CHECK_ERR", repr(e), flush=True)

    if not key:
        print("DEEPSEEK_API_KEY_MISSING", flush=True)
        report = {"date": date_tag, "status": "FAILED", "reason": "NO_DEEPSEEK_API_KEY",
                  "done_at": time.strftime("%Y-%m-%d %H:%M:%S")}
        if not skip_s3:
            mod.s3_put_retry(f"a3-reports/{date_tag}/deepseek_tag_done.json",
                             json.dumps(report, ensure_ascii=False).encode("utf-8"),
                             "application/json", cfg=mod.s3_cfg_aux())
        return 2
    if not pending:
        print("NO_TAG_DIVERGENCE", date_tag, flush=True)
        if not skip_s3:
            done = {"date": date_tag, "status": "OK", "md_sha256": md_sha, "processed": 0,
                    "done_at": time.strftime("%Y-%m-%d %H:%M:%S")}
            mod.s3_put_retry(f"a3-reports/{date_tag}/deepseek_tag_done.json",
                             json.dumps(done, ensure_ascii=False).encode("utf-8"),
                             "application/json", cfg=mod.s3_cfg_aux())
        return 0

    batches = [pending[i:i + BATCH_SIZE] for i in range(0, len(pending), BATCH_SIZE)]
    results = {}
    usage_sum = {"input": 0, "output": 0}
    lat_sum = 0.0
    done_batches = 0
    print(f"DS_TAG_BACKSTOP {date_tag} pending={len(pending)} batches={len(batches)}", flush=True)
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(process_batch, [items[iid] for iid in b], key): b for b in batches}
        for fut in as_completed(futs):
            ok, _, dt, usage = fut.result()
            results.update(ok)
            lat_sum += dt
            if usage:
                usage_sum["input"] += usage.get("input_tokens", 0) or usage.get("prompt_tokens", 0)
                usage_sum["output"] += usage.get("output_tokens", 0) or usage.get("completion_tokens", 0)
            done_batches += 1
            if done_batches % 5 == 0 or done_batches == len(batches):
                print(f"  batches {done_batches}/{len(batches)} ok={len(results)}", flush=True)

    failed = max(0, len(pending) - len(results))
    # 只改标签行，标题/情绪不动
    blocks, by_id = parse_md_blocks(md_path)
    changed = 0
    review = []
    for iid, ds in results.items():
        if iid not in items:
            continue
        bi = by_id.get(iid)
        if bi is None:
            continue
        old_block = blocks[bi]
        new_block = patch_block(old_block, items[iid], ds)
        same = block_tag_line(old_block).strip() == block_tag_line(new_block).strip()
        if not same:
            changed += 1
        blocks[bi] = new_block
        review.append({"id": iid, "sf_tags": block_tag_line(old_block),
                       "ds_tags": ds["tags"], "changed": not same})
    patched_text = "".join(blocks)
    with open(md_path, "w", encoding="utf-8", newline="\n") as f:
        f.write(patched_text)
    patched_sha = sha256_file(md_path)
    print(f"DS_PATCHED {date_tag} processed={len(results)} changed={changed} "
          f"failed={failed} tokens={usage_sum} md_sha={patched_sha[:16]}", flush=True)

    if not skip_s3:
        data = open(md_path, "rb").read()
        s3_ok = False
        for attempt in range(1, 4):
            try:
                mod.s3_put_retry(f"{date_tag}全网宏观信息流-硅基流动.md", data,
                                 "text/markdown; charset=utf-8", cfg=mod.s3_cfg())
                _, got = mod.s3_request("GET", f"{date_tag}全网宏观信息流-硅基流动.md")
                if hashlib.sha256(got).hexdigest() == hashlib.sha256(data).hexdigest():
                    s3_ok = True
                    break
                print(f"S3_HASH_MISMATCH attempt={attempt}", flush=True)
            except Exception as e:
                print(f"S3_UPLOAD_ERR attempt={attempt} {repr(e)}", flush=True)
        if not s3_ok:
            print("S3_UPLOAD_FAILED_RED", flush=True)
            return 6
        review_payload = json.dumps(review, ensure_ascii=False).encode("utf-8")
        mod.s3_put_retry(f"a3-reports/{date_tag}/deepseek_tag_review.json",
                         review_payload, "application/json", cfg=mod.s3_cfg_aux())
        done = {"date": date_tag, "status": "OK", "md_sha256": patched_sha,
                "processed": len(results), "changed": changed, "failed": failed,
                "tokens": usage_sum, "avg_latency_s": round(lat_sum / max(1, done_batches), 1),
                "done_at": time.strftime("%Y-%m-%d %H:%M:%S")}
        mod.s3_put_retry(f"a3-reports/{date_tag}/deepseek_tag_done.json",
                         json.dumps(done, ensure_ascii=False).encode("utf-8"),
                         "application/json", cfg=mod.s3_cfg_aux())
        print("DS_TAG_DONE", json.dumps(done, ensure_ascii=False), flush=True)
    else:
        print("DS_TAG_DONE_LOCAL_SKIP_S3", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
