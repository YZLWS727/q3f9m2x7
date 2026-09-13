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
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeout

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("a3label", os.path.join(HERE, "a3_label.py"))
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

DS_BASE = "https://api.deepseek.com"
DS_MODEL = "deepseek-v4-flash"
CAP = 1000


def _env_int(name, default):
    """可选数值型环境变量：空值/非法值回退默认值（避免 int('') 崩溃）"""
    raw = os.environ.get(name, "").strip()
    try:
        return int(raw) if raw else default
    except ValueError:
        return default


def _env_float(name, default):
    raw = os.environ.get(name, "").strip()
    try:
        return float(raw) if raw else default
    except ValueError:
        return default


T1_MAX_MINUTES = _env_int("T1_MAX_MINUTES", 150)     # 硬上限：到点停止收集，已完成部分照常写回（PARTIAL）
T1_WARN_MINUTES = _env_int("T1_WARN_MINUTES", 90)    # 软提醒：只推企业微信，不中止
T1_STALL_MINUTES = _env_int("T1_STALL_MINUTES", 20)  # 无进展熔断：连续 N 分钟无批次完成即中止
T1_COST_ALERT = _env_float("T1_COST_ALERT", 5.0)     # 估算成本超过该值推提醒（元）
TAG_JACCARD_THRESHOLD = _env_float("T1_TAG_THRESHOLD", 0.40)  # 标签分歧阈值（0.4 实际含 0.333 档）
T1_TAG_MAX = _env_int("T1_TAG_MAX", 8)                        # 单条标签上限
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

【多标签纪律（最高优先级）】
1. 目标是输出 4~8 个标签：请系统检查并覆盖新闻里所有明确相关的维度——地区/国别、市场（股市/债市/汇率/大宗商品等）、主体（央行/政府/公司/机构）、行业、政策、地缘政治、社会与认知维度（认知偏差、情绪、趋势预测等）；只有确实相关维度很少时才允许少于 4 个；绝不允许为凑数添加明显不相关的标签；
2. 所有标签必须一字不差来自《预设标签库》，绝对禁止自创、改写、合并或新增任何白名单之外的标签；
3. 标签上限 8 个，绝不超过；
4. 一条都找不到匹配时，只输出 ["#无法归类等待识别"]；该标签必须独占（唯一），禁止与其他任何标签同时出现；
5. 输出前逐条自检：标签数不超过 8 个；#无法归类等待识别 只能单独出现；#期权 仅当正文明确出现 期权/看涨期权/看跌期权/认购/认沽/行权/隐含波动率/到期/做市 等期权要素时才使用。

【对齐纪律（最高优先级）】
- 必须严格按输入顺序输出，idx 从 0 到 N-1，每个只用一次，禁止跳号、重复或错位；
- 逐条核对：第 idx 条的标签与 keys 只能依据第 idx 条快讯的正文，不得串到其他快讯；
- keys 输出 2~3 个支持标签判断的正文原文关键词或短语（必须能在该条正文中逐字找到），仅用于对齐校验，不写入成品。

《预设标签库》：
{tag_list}

仅返回纯 JSON 数组，禁止任何 markdown 标记。格式示例：
[ {{"idx": 0, "tags": ["#美国", "#债务和债券市场", "#金融流动性", "#宏观政策"], "keys": ["美联储", "国债"]}} ]

【输出前强制检查】
①每条标签数最多 8 个（通常 4~8 个）；禁止为凑数添加弱相关、泛化或“无法归类等待识别”标签；
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
        tags = tags[:T1_TAG_MAX]
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


def _fail_reason(res, i, it, err):
    r = (res or {}).get(i)
    if r is None:
        return ("BATCH_PARSE " + str(err)[:80]) if res is None else "MISSING_IDX"
    if not r.get("tags"):
        return "EMPTY_TAGS"
    if r["tags"] == ["#无法归类等待识别"]:
        return "FALLBACK_TAG"
    return "KEYS_MISMATCH"


def process_batch(batch, key):
    sys_prompt, user_content = build_tag_prompt(batch)
    text, usage, dt = ds_call(sys_prompt, user_content, key)
    if text is None:
        time.sleep(3)
        text, usage2, dt2 = ds_call(sys_prompt, user_content, key)
        dt += dt2
        if text is None:
            return {}, {it["id"]: "NETWORK_FAIL" for it in batch}, dt, usage
        usage = usage2
    res, err = parse_ds_content(text, len(batch))
    ok = {}
    still = []
    for i, it in enumerate(batch):
        r = (res or {}).get(i)
        if r and tag_ok(r) and keys_ok(r, it["rich_text"]):
            ok[it["id"]] = {"tags": r["tags"]}
        else:
            still.append((i, it))
    fails = {}
    for i, it in still:
        sp1, uc1 = build_tag_prompt([it])
        reason = _fail_reason(res, i, it, err)
        for _ in range(2):
            t2, u2, d2 = ds_call(sp1, uc1, key)
            dt += d2
            if t2:
                r2, err2 = parse_ds_content(t2, 1)
                rr = (r2 or {}).get(0)
                if rr and tag_ok(rr) and keys_ok(rr, it["rich_text"]):
                    ok[it["id"]] = {"tags": rr["tags"]}
                    reason = None
                    break
                reason = _fail_reason(r2, 0, it, err2)
            else:
                reason = "SINGLE_NETWORK_FAIL"
            time.sleep(1)
        if reason:
            fails[it["id"]] = reason
    return ok, fails, dt, usage


def pool_from_checkpoint(cp, threshold):
    """从检查点全量枚举条目（含 Qwen3 与 GLM9B 双方标签），按 Jaccard 升序返回。
    不再依赖 disagreement.json 的 0.3 口径，便于独立调整兜底阈值。"""
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
    for iid in set(list(qmap.keys()) + list(gmap.keys())):
        q, g = qmap.get(iid), gmap.get(iid)
        if not (q and g):
            continue
        a, b = set(q.get("tags", [])), set(g.get("tags", []))
        j = len(a & b) / max(1, len(a | b))
        if j >= threshold:
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


def push_wecom(title, content):
    """企业微信推送（无 webhook 时静默跳过）"""
    hook = os.environ.get("WECOM_WEBHOOK", "").strip()
    if not hook:
        return False
    try:
        body = {"msgtype": "text", "text": {"content": (title + "\n" + content)[:2000]}}
        req = urllib.request.Request(hook, method="POST",
                                     data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
                                     headers={"Content-Type": "application/json; charset=utf-8",
                                              "User-Agent": "t1-backstop"})
        with urllib.request.urlopen(req, timeout=30) as r:
            resp = json.loads(r.read().decode("utf-8", "replace"))
        print("T1_WECOM_PUSH", resp.get("errcode"), resp.get("errmsg", ""), flush=True)
        return True
    except Exception as e:
        print("T1_WECOM_PUSH_ERR", repr(e)[:150], flush=True)
        return False


def estimate_cost(usage):
    """按谷价估算：输出 4 元/M、未命中输入 1 元/M、命中输入 0.02 元/M"""
    total_in = usage.get("input", 0) or 0
    cached = min(usage.get("cached", 0) or 0, total_in)
    miss = max(0, total_in - cached)
    out = usage.get("output", 0) or 0
    return out / 1e6 * 4.0 + miss / 1e6 * 1.0 + cached / 1e6 * 0.02


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
    pool = pool_from_checkpoint(cp, TAG_JACCARD_THRESHOLD)
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
                                    "threshold": TAG_JACCARD_THRESHOLD, "tag_max": T1_TAG_MAX,
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
    failed_letters = {}
    usage_sum = {"input": 0, "output": 0, "cached": 0}
    lat_sum = 0.0
    done_batches = 0
    stop_reason = ""
    warn_sent = False
    start_ts = time.time()
    last_progress_ts = start_ts
    max_sec = max(1, T1_MAX_MINUTES) * 60
    warn_sec = max(1, T1_WARN_MINUTES) * 60
    stall_sec = max(1, T1_STALL_MINUTES) * 60
    print(f"DS_TAG_BACKSTOP {date_tag} pending={len(pending)} batches={len(batches)} "
          f"max={T1_MAX_MINUTES}min warn={T1_WARN_MINUTES}min stall={T1_STALL_MINUTES}min", flush=True)
    ex = ThreadPoolExecutor(max_workers=WORKERS)
    futs = {ex.submit(process_batch, [items[iid] for iid in b], key): b for b in batches}
    pending_futs = set(futs)
    try:
        while pending_futs and not stop_reason:
            try:
                for fut in as_completed(pending_futs, timeout=max(5, min(60, stall_sec))):
                    pending_futs.discard(fut)
                    ok, fails, dt, usage = fut.result()
                    results.update(ok)
                    failed_letters.update(fails)
                    lat_sum += dt
                    if usage:
                        usage_sum["input"] += usage.get("input_tokens", 0) or usage.get("prompt_tokens", 0)
                        usage_sum["output"] += usage.get("output_tokens", 0) or usage.get("completion_tokens", 0)
                        detail = usage.get("input_tokens_details") or {}
                        usage_sum["cached"] += (detail.get("cached_tokens", 0) or
                                                usage.get("prompt_cache_hit_tokens", 0) or 0)
                    done_batches += 1
                    last_progress_ts = time.time()
                    if done_batches % 5 == 0 or done_batches == len(batches):
                        print(f"  batches {done_batches}/{len(batches)} ok={len(results)} "
                              f"elapsed={int(time.time()-start_ts)}s", flush=True)
                    elapsed_now = time.time() - start_ts
                    if (not warn_sent) and elapsed_now >= warn_sec and elapsed_now < max_sec:
                        warn_sent = True
                        push_wecom("🟡 t1 运行偏慢",
                                   f"{date_tag} 已运行 {T1_WARN_MINUTES} 分钟，完成 {done_batches}/{len(batches)} 批、"
                                   f"{len(results)} 条；继续处理，暂不中止（硬上限 {T1_MAX_MINUTES} 分钟）。")
                        print("T1_WARN_PUSHED", flush=True)
                    if elapsed_now > max_sec:
                        stop_reason = "TIME_BUDGET"
                        break
                    if time.time() - last_progress_ts > stall_sec:
                        stop_reason = "NO_PROGRESS"
                        break
            except FuturesTimeout:
                pass
            if stop_reason:
                break
            if time.time() - start_ts > max_sec:
                stop_reason = "TIME_BUDGET"
            elif time.time() - last_progress_ts > stall_sec:
                stop_reason = "NO_PROGRESS"
    finally:
        for f in list(pending_futs):
            f.cancel()
        ex.shutdown(wait=False, cancel_futures=True)
    if stop_reason:
        print(f"T1_STOP reason={stop_reason} done_batches={done_batches}/{len(batches)} "
              f"processed={len(results)}", flush=True)

    failed = max(0, len(pending) - len(results))
    from collections import Counter as _Counter
    reason_sum = dict(_Counter(failed_letters.values()))
    est_cost = estimate_cost(usage_sum)
    elapsed_s = int(time.time() - start_ts)
    status = "PARTIAL" if stop_reason else "OK"
    print(f"FAIL_REASONS {reason_sum}", flush=True)
    print(f"T1_SUMMARY status={status} stop={stop_reason or 'none'} elapsed={elapsed_s}s "
          f"processed={len(results)}/{len(pending)} est_cost={est_cost:.2f}CNY", flush=True)
    failed_path = os.path.join(out_dir, "deepseek_tag_failed.json")
    with open(failed_path, "w", encoding="utf-8", newline="\n") as f:
        json.dump(failed_letters, f, ensure_ascii=False, indent=1)
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
        old_line = block_tag_line(old_block)
        src = items[iid]["source_tag"]
        old_ai = {x for x in old_line.split() if x.startswith("#") and x != src}
        new_ai = {x for x in ds["tags"] if x != src}
        if old_ai == new_ai:
            # 标签集合一致、仅顺序不同：不改写，避免无意义 churn
            review.append({"id": iid, "sf_tags": old_line, "ds_tags": ds["tags"],
                           "changed": False, "skip_reason": "SET_EQUAL"})
            continue
        new_block = patch_block(old_block, items[iid], ds)
        changed += 1
        blocks[bi] = new_block
        review.append({"id": iid, "sf_tags": old_line,
                       "ds_tags": ds["tags"], "changed": True})
    patched_text = "".join(blocks)
    with open(md_path, "w", encoding="utf-8", newline="\n") as f:
        f.write(patched_text)
    patched_sha = sha256_file(md_path)
    print(f"DS_PATCHED {date_tag} processed={len(results)} changed={changed} "
          f"failed={failed} tokens={usage_sum} est_cost={est_cost:.2f} md_sha={patched_sha[:16]}", flush=True)

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
        mod.s3_put_retry(f"a3-reports/{date_tag}/deepseek_tag_failed.json",
                         json.dumps(failed_letters, ensure_ascii=False).encode("utf-8"),
                         "application/json", cfg=mod.s3_cfg_aux())
        done = {"date": date_tag, "status": status, "stop_reason": stop_reason,
                "md_sha256": patched_sha, "cap": CAP, "threshold": TAG_JACCARD_THRESHOLD,
                "tag_max": T1_TAG_MAX, "pool": len(pool),
                "requested": len(pending), "processed": len(results), "changed": changed,
                "failed": failed, "tokens": usage_sum, "est_cost_cny": round(est_cost, 2),
                "elapsed_s": elapsed_s, "avg_latency_s": round(lat_sum / max(1, done_batches), 1),
                "done_at": time.strftime("%Y-%m-%d %H:%M:%S")}
        mod.s3_put_retry(f"a3-reports/{date_tag}/deepseek_tag_done.json",
                         json.dumps(done, ensure_ascii=False).encode("utf-8"),
                         "application/json", cfg=mod.s3_cfg_aux())
        print("DS_TAG_DONE", json.dumps(done, ensure_ascii=False), flush=True)
        if stop_reason:
            push_wecom("🟠 t1 提前收尾",
                       f"{date_tag} 原因={stop_reason}（硬上限 {T1_MAX_MINUTES} 分钟/无进展 {T1_STALL_MINUTES} 分钟）；"
                       f"已完成 {len(results)}/{len(pending)} 条，其余保留硅基标签；成品已回传主桶。")
        if est_cost > T1_COST_ALERT:
            push_wecom("💰 t1 成本提醒",
                       f"{date_tag} 本次估算 {est_cost:.2f} 元（阈值 {T1_COST_ALERT:.1f} 元），处理 {len(results)} 条；"
                       f"tokens 输入{usage_sum.get('input',0)}/输出{usage_sum.get('output',0)}。")
    else:
        print("DS_TAG_DONE_LOCAL_SKIP_S3", flush=True)
        if stop_reason:
            print(f"T1_PARTIAL_LOCAL stop={stop_reason}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
