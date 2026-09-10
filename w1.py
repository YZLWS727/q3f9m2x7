# -*- coding: utf-8 -*-
"""w1: 宏观信息流看门狗 v4（DeepSeek a0 版）

监控：私有仓 xinlang24H 的 a0 工作流（抓取 + DeepSeek 打标 + S3 推流）
产出：主桶 `YYYYMMDD周X全网宏观信息流.md`（a0 为无后缀命名）
规则：
- 00:00-06:30（北京）每 5 分钟心跳；
- 06:30 后：a0 在跑 → 每 5 分钟运行心跳；空闲 → 状态变化 1 条 + 每 2 小时空闲心跳；
- 异常（成品未生成 / a0 最近失败 / S3 同步步骤失败）状态变化时推送；
- 状态存辅助桶 watchdog/state.json；WATCHDOG_DRY=1 只检测不推送。
切回公共仓组合：把 REPO/WF 改回 q3f9m2x7 + a3.yml 即可（旧版逻辑在 git 历史）。
"""
import datetime
import importlib.util
import json
import os
import sys
import time
import urllib.error
import urllib.request

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = "YZLWS727/xinlang24H"
WF = "a0.yml"
PRODUCT_TAIL = "全网宏观信息流.md"
WEEKDAY_CN = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]

spec = importlib.util.spec_from_file_location("a3label", os.path.join(HERE, "a3_label.py"))
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)


def bj_now():
    return datetime.datetime.utcnow() + datetime.timedelta(hours=8)


def bj_from_iso(s):
    try:
        dt = datetime.datetime.strptime(
            (s or "").replace("Z", "").replace(" ", "T")[:19], "%Y-%m-%dT%H:%M:%S")
        return (dt + datetime.timedelta(hours=8)).strftime("%m-%d %H:%M")
    except Exception:
        return (s or "")[:16]


def gh_get(path, token):
    """GitHub API 读取（3 次重试，防瞬时 SSL/网络抖动误报）"""
    last = None
    for attempt in range(1, 4):
        try:
            req = urllib.request.Request("https://api.github.com/" + path, headers={
                "Authorization": "Bearer " + token,
                "Accept": "application/vnd.github+json",
                "User-Agent": "codex-watchdog"})
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.loads(r.read().decode("utf-8"))
        except Exception as e:
            last = e
            time.sleep(2 * attempt)
    raise last


def workflow_state(token):
    out = {"active": False, "conclusion": "none", "status": "none",
           "run_id": None, "created": "", "s3_step": None}
    try:
        runs = gh_get(f"repos/{REPO}/actions/workflows/{WF}/runs?per_page=3", token)
        items = runs.get("workflow_runs", [])
        active = any(x.get("status") in ("in_progress", "queued", "waiting", "pending")
                     for x in items)
        latest = items[0] if items else {}
        out.update({"active": active,
                    "conclusion": latest.get("conclusion") or "none",
                    "status": latest.get("status") or "none",
                    "run_id": latest.get("id"),
                    "created": latest.get("created_at", "")})
        if latest.get("id"):
            try:
                jobs = gh_get(f"repos/{REPO}/actions/runs/{latest['id']}/jobs?per_page=5", token)
                for j in jobs.get("jobs", []):
                    for stp in j.get("steps", []):
                        nm = stp.get("name") or ""
                        if "Rclone" in nm or "同步到 S3" in nm:
                            out["s3_step"] = stp.get("conclusion") or stp.get("status")
            except Exception as e:
                out["step_err"] = repr(e)[:80]
    except Exception as e:
        out["err"] = repr(e)[:120]
    return out


def product_status(date_str):
    dt = datetime.datetime.strptime(date_str, "%Y%m%d")
    prefix = f"{date_str}{WEEKDAY_CN[dt.weekday()]}全网宏观信息流"
    if not os.environ.get("S3_BUCKET"):
        return {"exists": None, "name": prefix, "note": "S3 未配置"}
    try:
        keys = mod.s3_list_keys("", mod.s3_cfg())
        hit = [k for k in keys
               if k.rsplit("/", 1)[-1].startswith(prefix)
               and k.rsplit("/", 1)[-1].endswith(".md")]
        hit.sort()
        if not hit:
            return {"exists": False, "name": prefix, "note": "未生成"}
        return {"exists": True, "name": hit[0].rsplit("/", 1)[-1], "key": hit[0],
                "candidates": hit}
    except Exception as e:
        return {"exists": None, "name": name, "note": "读取异常 " + repr(e)[:80]}


def product_size(key):
    try:
        st, body = mod.s3_request("GET", key)
        if st == 200 and body:
            return len(body)
    except Exception:
        pass
    return None


def recent_block(wf, product):
    lines = ["【最近运行】"]
    if wf.get("err"):
        lines.append("a0: 查询异常 " + str(wf.get("err"))[:60])
        if product.get("exists") is True:
            sz = product.get("size")
            lines.append("成品: 已生成" + (f"（{round(sz/1024)}KB）" if sz else ""))
        elif product.get("exists") is False:
            lines.append("成品: 未生成")
        else:
            lines.append("成品: " + str(product.get("note")))
        return "\n".join(lines)
    concl = wf.get("conclusion") or "none"
    line = f"a0: {concl} ({bj_from_iso(wf.get('created', ''))})"
    if wf.get("s3_step"):
        line += " · S3同步=" + str(wf.get("s3_step"))
    lines.append(line)
    if product.get("exists") is True:
        sz = product.get("size")
        lines.append("成品: 已生成" + (f"（{round(sz/1024)}KB）" if sz else ""))
    elif product.get("exists") is False:
        lines.append("成品: 未生成")
    else:
        lines.append("成品: " + str(product.get("note")))
    return "\n".join(lines)


def load_state():
    if not os.environ.get("S3_AUX_BUCKET"):
        return {}
    try:
        st, body = mod.s3_request("GET", "watchdog/state.json", cfg=mod.s3_cfg_aux())
        if st == 200 and body:
            return json.loads(body.decode("utf-8", "replace"))
    except Exception:
        pass
    return {}


def save_state(state):
    if not os.environ.get("S3_AUX_BUCKET"):
        return False
    try:
        mod.s3_put_retry("watchdog/state.json",
                         json.dumps(state, ensure_ascii=False).encode("utf-8"),
                         "application/json", cfg=mod.s3_cfg_aux())
        return True
    except Exception as e:
        print("STATE_SAVE_ERR", repr(e), flush=True)
        return False


def push_wecom(webhook, title, content):
    body = {"msgtype": "text",
            "text": {"content": (title + "\n" + content)[:2000]}}
    req = urllib.request.Request(webhook, method="POST", data=json.dumps(
        body, ensure_ascii=False).encode("utf-8"), headers={
        "Content-Type": "application/json; charset=utf-8",
        "User-Agent": "codex-watchdog"})
    with urllib.request.urlopen(req, timeout=30) as r:
        resp = json.loads(r.read().decode("utf-8", "replace"))
    return resp


def main():
    dry = os.environ.get("WATCHDOG_DRY", "").strip() == "1"
    token = (os.environ.get("READ_TOKEN") or os.environ.get("GH_TOKEN") or "").strip()
    webhook = os.environ.get("WECOM_WEBHOOK", "").strip()
    if not token:
        print("TOKEN_MISSING", flush=True)
        return 2
    now = bj_now()
    now_iso = now.strftime("%Y-%m-%d %H:%M:%S")
    night = now.hour < 6 or (now.hour == 6 and now.minute <= 30)
    yesterday = (now - datetime.timedelta(days=1)).strftime("%Y%m%d")

    wf = workflow_state(token)
    state = load_state()
    product = product_status(yesterday)
    if product.get("exists") and state.get("last_product_key") != product.get("key"):
        sz = product_size(product["key"])
        if sz:
            product["size"] = sz
        state["last_product_key"] = product.get("key")

    anomalies = []
    if wf.get("err"):
        state["err_streak"] = int(state.get("err_streak", 0)) + 1
    else:
        state["err_streak"] = 0
    if int(state.get("err_streak", 0)) >= 3:
        anomalies.append("a0 查询连续失败: " + str(wf.get("err"))[:60])
    if wf.get("conclusion") == "failure":
        anomalies.append("a0 最近一次运行失败")
    if wf.get("s3_step") in ("failure", "cancelled"):
        anomalies.append("a0 S3 同步步骤 " + str(wf.get("s3_step")))
    if product.get("exists") is False and now.hour >= 6:
        anomalies.append(f"{yesterday} 成品未生成")
    elif product.get("exists") is None:
        anomalies.append("成品检查异常: " + str(product.get("note")))

    running = ["a0"] if wf.get("active") else []
    last_state = state.get("state", "")
    last_beat = state.get("last_beat", "")
    last_anomaly = state.get("anomaly_id", "")
    anomaly_id = "|".join(anomalies)
    recent = recent_block(wf, product)
    last_concl = wf.get("conclusion", "none")

    if running:
        new_state = "RUNNING:a0"
    else:
        new_state = "IDLE:" + str(last_concl)

    msg = None
    if anomaly_id and anomaly_id != last_anomaly:
        msg = ("🔴 看门狗异常", anomaly_id + "\n时间：" + now_iso)
    elif new_state != last_state:
        if running:
            msg = ("🟢 运行中", "a0 正在运行\nrun=" + str(wf.get("run_id")) + "\n时间：" + now_iso)
        else:
            msg = ("⚪ 空闲", "a0 无运行，上次结论=" + str(last_concl) + "\n时间：" + now_iso)
    else:
        delta = 999999.0
        if last_beat:
            try:
                delta = (now - datetime.datetime.strptime(last_beat, "%Y-%m-%d %H:%M:%S")).total_seconds()
            except Exception:
                delta = 999999.0
        interval = 300 if (night or running) else 7200
        if delta >= interval:
            if running:
                msg = ("🟢 运行心跳", "a0 运行中\n时间：" + now_iso)
            elif night:
                msg = ("🌙 夜间心跳", "当前空闲，a0 无运行\n时间：" + now_iso)
            else:
                msg = ("⚪ 空闲心跳", "2 小时例行状态：a0 无运行，上次结论=" + str(last_concl) + "\n时间：" + now_iso)

    if msg:
        msg = (msg[0], msg[1] + "\n" + recent)
        if dry:
            print("DRY_PUSH", json.dumps(msg, ensure_ascii=False), flush=True)
        elif webhook:
            try:
                resp = push_wecom(webhook, msg[0], msg[1])
                print("PUSH_OK", resp.get("errcode"), resp.get("errmsg", ""), flush=True)
            except Exception as e:
                print("PUSH_ERR", repr(e)[:200], flush=True)
        else:
            print("WECOM_WEBHOOK_MISSING", flush=True)
        state["last_beat"] = now_iso
    state["state"] = new_state
    state["anomaly_id"] = anomaly_id
    if not dry:
        save_state(state)

    print("SUMMARY", json.dumps({
        "time": now_iso, "running": running, "last_concl": last_concl,
        "anomalies": anomalies, "state": new_state, "pushed": bool(msg),
        "product": product, "a0": {"conclusion": wf.get("conclusion"),
                                   "status": wf.get("status"),
                                   "s3_step": wf.get("s3_step"),
                                   "created": wf.get("created")}},
       ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())