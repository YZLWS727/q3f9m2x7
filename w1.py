# -*- coding: utf-8 -*-
"""w1: 宏观信息流看门狗（GitHub Actions 每 5 分钟巡检，企业微信推送）
规则：
- 00:00-06:30（北京）：每 5 分钟一条心跳；
- 06:30 之后：有 a1/a3 在跑 → 每 5 分钟一条运行心跳；无运行 → 状态变化 1 条 + 每 2 小时一条空闲心跳；
- 异常（raw 缺失 / 新浪源 0 条 / 最近 run 失败）在状态变化时推送；
- 状态存辅助桶 watchdog/state.json，防刷屏；WATCHDOG_DRY=1 只检测不推送。
"""
import datetime
import importlib.util
import json
import os
import re
import sys
import urllib.request

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = "YZLWS727/q3f9m2x7"
SRC_TAGS = ["#新浪24H", "#格隆汇电报", "#华尔街见闻"]

spec = importlib.util.spec_from_file_location("a3label", os.path.join(HERE, "a3_label.py"))
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)


def bj_now():
    return datetime.datetime.utcnow() + datetime.timedelta(hours=8)


def gh_get(path, token):
    req = urllib.request.Request("https://api.github.com/" + path, headers={
        "Authorization": "Bearer " + token,
        "Accept": "application/vnd.github+json",
        "User-Agent": "codex-watchdog"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read().decode("utf-8"))


def workflow_state(token):
    out = {}
    for name, wf in (("a1", "a1.yml"), ("a3", "a3.yml")):
        try:
            runs = gh_get(f"repos/{REPO}/actions/workflows/{wf}/runs?per_page=3", token)
            items = runs.get("workflow_runs", [])
            active = any(x.get("status") in ("in_progress", "queued", "waiting", "pending")
                         for x in items)
            latest = items[0] if items else {}
            out[name] = {"active": active,
                         "conclusion": latest.get("conclusion") or "none",
                         "status": latest.get("status") or "none",
                         "run_id": latest.get("id"),
                         "created": latest.get("created_at", "")}
        except Exception as e:
            out[name] = {"active": False, "conclusion": "error",
                         "status": "error", "run_id": None,
                         "created": "", "err": repr(e)[:100]}
    return out


def raw_sources(token, date_str):
    try:
        items = gh_get(f"repos/{REPO}/contents/raw", token)
        target = None
        for it in items:
            if it.get("type") == "file" and it.get("name", "").startswith(date_str):
                target = it
                break
        if not target:
            return None
        if target.get("content"):
            import base64
            text = base64.b64decode(target["content"]).decode("utf-8", "replace")
        else:
            blob = gh_get(f"repos/{REPO}/git/blobs/{target['sha']}", token)
            import base64
            text = base64.b64decode(blob["content"]).decode("utf-8", "replace")
        counts = {s: len(re.findall(r"### \[" + re.escape(s) + r"\]", text)) for s in SRC_TAGS}
        return counts
    except Exception as e:
        return {"err": repr(e)[:100]}


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
    token = os.environ.get("GH_TOKEN", "").strip()
    webhook = os.environ.get("WECOM_WEBHOOK", "").strip()
    if not token:
        print("GH_TOKEN_MISSING", flush=True)
        return 2
    now = bj_now()
    now_iso = now.strftime("%Y-%m-%d %H:%M:%S")
    night = now.hour < 6 or (now.hour == 6 and now.minute <= 30)
    yesterday = (now - datetime.timedelta(days=1)).strftime("%Y%m%d")

    wf = workflow_state(token)
    running = [k for k, v in wf.items() if v.get("active")]
    last_concl = "none"
    last_created = ""
    for k, v in wf.items():
        if v.get("created", "") > last_created:
            last_created = v.get("created", "")
            last_concl = v.get("conclusion", "none")

    raw = raw_sources(token, yesterday)
    anomalies = []
    if raw is None:
        anomalies.append(f"{yesterday} raw 未生成")
    elif "err" in raw:
        anomalies.append(f"raw 检查异常: {raw['err'][:60]}")
    elif raw.get("#新浪24H", 0) == 0:
        anomalies.append(f"{yesterday} 新浪源 0 条（格隆汇{raw.get('#格隆汇电报', 0)}/华尔街{raw.get('#华尔街见闻', 0)}）")

    state = load_state()
    last_state = state.get("state", "")
    last_beat = state.get("last_beat", "")
    last_anomaly = state.get("anomaly_id", "")
    anomaly_id = "|".join(anomalies)

    if running:
        new_state = "RUNNING:" + ",".join(sorted(running))
    else:
        new_state = "IDLE:" + str(last_concl)

    msg = None
    if anomaly_id and anomaly_id != last_anomaly:
        msg = ("🔴 看门狗异常", anomaly_id + "\n时间：" + now_iso)
    elif new_state != last_state:
        if running:
            msg = ("🟢 运行中", "、".join(running) + " 正在运行\nrun=" + str(wf[running[0]].get("run_id")) + "\n时间：" + now_iso)
        else:
            msg = ("⚪ 空闲", "a1/a3 均无运行，上次结论=" + str(last_concl) + "\n时间：" + now_iso)
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
                msg = ("🟢 运行心跳", "、".join(running) + " 运行中\n时间：" + now_iso)
            elif night:
                msg = ("🌙 夜间心跳", "当前空闲，a1/a3 无运行\n时间：" + now_iso)
            else:
                msg = ("⚪ 空闲心跳", "2 小时例行状态：a1/a3 无运行，上次结论=" + str(last_concl) + "\n时间：" + now_iso)

    if msg:
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
        "anomalies": anomalies, "state": new_state,
        "pushed": bool(msg)}, ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
