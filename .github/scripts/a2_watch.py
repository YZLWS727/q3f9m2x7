# -*- coding: utf-8 -*-
"""a2 看门狗：若公开仓当前没有任何 a2 活动运行（进行中/排队），派发一次 a2。
用途：GitHub 免费档/低活跃公开仓的 schedule 可能延迟或整日丢弃（社区实证），
本看门狗作为独立 workflow 实体，双 cron（每小时的 17/47 分）兜底拉起 a2。"""
import json
import os
import sys
import urllib.request


def gh(method, path, body=None):
    req = urllib.request.Request("https://api.github.com" + path, method=method)
    req.add_header("Authorization", "Bearer " + os.environ.get("GH_TOKEN", ""))
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("User-Agent", "a2-watch")
    data = json.dumps(body).encode("utf-8") if body else None
    if data:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, data=data, timeout=60) as r:
            out = r.read()
            return json.loads(out.decode("utf-8")) if out else {}
    except Exception as e:
        print("WATCH_ERR", repr(e)[:200])
        return None


def main():
    repo = os.environ.get("GH_REPO", "")
    if not repo:
        print("WATCH_SKIP 无 GH_REPO")
        return 0
    runs = gh("GET", "/repos/%s/actions/workflows/a2.yml/runs?per_page=10" % repo)
    if runs is None:
        return 0
    active = [r for r in runs.get("workflow_runs", []) if r.get("status") != "completed"]
    if active:
        print("WATCH_SKIP_ACTIVE 已有 a2 活动运行:",
              [(r.get("id"), r.get("status")) for r in active])
        return 0
    res = gh("POST", "/repos/%s/actions/workflows/a2.yml/dispatches" % repo,
             {"ref": "main"})
    print("WATCH_DISPATCH", "OK" if res is not None else "FAILED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
