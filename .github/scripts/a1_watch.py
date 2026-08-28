# -*- coding: utf-8 -*-
"""a1 看门狗：目标日（昨天北京）raw 缺失且 a1 无近期活动时自动派发 a1。
幂等：raw 已存在则跳过；a1 运行中/15 分钟内刚完成则跳过。"""
import datetime
import glob
import json
import os
import subprocess
import sys

GH_TOKEN = os.environ.get("GH_TOKEN", "")
GH_REPO = os.environ.get("GH_REPO", "")
RECENT_MIN = 15


def sh(args):
    env = dict(os.environ)
    env["GH_TOKEN"] = GH_TOKEN
    return subprocess.run(args, capture_output=True, text=True, env=env)


def yesterday_bjt():
    bjt = datetime.timezone(datetime.timedelta(hours=8))
    return (datetime.datetime.now(bjt) - datetime.timedelta(days=1)).strftime("%Y%m%d")


def main():
    if not GH_TOKEN or not GH_REPO:
        print("WATCH_SKIP no GH env", flush=True)
        return 0
    d = yesterday_bjt()
    raws = glob.glob(f"raw/*{d}*原始未打标*.md")
    if raws:
        print(f"WATCH_SKIP raw exists {raws[0]}", flush=True)
        return 0
    r = sh(["gh", "api", f"repos/{GH_REPO}/actions/workflows/a1.yml/runs?per_page=5"])
    if r.returncode != 0:
        print("WATCH_ERR a1 runs query", r.stderr[-200:], flush=True)
        return 0
    runs = json.loads(r.stdout).get("workflow_runs", [])
    now = datetime.datetime.now(datetime.timezone.utc)
    for run in runs:
        created = datetime.datetime.fromisoformat(run["created_at"].replace("Z", "+00:00"))
        age_sec = (now - created).total_seconds()
        if run["status"] == "in_progress":
            print("WATCH_SKIP a1 in_progress", flush=True)
            return 0
        if age_sec < RECENT_MIN * 60:
            print(f"WATCH_SKIP a1 recently active ({run['conclusion']})", flush=True)
            return 0
    dsp = sh(["gh", "workflow", "run", "a1.yml", "--repo", GH_REPO])
    if dsp.returncode == 0:
        print("WATCH_DISPATCH a1 OK", flush=True)
    else:
        print("WATCH_DISPATCH_FAIL a1", dsp.stderr[-300:], flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
