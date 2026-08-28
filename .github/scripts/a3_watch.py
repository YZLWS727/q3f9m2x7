# -*- coding: utf-8 -*-
"""a3 看门狗：目标日 raw 已存在但 a3 无近期成功/运行中时自动派发 a3。
幂等：raw 缺失跳过；a3 运行中/15 分钟内活动跳过；8 小时内成功过跳过（成品假设已就绪）。"""
import datetime
import glob
import json
import os
import subprocess
import sys

GH_TOKEN = os.environ.get("GH_TOKEN", "")
GH_REPO = os.environ.get("GH_REPO", "")
RECENT_MIN = 15
SUCCESS_WINDOW_H = 8


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
    if not raws:
        print("WATCH_SKIP no raw for target date", flush=True)
        return 0
    r = sh(["gh", "api", f"repos/{GH_REPO}/actions/workflows/a3.yml/runs?per_page=5"])
    if r.returncode != 0:
        print("WATCH_ERR a3 runs query", r.stderr[-200:], flush=True)
        return 0
    runs = json.loads(r.stdout).get("workflow_runs", [])
    now = datetime.datetime.now(datetime.timezone.utc)
    for run in runs:
        created = datetime.datetime.fromisoformat(run["created_at"].replace("Z", "+00:00"))
        age_sec = (now - created).total_seconds()
        if run["status"] == "in_progress":
            print("WATCH_SKIP a3 in_progress", flush=True)
            return 0
        if age_sec < RECENT_MIN * 60:
            print(f"WATCH_SKIP a3 recently active ({run['conclusion']})", flush=True)
            return 0
        if run["conclusion"] == "success" and age_sec < SUCCESS_WINDOW_H * 3600:
            print("WATCH_SKIP a3 success within 8h", flush=True)
            return 0
    sh(["gh", "workflow", "enable", "a3.yml", "--repo", GH_REPO])
    dsp = sh(["gh", "workflow", "run", "a3.yml", "--repo", GH_REPO])
    if dsp.returncode == 0:
        print("WATCH_DISPATCH a3 OK", flush=True)
    else:
        print("WATCH_DISPATCH_FAIL a3", dsp.stderr[-300:], flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
