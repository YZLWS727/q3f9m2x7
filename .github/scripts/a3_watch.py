# -*- coding: utf-8 -*-
"""a3 看门狗：目标日 raw 已存在且 S3 主桶无成品、a3 无近期活动时自动派发 a3。
判断“已完成”必须绑定目标日成品存在性（S3），不能用全局最近成功（8/29 教训）。"""
import datetime
import glob
import json
import os
import subprocess
import sys
import importlib.util
import urllib.error

GH_TOKEN = os.environ.get("GH_TOKEN", "")
GH_REPO = os.environ.get("GH_REPO", "")
RECENT_MIN = 15

sys.path.insert(0, os.getcwd())
spec = importlib.util.spec_from_file_location("a3label", os.path.join(os.getcwd(), "a3_label.py"))
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)


def sh(args):
    env = dict(os.environ)
    env["GH_TOKEN"] = GH_TOKEN
    return subprocess.run(args, capture_output=True, text=True, env=env)


def yesterday_bjt():
    bjt = datetime.timezone(datetime.timedelta(hours=8))
    return (datetime.datetime.now(bjt) - datetime.timedelta(days=1)).strftime("%Y%m%d")


def product_exists(d):
    """S3 主桶目标日成品存在性（200=存在；404=缺失；异常返回 None 表示本轮无法判断）。"""
    week = "一二三四五六日"
    date_tag = f"{d}周{week[datetime.date(int(d[:4]), int(d[4:6]), int(d[6:8])).weekday()]}"
    key = f"{date_tag}全网宏观信息流-硅基流动.md"
    try:
        st, _ = mod.s3_request("GET", key, cfg=mod.s3_cfg())
        return st == 200, st
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return False, 404
        return None, repr(e)
    except Exception as e:
        return None, repr(e)


def main():
    if not GH_TOKEN or not GH_REPO:
        print("WATCH_SKIP no GH env", flush=True)
        return 0
    d = yesterday_bjt()
    raws = glob.glob(f"raw/*{d}*原始未打标*.md")
    if not raws:
        print("WATCH_SKIP no raw for target date", flush=True)
        return 0
    exists, info = product_exists(d)
    if exists is True:
        print(f"WATCH_SKIP product exists (s3={info})", flush=True)
        return 0
    if exists is None:
        print(f"WATCH_ERR s3 check failed {info}", flush=True)
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
    sh(["gh", "workflow", "enable", "a3.yml", "--repo", GH_REPO])
    dsp = sh(["gh", "workflow", "run", "a3.yml", "--repo", GH_REPO])
    if dsp.returncode == 0:
        print("WATCH_DISPATCH a3 OK", flush=True)
    else:
        print("WATCH_DISPATCH_FAIL a3", dsp.stderr[-300:], flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
