# -*- coding: utf-8 -*-
"""Neutral launcher: downloads the real job script from a private source and runs it."""
import os
import subprocess
import sys
import urllib.request


def main():
    url = os.environ.get("PRIVATE_RAW_URL", "")
    token = os.environ.get("READ_TOKEN", "")
    if not url:
        print("PRIVATE_RAW_URL missing")
        return 2
    os.makedirs("_job", exist_ok=True)
    target = os.path.join("_job", "real.py")
    req = urllib.request.Request(url)
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Accept", "application/vnd.github.raw+json")
    req.add_header("User-Agent", "ok")
    with urllib.request.urlopen(req, timeout=120) as resp:
        data = resp.read()
    with open(target, "wb") as f:
        f.write(data)
    env = dict(os.environ)
    env["PYTHONIOENCODING"] = "utf-8"
    return subprocess.call([sys.executable, target], env=env)


if __name__ == "__main__":
    sys.exit(main())
