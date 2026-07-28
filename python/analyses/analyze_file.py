#!/usr/bin/env python3
"""
analyze_file.py — simplest end-to-end file analysis with the Primate Intelligence API.

  1. POST /v1/videos           → upload URL
  2. PUT  <bytes>              → upload
  3. POST /v1/videos/:id/complete
  4. POST /v1/analyses         → 202 queued
  5. GET  /v1/analyses/:id     → poll (or use Prefer: wait=30 to long-poll)

Usage:
  python3 analyze_file.py --api-key pv_live_... --file video.mp4 --prompt "is there a person?"

Only dependency: requests.
"""

import argparse
import mimetypes
import pathlib
import sys
import time

import requests

BASE = "https://api.primateintelligence.ai"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--api-key", required=True)
    ap.add_argument("--file", required=True)
    ap.add_argument("--prompt", default="is there a person?")
    ap.add_argument("--base-url", default=BASE)
    args = ap.parse_args()

    H = {"Authorization": f"Bearer {args.api_key}"}
    path = pathlib.Path(args.file)
    ctype = mimetypes.guess_type(path.name)[0] or "video/mp4"

    # 1. create video resource
    r = requests.post(f"{args.base_url}/v1/videos", headers=H,
                      json={"filename": path.name, "content_type": ctype}, timeout=30)
    r.raise_for_status()
    video = r.json()
    print(f"video: {video['id']}")

    # 2. upload bytes
    with open(path, "rb") as f:
        up = requests.put(video["upload"]["url"], data=f,
                          headers={"Content-Type": ctype}, timeout=600)
    up.raise_for_status()

    # 3. complete (metadata probed synchronously)
    r = requests.post(f"{args.base_url}/v1/videos/{video['id']}/complete", headers=H, timeout=60)
    r.raise_for_status()
    print(f"uploaded: duration_s={r.json().get('duration_s')}")

    # 4. submit analysis
    r = requests.post(f"{args.base_url}/v1/analyses", headers=H,
                      json={"video_id": video["id"], "prompt": args.prompt}, timeout=30)
    r.raise_for_status()
    analysis = r.json()
    print(f"analysis: {analysis['id']} status={analysis['status']}")

    # 5. long-poll until terminal
    while analysis["status"] in ("queued", "processing"):
        r = requests.get(f"{args.base_url}/v1/analyses/{analysis['id']}",
                         headers={**H, "Prefer": "wait=30"}, timeout=45)
        r.raise_for_status()
        analysis = r.json()
        print(f"  status={analysis['status']}")

    if analysis["status"] != "completed":
        print(f"failed: {analysis.get('failure_reason')}")
        return 1

    result = analysis["result"]
    print(f"\nanswer:     {result['answer']}")
    print(f"confidence: {result['confidence']}")
    if result.get("detected_count") is not None:
        print(f"count:      {result['detected_count']}")
    for clip in result.get("clips") or []:
        print(f"  clip {clip['start_s']}s–{clip['end_s']}s (conf {clip['confidence']})")
    print(f"billed:     {analysis['usage']['billed_seconds']}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
