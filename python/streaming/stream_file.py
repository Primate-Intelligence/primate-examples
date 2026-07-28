#!/usr/bin/env python3
"""
stream_file.py — stream a video file (or synthetic pattern) through the Primate Intelligence live WebRTC path.

Emulates the exact client profile CoWork's audit used and that Primate's target
market (robotics/edge fleets, CI, datacenter agents) actually has:

  * UDP effectively unusable → the client offers ONLY TURN relay candidates
    (we strip host/srflx from the SDP and never trickle them), preferring
    TURN-over-TCP:443 when available.
  * Full public-API path: POST /v1/streams → mint pvct_ client token →
    signaling WS join/offer/answer/ice → (expect) live → result frames.
  * Streams a real video file (or synthetic test pattern) through the live
    path — the "stream a file" recipe from the docs.

Why relay-only reproduces the datacenter failure:
  A relay-only client creates TURN permissions only toward the candidates the
  SERVER advertises. If the server's answer carries only a private VPC IP
  (172.31.x), the client's checks go nowhere routable AND the server's own
  outbound checks arrive at the relay from an address the client never
  permissioned — so they are dropped. No peer-reflexive rescue (that's the
  browser-on-home-NAT trick). ICE stalls in `checking` forever.

Exit codes:
  0  = session went live and produced >= 1 result frame (PASS)
  10 = ICE never connected (the Gap-1 failure)
  11 = connected but no result frames
  12 = API/signaling error
Audit JSON is written to --json-out for the pass/fail scoring.
"""

import argparse
import asyncio
import json
import re
import sys
import time
from datetime import datetime, timezone

import requests
import websockets
from aiortc import (
    RTCConfiguration,
    RTCIceServer,
    RTCPeerConnection,
    RTCSessionDescription,
    VideoStreamTrack,
)
from aiortc.contrib.media import MediaPlayer
from av import VideoFrame
import numpy as np


def log(msg: str) -> None:
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S.%f')[:-3]}] {msg}", flush=True)


class SyntheticTrack(VideoStreamTrack):
    """640x480 moving-box test pattern @ ~10fps (person-free; answer should be no)."""

    def __init__(self):
        super().__init__()
        self.count = 0

    async def recv(self):
        pts, time_base = await self.next_timestamp()
        img = np.zeros((480, 640, 3), dtype=np.uint8)
        x = (self.count * 8) % 560
        img[200:280, x:x + 80] = (0, 200, 255)
        self.count += 1
        frame = VideoFrame.from_ndarray(img, format="bgr24")
        frame.pts = pts
        frame.time_base = time_base
        return frame


def pick_relay_servers(ice_servers: list, prefer_tcp: bool) -> list[RTCIceServer]:
    """Keep only TURN servers; prefer transport=tcp / turns: when asked."""
    turn, turn_tcp = [], []
    for s in ice_servers or []:
        urls = s.get("urls") or s.get("url")
        if urls is None:
            continue
        if isinstance(urls, str):
            urls = [urls]
        t = [u for u in urls if u.startswith(("turn:", "turns:"))]
        if not t:
            continue
        entry = RTCIceServer(
            urls=t,
            username=s.get("username"),
            credential=s.get("credential"),
        )
        turn.append(entry)
        tcp_urls = [u for u in t if "transport=tcp" in u or u.startswith("turns:")]
        if tcp_urls:
            turn_tcp.append(RTCIceServer(urls=tcp_urls, username=s.get("username"), credential=s.get("credential")))
    if prefer_tcp and turn_tcp:
        return turn_tcp
    return turn


RELAY_RE = re.compile(r"^a=candidate:\S+ \d+ \S+ \d+ \S+ \d+ typ relay", re.I)


def strip_non_relay(sdp: str) -> tuple[str, int, int]:
    """Remove every a=candidate line that is not typ relay. Returns (sdp, kept, dropped)."""
    kept = dropped = 0
    out = []
    for line in sdp.split("\r\n"):
        if line.startswith("a=candidate:"):
            if RELAY_RE.match(line):
                kept += 1
                out.append(line)
            else:
                dropped += 1
            continue
        out.append(line)
    return "\r\n".join(out), kept, dropped


def classify_candidates(sdp: str) -> dict:
    """Audit the remote SDP: what candidate types did the server advertise?"""
    kinds = {"host": [], "srflx": [], "relay": [], "prflx": [], "other": []}
    for line in sdp.split("\r\n"):
        if not line.startswith("a=candidate:"):
            continue
        m = re.search(r"typ (\S+)", line)
        kind = m.group(1) if m else "other"
        addr = line.split()[4] if len(line.split()) > 4 else "?"
        kinds.setdefault(kind, kinds["other"]).append(addr)
    private = [a for a in kinds["host"] if a.startswith(("10.", "172.", "192.168.", "169.254."))]
    public_reachable = (
        [a for a in kinds["host"] if a not in private] + kinds["srflx"] + kinds["relay"]
    )
    return {
        "host": kinds["host"], "srflx": kinds["srflx"], "relay": kinds["relay"],
        "private_only": len(public_reachable) == 0 and len(kinds["host"]) > 0,
        "public_or_relayed": public_reachable,
    }


async def run(args) -> int:
    base = args.base_url.rstrip("/")
    H = {"Authorization": f"Bearer {args.api_key}", "Content-Type": "application/json"}
    audit = {
        "meta": {"base_url": base, "started_at": datetime.now(timezone.utc).isoformat(),
                 "profile": "relay-only" if args.relay_only else "unrestricted",
                 "source": args.file or "synthetic"},
        "steps": [],
    }

    def step(name, **kw):
        audit["steps"].append({"name": name, "t": time.time(), **kw})
        log(f"STEP {name} {json.dumps(kw, default=str)[:300]}")

    # ── 1. Create stream ────────────────────────────────────────────────────
    r = requests.post(f"{base}/v1/streams", headers=H,
                      json={"prompt": args.prompt}, timeout=30)
    if r.status_code != 201:
        step("create_stream_failed", status=r.status_code, body=r.text[:500])
        return finish(audit, args, 12)
    stream = r.json()
    sid = stream["id"]
    step("create_stream", id=sid, status=stream["status"],
         ice_server_count=len(stream.get("ice_servers") or []),
         relay_urls=[u for s in (stream.get("ice_servers") or [])
                     for u in ([s.get("urls")] if isinstance(s.get("urls"), str) else (s.get("urls") or []))
                     if u.startswith(("turn:", "turns:"))])

    # ── 2. Mint client token ────────────────────────────────────────────────
    r = requests.post(f"{base}/v1/client_tokens", headers=H,
                      json={"scopes": ["streams:signal"], "stream_id": sid, "ttl_s": 600}, timeout=30)
    if r.status_code != 201:
        step("client_token_failed", status=r.status_code, body=r.text[:300])
        return finish(audit, args, 12)
    pvct = r.json()["token"]
    step("client_token", ok=True)

    # ── 3. Peer connection (relay-only profile) ─────────────────────────────
    servers = []
    if args.relay_only:
        servers = pick_relay_servers(stream.get("ice_servers") or [], prefer_tcp=args.turn_tcp)
        if not servers:
            step("no_turn_servers_in_response")
            return finish(audit, args, 12)
    else:
        for s in stream.get("ice_servers") or []:
            urls = s.get("urls") or s.get("url")
            servers.append(RTCIceServer(urls=urls, username=s.get("username"), credential=s.get("credential")))
    pc = RTCPeerConnection(RTCConfiguration(iceServers=servers))

    if args.file:
        player = MediaPlayer(args.file, loop=True)
        pc.addTrack(player.video)
    else:
        pc.addTrack(SyntheticTrack())
    dc = pc.createDataChannel("control")

    states = {"ice": [], "conn": []}

    @pc.on("iceconnectionstatechange")
    def _():
        states["ice"].append({"t": time.time(), "s": pc.iceConnectionState})
        log(f"ice: {pc.iceConnectionState}")

    @pc.on("connectionstatechange")
    def _():
        states["conn"].append({"t": time.time(), "s": pc.connectionState})
        log(f"conn: {pc.connectionState}")

    # ── 4. Signaling ────────────────────────────────────────────────────────
    ws_url = stream["signaling"]["url"] + f"?token={pvct}"
    if ws_url.startswith("https://"):
        ws_url = "wss://" + ws_url[len("https://"):]
    elif ws_url.startswith("http://"):
        ws_url = "ws://" + ws_url[len("http://"):]

    live = asyncio.Event()
    results = []
    end_info = {}
    server_trickle = []

    async with websockets.connect(ws_url, open_timeout=20) as ws:
        async def send(o):
            await ws.send(json.dumps(o))

        await send({"type": "join"})

        async def reader():
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except Exception:
                    continue
                t = msg.get("type")
                if t == "ready":
                    step("ready")
                    # createOffer AFTER ready; gather fully, then strip non-relay.
                    offer = await pc.createOffer()
                    await pc.setLocalDescription(offer)
                    # aiortc gathers before returning; strip host/srflx if relay-only
                    sdp = pc.localDescription.sdp
                    if args.relay_only:
                        sdp, kept, dropped = strip_non_relay(sdp)
                        step("offer_sanitized", relay_kept=kept, non_relay_dropped=dropped)
                        if kept == 0:
                            step("no_relay_candidates_gathered")
                    await send({"type": "offer", "sdp": sdp})
                elif t == "queued":
                    step("queued", position=msg.get("position"))
                elif t == "answer":
                    sdp = msg["sdp"]
                    ans_audit = classify_candidates(sdp)
                    audit["server_answer_candidates"] = ans_audit
                    step("answer", **ans_audit)
                    await pc.setRemoteDescription(RTCSessionDescription(sdp=sdp, type="answer"))
                elif t == "ice":
                    cand = msg.get("candidate") or {}
                    server_trickle.append(cand)
                    step("server_trickle_ice", candidate=str(cand.get("candidate", ""))[:120])
                    try:
                        from aiortc.sdp import candidate_from_sdp
                        c = candidate_from_sdp(str(cand.get("candidate", "")).replace("candidate:", "", 1))
                        c.sdpMid = cand.get("sdpMid")
                        c.sdpMLineIndex = cand.get("sdpMLineIndex")
                        await pc.addIceCandidate(c)
                    except Exception as e:
                        log(f"trickle add failed: {e}")
                elif t == "live":
                    step("live")
                    live.set()
                elif t == "result":
                    results.append(msg)
                    if len(results) == 1:
                        step("first_result", frame_num=msg.get("frame_num"))
                elif t == "metering":
                    pass
                elif t in ("end", "error"):
                    end_info.update(msg)
                    step("ws_terminal", **{k: v for k, v in msg.items() if k != "type"}, msg_type=t)
                    return

        reader_task = asyncio.create_task(reader())
        try:
            await asyncio.wait_for(live.wait(), timeout=args.connect_timeout)
            # live! hold for the observation window
            await asyncio.sleep(args.live_seconds)
            await send({"type": "end"})
            await asyncio.wait_for(reader_task, timeout=10)
        except asyncio.TimeoutError:
            step("timeout_waiting_for_live", waited_s=args.connect_timeout,
                 ice_states=[s["s"] for s in states["ice"]],
                 server_trickled=len(server_trickle))
        finally:
            reader_task.cancel()
            await pc.close()

    # ── 5. Fetch terminal resource — audit end_reason honesty ───────────────
    time.sleep(2)
    r = requests.get(f"{base}/v1/streams/{sid}", headers=H, timeout=30)
    final = r.json() if r.ok else {"fetch_failed": r.status_code}
    audit["final_resource"] = final
    audit["ws_end"] = end_info
    audit["ice_states"] = states
    audit["result_frames"] = len(results)
    audit["sample_results"] = results[:3] + results[-2:] if results else []
    audit["server_trickled_candidates"] = server_trickle

    went_live = live.is_set()
    step("final", went_live=went_live, frames=len(results),
         end_reason=final.get("end_reason"), billed=final.get("usage", {}).get("billed_seconds") if isinstance(final.get("usage"), dict) else None)

    # end_reason honesty check (Gap 2)
    audit["end_reason_honest"] = None
    if not went_live:
        er = final.get("end_reason")
        audit["end_reason_honest"] = er not in ("completed", None)

    if went_live and len(results) > 0:
        return finish(audit, args, 0)
    if not went_live:
        return finish(audit, args, 10)
    return finish(audit, args, 11)


def finish(audit, args, code):
    audit["exit_code"] = code
    audit["verdict"] = {0: "PASS", 10: "FAIL_ICE_NEVER_CONNECTED",
                        11: "FAIL_NO_RESULTS", 12: "FAIL_API"}[code]
    if args.json_out:
        with open(args.json_out, "w") as f:
            json.dump(audit, f, indent=2, default=str)
    log(f"VERDICT: {audit['verdict']}")
    return code


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--base-url", default="https://api.dev.primateintelligence.ai")
    p.add_argument("--api-key", required=True)
    p.add_argument("--prompt", default="can you see a person?")
    p.add_argument("--file", default=None, help="video file to stream (default: synthetic pattern)")
    p.add_argument("--relay-only", action="store_true", default=True)
    p.add_argument("--no-relay-only", dest="relay_only", action="store_false")
    p.add_argument("--turn-tcp", action="store_true", help="prefer TURN over TCP/turns:443")
    p.add_argument("--connect-timeout", type=float, default=40)
    p.add_argument("--live-seconds", type=float, default=20)
    p.add_argument("--json-out", default=None)
    args = p.parse_args()
    sys.exit(asyncio.run(run(args)))


if __name__ == "__main__":
    main()
