# Primate Intelligence — Examples

Working, runnable examples for the [Primate Intelligence Public API](https://api.primateintelligence.ai/llms.txt).

- **Docs (agent guide):** https://api.primateintelligence.ai/docs/agents.md
- **OpenAPI spec:** https://api.primateintelligence.ai/v1/openapi.json
- **Changelog:** https://api.primateintelligence.ai/docs/changelog.md

## Get a key (zero-human, ~10 seconds)

```bash
# 1. Sandbox key (no auth, fixture results)
curl -s -X POST https://api.primateintelligence.ai/v1/sandbox

# 2. Upgrade to a free live key (6,000s GPU credit, no card)
curl -s -X POST https://api.primateintelligence.ai/v1/keys/upgrade \
  -H "Authorization: Bearer ***"
```

## Examples

| Example | What it shows |
|---|---|
| [`python/streaming/stream_file.py`](python/streaming/stream_file.py) | **Stream a video file through the live WebRTC path** (aiortc, no browser). Full protocol: create → client token → signaling WS → offer/answer/trickle ICE → live → per-frame results → honest end_reason. Includes a `--relay-only` mode that emulates datacenter/CI/UDP-blocked clients (TURN-over-TCP) — the same harness Primate runs as a deploy regression test. Writes a complete session audit to JSON. |
| [`python/streaming/requirements.txt`](python/streaming/requirements.txt) | Pinned deps for the streaming client. |
| [`python/analyses/analyze_file.py`](python/analyses/analyze_file.py) | File upload → analysis → poll result. The simplest end-to-end path. |
| [`browser/webcam.html`](browser/webcam.html) | **Webcam streaming from a browser** — single self-contained HTML file. Create a stream server-side, paste the stream JSON + client token, click start. Shows live answers + a full protocol log on-page. |

## Streaming quick reference

```
POST /v1/streams {prompt}            → signaling.url + ice_servers + limits
POST /v1/client_tokens {streams:signal, stream_id} → pvct_ token (never use secret keys in clients)
WS   signaling.url?token=***       → join → ready → offer/answer + trickle ICE (both directions)
                                      → live → result{frame_num, detections} → end{reason}
```

Result rows use the **same contract as file analyses**: `answer: "yes"|"no"|"indeterminate"` (lowercase), `confidence: 0..1`, `prompt` echoed exactly as submitted. Latency telemetry lives under `timing`.

`end_reason` is honest: a session that never went live is never `"completed"` — expect `ice_failed` (with a `failure_diagnostic` candidate summary) or `media_timeout` on transport failures, and exactly `billed_seconds: 0` for them.

## Running the streaming example

```bash
cd python/streaming
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt

# Stream a file with the datacenter (relay-only, TURN-over-TCP) profile:
.venv/bin/python stream_file.py \
  --api-key "$PRIMATE_API_KEY" \
  --file path/to/video.mp4 \
  --prompt "can you see a person?" \
  --turn-tcp --json-out audit.json

# Or with full candidate freedom (typical server/edge deployment):
.venv/bin/python stream_file.py --api-key "$PRIMATE_API_KEY" --no-relay-only \
  --file path/to/video.mp4 --prompt "can you see a person?"
```

Exit code 0 = went live and produced results. 10 = ICE failed. 11 = connected but no results. 12 = API error. The JSON audit contains the server's answer SDP candidate classification, ICE state timeline, sample results, final resource, and billing — everything needed to file a precise bug report (or to gate your CI).

## License

MIT — use these freely as starting points.
