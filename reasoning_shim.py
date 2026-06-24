#!/usr/bin/env python3
"""
reasoning-shim: a tiny HTTP proxy that sits between Hermes (fbpublic profile)
and the LM Studio server, and decides PER-REQUEST whether the local Gemma model
should "think" (reasoning_effort=medium) or answer fast (reasoning_effort=none).

WHY: Hermes sends reasoning_effort="medium" to LM Studio for the a local reasoning-capable model
model regardless of the profile's `model.reasoning_effort: none` (a known Hermes
quirk: the QAT GGUF publishes no reasoning capability, so Hermes mis-handles the
field). With thinking ON, every group reply burns ~700 reasoning tokens => 6-9s.
Most group chatter is trivial and gains nothing from thinking. This shim forces
thinking OFF for simple turns (fast ~2s) and leaves it ON only for turns that
actually look hard. ZERO extra model/LLM calls — classification is pure regex.

This is a dumb forwarder to an OpenAI-compatible local server.

Config via env:
  SHIM_UPSTREAM   LM Studio base, default http://127.0.0.1:1234
  SHIM_PORT       listen port, default 9877
  SHIM_HOST       listen host, default 127.0.0.1
  SHIM_HARD_EFFORT  override effort for the "hard" branch, default "medium"
"""
import http.server
import json
import os
import re
import sys
import urllib.request

UPSTREAM = os.environ.get("SHIM_UPSTREAM", "http://127.0.0.1:1234").rstrip("/")
PORT = int(os.environ.get("SHIM_PORT", "9877"))
HOST = os.environ.get("SHIM_HOST", "127.0.0.1")
HARD_EFFORT = os.environ.get("SHIM_HARD_EFFORT", "medium")
FAST_EFFORT = "none"

# --- Heuristic: does this turn warrant the model thinking? ----------------
# Signals that a turn is HARD (worth thinking):
#   - the model is about to synthesize AFTER a tool call (a `tool` message is
#     present, or the last assistant message requested a tool_call) — reasoning
#     over fetched data benefits from thinking.
#   - the user's last message is long, or contains "reasoning" cue words
#     (why/how/compare/explain/analyze/calculate/code/debug...), in VI + EN.
# Everything else (short greetings, chit-chat, one-liners) => FAST.

_HARD_CUES = re.compile(
    r"\b(tại sao|vì sao|vì\s*đâu|so sánh|giải thích|phân tích|chứng minh|"
    r"tính toán|tính giúp|bao nhiêu|khác nhau|ưu nhược|đánh giá|"
    r"viết code|sửa lỗi|debug|thuật toán|công thức|"
    r"why|how come|compare|explain|analy[sz]e|calculate|prove|algorithm|"
    r"step by step|từng bước|lập luận|suy luận|tóm tắt|summari[sz]e)\b",
    re.IGNORECASE,
)
# A question that's reasonably long is more likely to need care.
_LONG_USER_CHARS = int(os.environ.get("SHIM_LONG_CHARS", "180"))


def _last_user_text(messages: list) -> str:
    for m in reversed(messages):
        if m.get("role") == "user":
            c = m.get("content", "")
            if isinstance(c, list):  # multimodal content array
                c = " ".join(
                    p.get("text", "") for p in c if isinstance(p, dict)
                )
            return c or ""
    return ""


def _has_tool_context(messages: list) -> bool:
    """True if a tool result is in the convo (model must reason over it) or the
    most recent assistant turn issued tool_calls."""
    for m in messages:
        if m.get("role") == "tool":
            return True
        if m.get("role") == "assistant" and m.get("tool_calls"):
            return True
    return False


def decide_effort(body: dict) -> str:
    """Return the reasoning_effort to send upstream for this request."""
    msgs = body.get("messages", []) or []
    if _has_tool_context(msgs):
        return HARD_EFFORT
    user = _last_user_text(msgs)
    if len(user) >= _LONG_USER_CHARS:
        return HARD_EFFORT
    if _HARD_CUES.search(user):
        return HARD_EFFORT
    return FAST_EFFORT


class Handler(http.server.BaseHTTPRequestHandler):
    def _forward(self, method: str):
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length) if length else b""
        out = raw
        decided = None
        if "chat/completions" in self.path and raw:
            try:
                body = json.loads(raw)
                decided = decide_effort(body)
                body["reasoning_effort"] = decided
                out = json.dumps(body, ensure_ascii=False).encode("utf-8")
            except Exception as e:
                sys.stderr.write(f"[shim] parse/modify err: {e!r}\n")
                out = raw
        req = urllib.request.Request(UPSTREAM + self.path, data=out, method=method)
        for k, v in self.headers.items():
            if k.lower() not in ("host", "content-length"):
                req.add_header(k, v)
        if decided is not None:
            sys.stderr.write(f"[shim] effort={decided} path={self.path}\n")
            sys.stderr.flush()
        try:
            r = urllib.request.urlopen(req, timeout=180)
            data = r.read()
            self.send_response(r.status)
            for k, v in r.headers.items():
                if k.lower() not in ("transfer-encoding", "content-length", "connection"):
                    self.send_header(k, v)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        except Exception as e:
            sys.stderr.write(f"[shim] upstream err: {e!r}\n")
            self.send_response(502)
            self.end_headers()
            self.wfile.write(str(e).encode())

    def do_POST(self):
        self._forward("POST")

    def do_GET(self):
        self._forward("GET")

    def log_message(self, *a):
        pass


def main():
    srv = http.server.ThreadingHTTPServer((HOST, PORT), Handler)
    sys.stderr.write(f"[shim] listening {HOST}:{PORT} -> {UPSTREAM} "
                     f"(hard={HARD_EFFORT}, fast={FAST_EFFORT})\n")
    sys.stderr.flush()
    srv.serve_forever()


if __name__ == "__main__":
    main()
