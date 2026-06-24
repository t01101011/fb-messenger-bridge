#!/usr/bin/env python3
"""Enqueue a message for the live FB bridge to deliver into a group thread.
Usage:
  echo "message body" | ./venv/bin/python fb_enqueue.py <thread_id>
  ./venv/bin/python fb_enqueue.py <thread_id> "message body"
  ./venv/bin/python fb_enqueue.py <thread_id> "... @Example Name ..." --mention <uid>:@Example Name

--mention <uid>:<@text> tags a real user: the script locates <@text> in the
body and records its offset/length so the bridge sends a true FB @mention
(notification ping). Repeatable for multiple mentions.

The live bridge process polls outbox.json (~15s) and sends pending items into
their thread, then marks them sent. This avoids opening a second FB session
(which would kick the bridge's MQTT connection).
"""
import json
import os
import sys
import time
import uuid


_DIR = os.path.dirname(os.path.abspath(__file__))
_OUTBOX = os.path.join(_DIR, "outbox.json")


def main():
    args = sys.argv[1:]
    mention_specs = []
    rest = []
    i = 0
    while i < len(args):
        if args[i] == "--mention" and i + 1 < len(args):
            mention_specs.append(args[i + 1])
            i += 2
        else:
            rest.append(args[i])
            i += 1
    if len(rest) < 1:
        print("usage: fb_enqueue.py <thread_id> [text] [--mention uid:@Name ...]",
              file=sys.stderr)
        sys.exit(2)
    thread_id = rest[0].strip()
    text = rest[1] if len(rest) > 1 else sys.stdin.read()
    text = (text or "").strip()
    if not thread_id or not text:
        print("error: empty thread_id or text", file=sys.stderr)
        sys.exit(2)

    mentions = []
    for spec in mention_specs:
        uid, _, mtext = spec.partition(":")
        uid = uid.strip()
        mtext = mtext.strip()
        if not uid or not mtext:
            print(f"error: bad --mention spec {spec!r} (want uid:@Name)", file=sys.stderr)
            sys.exit(2)
        off = text.find(mtext)
        if off < 0:
            print(f"error: mention text {mtext!r} not found in body", file=sys.stderr)
            sys.exit(2)
        mentions.append({"user_id": uid, "offset": off,
                         "length": len(mtext), "name": mtext.lstrip("@")})

    try:
        with open(_OUTBOX, encoding="utf-8") as f:
            items = json.load(f)
            if not isinstance(items, list):
                items = []
    except (FileNotFoundError, json.JSONDecodeError):
        items = []
    item = {
        "id": uuid.uuid4().hex[:12],
        "thread_id": thread_id,
        "text": text,
        "queued_at": time.time(),
        "sent_at": None,
    }
    if mentions:
        item["mentions"] = mentions
    items.append(item)
    tmp = _OUTBOX + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)
    os.replace(tmp, _OUTBOX)
    print(f"queued id={item['id']} thread={thread_id} ({len(text)} chars, "
          f"{len(mentions)} mention(s))")


if __name__ == "__main__":
    main()
