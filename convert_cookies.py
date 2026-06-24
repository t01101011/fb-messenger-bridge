#!/usr/bin/env python3
"""Convert J2TEAM / C3C cookie export to the flat list fbchat-muqit needs."""
import json
import os
import sys


def main():
    if len(sys.argv) < 2:
        print("usage: convert_cookies.py <exported-cookies.json> [cookies.json]", file=sys.stderr)
        sys.exit(2)
    src = sys.argv[1]
    dst = sys.argv[2] if len(sys.argv) > 2 else "cookies.json"

    d = json.load(open(src, encoding="utf-8"))

    if isinstance(d, dict) and isinstance(d.get("cookies"), list):
        cookies = d["cookies"]
    elif isinstance(d, list):
        cookies = d
    else:
        print("UNEXPECTED shape:", type(d).__name__, list(d.keys())[:10] if isinstance(d, dict) else "")
        sys.exit(1)

    # normalize: ensure each has name+value (loader accepts name OR key)
    flat = []
    for c in cookies:
        name = c.get("name") or c.get("key")
        val = c.get("value")
        if not name or val is None:
            continue
        item = dict(c)
        item["name"] = name
        item["value"] = val
        flat.append(item)

    names = {c["name"] for c in flat}
    print("cookies:", len(flat), "| has c_user:", "c_user" in names, "| has xs:", "xs" in names)

    if "c_user" not in names or "xs" not in names:
        print("MISSING c_user or xs — cookies won't authenticate")
        sys.exit(2)

    with open(dst, "w", encoding="utf-8") as f:
        json.dump(flat, f, ensure_ascii=False)
    os.chmod(dst, 0o600)
    print("wrote", dst)


if __name__ == "__main__":
    main()
