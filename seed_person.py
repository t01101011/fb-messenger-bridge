#!/usr/bin/env python3
"""Seed owner-locked notes about a user, BEFORE they're added to a group.

Locked notes are used by the bot SILENTLY (it acts on them but never reads them
back / admits having them), are never auto-trimmed, and the user can't make the
bot forget them — only editing people.json (or this tool) changes them.

Usage:
  ./seed_person.py <UID> "ghi chú 1" "ghi chú 2" ...
  ./seed_person.py <UID> --name "Khoa" "hay dìm hàng, cẩn thận" "là designer"
  ./seed_person.py <UID> --show          # xem hồ sơ hiện tại của UID
  ./seed_person.py --list                # liệt kê tất cả UID đã có hồ sơ
  ./seed_person.py <UID> --unlock "chuỗi" # gỡ 1 locked note (chỉ owner)

Get a user's UID by having them message the group once and reading the
`from=<uid>` in service.log, or from their facebook.com profile id.
"""
import sys
import bridge   # reuse the bridge's people store + helpers


def main():
    args = sys.argv[1:]
    if not args:
        print(__doc__)
        sys.exit(1)

    if args[0] == "--list":
        people = bridge._load_people()
        if not people:
            print("(chưa có hồ sơ ai)")
            return
        for uid, p in people.items():
            print(f"{uid}: name={p.get('name','')!r} "
                  f"locked={len(p.get('locked',[]))} soft={len(p.get('notes',[]))}")
        return

    uid = args[0]
    rest = args[1:]

    if "--show" in rest:
        p = bridge._load_people().get(uid)
        if not p:
            print(f"(chưa có hồ sơ cho {uid})")
            return
        import json
        print(json.dumps(p, ensure_ascii=False, indent=2))
        return

    name = ""
    if "--name" in rest:
        i = rest.index("--name")
        name = rest[i + 1]
        rest = rest[:i] + rest[i + 2:]

    if "--unlock" in rest:
        i = rest.index("--unlock")
        target = rest[i + 1]
        people = bridge._load_people()
        p = people.get(uid)
        if not p:
            print(f"(không có hồ sơ {uid})")
            return
        before = len(p.get("locked", []))
        p["locked"] = [n for n in p.get("locked", []) if target.lower() not in n.lower()]
        bridge._save_people(people)
        print(f"gỡ {before - len(p['locked'])} locked note khớp {target!r}")
        return

    notes = [a for a in rest if a.strip()]
    if not notes and not name:
        print("Cần ít nhất 1 ghi chú hoặc --name. Xem --help.")
        sys.exit(1)
    p = bridge.seed_locked(uid, notes, name=name)
    print(f"✓ seeded cho {uid}: name={p.get('name','')!r}, "
          f"locked={len(p['locked'])} note(s)")
    for n in p["locked"]:
        print(f"   🔒 {n}")


if __name__ == "__main__":
    main()
