<!-- gitnexus:start -->
# GitNexus — Code Intelligence

This project is indexed by GitNexus as **fb-messenger-bridge**. Use the GitNexus MCP tools to understand code, assess impact, and navigate safely.

> Index stale? Run `node .gitnexus/run.cjs analyze` from the project root — it auto-selects an available runner. No `.gitnexus/run.cjs` yet? `npx gitnexus analyze` (npm 11 crash → `npm i -g gitnexus`; #1939).

## Always Do

- **MUST run impact analysis before editing any symbol.** Before modifying a function, class, or method, run `impact({target: "symbolName", direction: "upstream"})` and report the blast radius (direct callers, affected processes, risk level) to the user.
- **MUST run `detect_changes()` before committing** to verify your changes only affect expected symbols and execution flows. For regression review, compare against the default branch: `detect_changes({scope: "compare", base_ref: "main"})`.
- **MUST warn the user** if impact analysis returns HIGH or CRITICAL risk before proceeding with edits.
- When exploring unfamiliar code, use `query({search_query: "concept"})` to find execution flows instead of grepping. It returns process-grouped results ranked by relevance.
- When you need full context on a specific symbol — callers, callees, which execution flows it participates in — use `context({name: "symbolName"})`.
- For security review, `explain({target: "fileOrSymbol"})` lists taint findings (source→sink flows; needs `analyze --pdg`).

## Never Do

- NEVER edit a function, class, or method without first running `impact` on it.
- NEVER ignore HIGH or CRITICAL risk warnings from impact analysis.
- NEVER rename symbols with find-and-replace — use `rename` which understands the call graph.
- NEVER commit changes without running `detect_changes()` to check affected scope.

## Resources

| Resource | Use for |
|----------|---------|
| `gitnexus://repo/fb-messenger-bridge/context` | Codebase overview, check index freshness |
| `gitnexus://repo/fb-messenger-bridge/clusters` | All functional areas |
| `gitnexus://repo/fb-messenger-bridge/processes` | All execution flows |
| `gitnexus://repo/fb-messenger-bridge/process/{name}` | Step-by-step execution trace |

## CLI

| Task | Read this skill file |
|------|---------------------|
| Understand architecture / "How does X work?" | `.claude/skills/gitnexus/gitnexus-exploring/SKILL.md` |
| Blast radius / "What breaks if I change X?" | `.claude/skills/gitnexus/gitnexus-impact-analysis/SKILL.md` |
| Trace bugs / "Why is X failing?" | `.claude/skills/gitnexus/gitnexus-debugging/SKILL.md` |
| Rename / extract / split / refactor | `.claude/skills/gitnexus/gitnexus-refactoring/SKILL.md` |
| Tools, resources, schema reference | `.claude/skills/gitnexus/gitnexus-guide/SKILL.md` |
| Index, status, clean, wiki CLI commands | `.claude/skills/gitnexus/gitnexus-cli/SKILL.md` |

<!-- gitnexus:end -->

<!-- static-context:start -->
# Portable repo context (works without local GitNexus index)

The `.gitnexus/` database is intentionally **not committed**. Fresh clones should run:

```bash
npx --yes gitnexus@latest analyze --no-stats --name fb-messenger-bridge
```

Until that local index exists, use this static map to avoid reading the whole repo.

## File map

| Path | Purpose |
|---|---|
| `bridge.py` | Main Facebook Messenger ↔ Hermes bridge. Handles login, message gating, session persistence, notes, reminders, member cache, outbox, vision, and reply sending. |
| `reasoning_shim.py` | Optional OpenAI-compatible HTTP proxy. Rewrites `reasoning_effort` per request so simple chat is fast and tool/long/reasoning turns can still think. |
| `fb_enqueue.py` | Appends outbound messages to `outbox.json`; the live bridge polls and sends them from the existing Messenger session. |
| `convert_cookies.py` | Converts browser cookie exports into `cookies.json` format expected by `fbchat-muqit`. |
| `seed_person.py` | Local admin helper for owner-seeded per-person notes. Imports `bridge.py` storage helpers. |
| `run.sh` | Loads `.env`, sets defaults, runs `bridge.py`. |
| `systemd/` | Example services for the bridge and reasoning shim. |

## Critical flows

### Incoming Messenger message → Hermes reply

1. `Bridge.on_message(event_data)` receives Messenger events from `fbchat-muqit`.
2. It ignores self-messages and empty non-image messages.
3. It stashes recent bubbles for “note this/link/map” fallback and stitches split image/question bubbles.
4. It decides whether the bot is addressed:
   - reply to bot message;
   - `FB_MENTION_ONLY=1`: resolve `@Name` tokens against cached thread members;
   - otherwise prefix trigger via `FB_TRIGGER`.
5. It enforces allow-list / owner auto-join behavior, quiet hours, reaction-only ignore, anti-spam/flood/debounce guards.
6. It builds prompt context: quoted reply, roster, group notes, clock line, sender notes, tagged-user notes, optional pre-search/web snippets, image captions.
7. It calls `ask_hermes(...)`.
8. `ask_hermes` runs `hermes -p <HERMES_PROFILE> chat -Q -t <HERMES_TOOLSETS> --source fb-messenger`, resuming per-thread sessions from `sessions.json` unless age/turn caps roll a fresh session.
9. The bridge strips/stores hidden markers (`[[NHỚ:]]`, `[[SỔ:]]`, `[[NHẮC:]]`), schedules notes/reminders, caps/scrubs output, then sends chunks back with `send_message`.

Key symbols: `Bridge.on_message`, `ask_hermes`, `_wrap_untrusted`, `_extract_and_store_notes`, `_extract_and_store_group_notes`, `_extract_and_store_reminders`, `_scrub_output`.

### Background loops started when listening

`Bridge.on_listening` starts long-running tasks:

- cookie persistence: `_cookie_flush_loop` / `_persist_cookies` keep rotated Facebook cookies fresh;
- member cache: `_members_loop` / `_refresh_members` map thread UID → real name for mention resolution;
- announcements: `_announce_loop` / `_announce_sweep` / `_gen_greeting` optionally send goodnight/morning lines;
- external delivery: `_outbox_loop` sends `outbox.json` items created by `fb_enqueue.py`;
- reminders: `_reminder_loop` fires scheduled reminders into the thread.

### Reasoning shim flow

1. `reasoning_shim.Handler.do_POST/do_GET` forwards requests to `SHIM_UPSTREAM`.
2. For `/chat/completions`, `_forward` parses the JSON request.
3. `decide_effort` sets `reasoning_effort`:
   - `medium`/`SHIM_HARD_EFFORT` when tool context is present, the last user turn is long, or cue words indicate reasoning/debug/analysis;
   - `none` for short/simple chat.
4. The modified request is sent upstream; response is returned unchanged.

Key symbols: `Handler._forward`, `decide_effort`, `_has_tool_context`, `_last_user_text`.

## Runtime/private files — never commit

The repo is designed to keep these local-only: `cookies.json*`, `sessions.json*`, `people.json*`, `thread_members.json*`, `group_notes.json*`, `allowed_threads*.json`, `outbox.json`, `announce_state.json`, logs, `.env`, `venv/`, and `.gitnexus/`.

## Safe edit checklist

1. Prefer GitNexus first when available: `query`, `context`, `impact`, then read only relevant source spans.
2. Before changing `bridge.py`, identify whether the change touches reply gating, note persistence, reminder scheduling, session management, or send rate/caps.
3. After edits, run at least:
   - `python3 -m py_compile bridge.py reasoning_shim.py fb_enqueue.py convert_cookies.py seed_person.py`
   - targeted helper tests if relevant.
4. Before commit, audit with:

```bash
git status --short
git diff
grep -RIn --exclude-dir=.git --exclude-dir=.gitnexus --exclude-dir=__pycache__ \
  -E 'cookies|token|password|FB_OWNER_UID=.+|[0-9]{12,20}|100\.[0-9]+\.[0-9]+\.[0-9]+' . || true
```
<!-- static-context:end -->

