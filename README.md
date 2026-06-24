# Hermes ↔ Facebook Messenger bridge

A small, unofficial bridge that lets a Hermes Agent profile reply inside Facebook Messenger group chats using `fbchat-muqit` session emulation.

> Warning: this uses an unofficial Facebook/Messenger client flow. It may violate Meta ToS and can get the Facebook account checkpointed or locked. Use a disposable account.

## What it does

```text
Messenger group  --message-->  bridge.py  --hermes chat-->  Hermes Agent
       ^                                                        |
       +-------------------- send reply -------------------------+
```

- Logs in from exported cookies/appstate, not a password.
- Keeps a separate Hermes continuation session per Messenger thread.
- Can restrict replies to allow-listed thread IDs.
- Supports mention-only mode, anti-spam caps, quiet hours, simple reminders, per-thread notes, and an optional local OpenAI-compatible reasoning shim.

## Quick start

```bash
git clone <your-repo-url>
cd fb-messenger-bridge
python3 -m venv venv
./venv/bin/pip install -r requirements.txt
cp .env.example .env
# edit .env, then put exported Facebook cookies in cookies.json
./run.sh
```

### Export cookies

1. Log into a disposable Facebook account in a browser.
2. Export cookies as JSON using an extension such as C3C FBState or J2TEAM Cookies.
3. The file must be a list of cookie objects with `name`/`key` and `value`; at minimum it needs `c_user` and `xs`.
4. Save as `cookies.json` in this directory, or point `FB_COOKIES` elsewhere.

If your export shape is different:

```bash
./convert_cookies.py exported-cookies.json cookies.json
```

## Configuration

Copy `.env.example` and edit values:

| Variable | Default | Meaning |
|---|---:|---|
| `FB_COOKIES` | `./cookies.json` | Facebook cookie/appstate JSON |
| `FB_TRIGGER` | `bot` | Prefix trigger when mention-only mode is off |
| `FB_MENTION_ONLY` | `1` | Reply only when the bot is @mentioned |
| `FB_MENTION_NAME` | auto | Bot display name to detect textual @mentions |
| `FB_ALLOW_THREADS` | empty | CSV thread IDs allowed to use the bot; empty = all |
| `FB_OWNER_UID` | empty | Facebook UID treated as owner/admin |
| `HERMES_PROFILE` | `fbpublic` | Hermes profile to run for Messenger replies |
| `HERMES_TOOLSETS` | `web` | Hermes toolset allow-list passed with `-t` |
| `HERMES_BIN` | `hermes` | Hermes CLI path |
| `HERMES_TIMEOUT` | `180` | Seconds to wait for the model reply |

## Run with systemd

```bash
sudo cp systemd/fb-messenger-bridge.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now fb-messenger-bridge
sudo journalctl -u fb-messenger-bridge -f
```

Optional local reasoning shim:

```bash
sudo cp systemd/reasoning-shim.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now reasoning-shim
```

## Files intentionally not committed

Runtime/auth/private state is ignored by `.gitignore`: `cookies.json`, `sessions.json`, `people.json`, `thread_members.json`, `group_notes.json`, `allowed_threads.json`, `outbox.json`, logs, backups, `venv/`, and `__pycache__/`.
