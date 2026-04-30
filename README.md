# 🎙️ Discord Voice Stats Bot

A Discord bot that tracks **voice channel activity** for small servers and surfaces the data through slash commands — leaderboards, charts, personal dashboards, rivalry cards, and more.

Built with [discord.py](https://github.com/Rapptz/discord.py) and [aiosqlite](https://pypi.org/project/aiosqlite/).
Designed to run on a **Raspberry Pi** (or any Linux host) as a systemd service.

---

## ✨ Features

- Tracks every voice join, leave, and channel switch into a local SQLite database
- Ignores time spent in the AFK channel (configurable)
- Slash commands restricted to designated channels
- Admin-only guard on privacy-sensitive commands
- Real-time milestone announcements (1h, 5h, 10h … 10 000h)
- Automatic monthly recap embed posted on the 1st of each month
- Graceful shutdown — flushes open sessions on SIGTERM/SIGINT
- Startup reconciliation — caps orphaned sessions after ungraceful shutdowns (power cut, crash)

---

## 📋 Commands

Commands default to `ephemeral: false` (public) unless marked otherwise. The `private` flag is only honoured for the designated `VOICE_TOP_PRIVATE_USER`.

### Personal stats
| Command | Description |
|---|---|
| `/voice_me` | Your personal dashboard — lifetime, last 7d, last 30d, server rank, daily streak, and who you've spent the most time with this month. Always ephemeral. |
| `/voice_report [days]` | Your total voice time in the last N days (default 7). Ephemeral. |
| `/voice_total` | Your all-time lifetime voice time. Ephemeral. |
| `/voice_history [private]` | Your last 10 sessions with timestamps, channel names, and durations. Marks any ongoing session. |

### Server leaderboards
| Command | Description |
|---|---|
| `/voice_top [days] [private]` | Top 50 users by voice time in the last N days (default 7). |
| `/voice_solo [days] [private]` | Top 50 users by time spent *alone* in a channel (occupancy = 1). |
| `/voice_bestfriends [days] [private]` | Top 10 pairs of users by shared voice time — who hangs out together most. |
| `/voice_marathon [days] [private]` | Top 10 longest single sessions on the server, with channel and timestamp. |
| `/voice_night_owl [days] [start_hour] [end_hour] [private]` | Leaderboard for time spent in voice during late-night hours (default 23:00–04:00, wraps midnight). |
| `/voice_ghost [private]` | Members who have been absent from voice the longest — calls them out by name. |

### Head-to-head
| Command | Description |
|---|---|
| `/voice_rivalry @opponent [days] [private]` | Side-by-side embed comparing you vs another member — lifetime, recent, solo time, streak, rank, and time spent together. Winner of each stat is bolded. |
| `/voice_together @user1 @user2 [private]` | *Admin only.* Total time two specific members have spent in voice together. |

### Server-wide charts
All chart commands produce a PNG bar chart. Set `public: false` to post privately.

| Command | Description |
|---|---|
| `/voice_heatmap [days] [public]` | Total voice hours by hour of day (00–23). Good for seeing when the server is most active. |
| `/voice_weekdays [days] [public]` | Total voice hours by day of week (Mon–Sun). |
| `/voice_daily [days] [public]` | Total server voice hours per day (trend). Requires admin for >7 days. |
| `/voice_daily_unique [days] [public]` | Unique participants per day. Requires admin for >7 days. |
| `/voice_peak [days] [public]` | Peak concurrent users per day. Requires admin for >7 days. |
| `/voice_channel_stats [days] [private]` | Per-channel total hours and unique user count, ranked. |

### Admin / utility
| Command | Description |
|---|---|
| `/pi_storage [path]` | Shows disk usage for a given path on the host (default `/`). Restricted to `VOICE_TOP_PRIVATE_USER`. Always ephemeral. |

---

## ⚙️ Configuration

All settings go in a `.env` file next to `bot.py`.

```env
# Required
DISCORD_TOKEN=your_bot_token_here
GUILD_ID=your_server_id

# Channels
VOICE_COMMAND_CHANNEL=channel_id   # commands are only allowed here...
VOICE_BOT_CHANNEL=channel_id       # ...or here (also where auto-posts go)

# Optional
TIMEZONE=Europe/Stockholm          # used for chart labels and weekly/monthly timing
AFK_CHANNEL_ID=channel_id          # sessions in this channel are excluded from all stats
VOICE_TOP_PRIVATE_USER=user_id     # this user can use the 'private' flag on any command
MAX_SESSION_HOURS=6                # sessions longer than this are capped on startup reconciliation (default 6)
```

---

## 🚀 Setup

### 1. Install dependencies

```bash
pip install discord.py aiosqlite python-dotenv matplotlib
```

### 2. Create a `.env` file

Copy the configuration block above and fill in your values.

### 3. Enable required intents

In the [Discord Developer Portal](https://discord.com/developers/applications), under your bot's settings:

- Enable **Server Members Intent** (used for reliable member display name lookups)
- Enable **Voice State** events (on by default)

### 4. Run directly (for testing)

```bash
python bot.py
```

### 5. Run as a systemd service (recommended for Pi)

Create `/etc/systemd/system/voicebot.service`:

```ini
[Unit]
Description=Discord Voice Bot
After=network-online.target
Wants=network-online.target

[Service]
User=pi
WorkingDirectory=/home/pi/your-bot-folder
ExecStart=/home/pi/your-bot-folder/venv/bin/python bot.py
Restart=on-failure
RestartSec=5
TimeoutStopSec=15

[Install]
WantedBy=multi-user.target
```

Then enable and start it:

```bash
sudo systemctl enable voicebot
sudo systemctl start voicebot
```

**Useful commands:**

```bash
sudo systemctl restart voicebot   # restart the bot
sudo systemctl stop voicebot      # graceful stop (flushes open sessions)
sudo systemctl status voicebot    # check if it's running
journalctl -u voicebot -f         # live logs
systemctl list-units --type=service | grep -i bot  # find the service name if you forgot it
```

---

## 🛡️ Data integrity

The bot protects session data against shutdowns in two ways:

**Graceful shutdown** — when the process receives SIGTERM or SIGINT (systemd stop, Ctrl+C), all open sessions are stamped with the current timestamp before the process exits.

**Startup reconciliation** — on every boot, the bot checks for sessions that are still open (from a previous ungraceful shutdown such as a power cut). For each orphaned session:
- If the user is **still in that channel** — the old session is closed and a fresh one opened from now.
- If the user has **left** — the session is capped at `joined_ts + MAX_SESSION_HOURS` so a short session before a long outage doesn't inflate anyone's stats.

---

## 📁 Files

```
bot.py        — the bot
bot.db        — SQLite database (auto-created on first run)
.env          — your configuration (never commit this)
```

---

## 🤝 Contributing

PRs welcome. This project is intentionally focused — no moderation, leveling, music, or spam detection. If you want those, fork it and hack away.