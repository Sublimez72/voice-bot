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

Commands default to `ephemeral: false` (public) unless marked otherwise.
The `private` flag is only honoured for the designated `VOICE_TOP_PRIVATE_USER`.

### 👤 Personal stats
| Command | Description |
|---|---|
| `/voice_me` | Full personal dashboard — lifetime, last 7d/30d, server rank, daily streak, top voice partners. Always ephemeral. |
| `/voice_report [days]` | Your total voice time in the last N days (default 7). Ephemeral. |
| `/voice_total` | Your all-time lifetime voice time. Ephemeral. |
| `/voice_history [private]` | Your last 10 sessions with timestamps, channel names, and durations. |
| `/voice_my_chart [days]` | 📈 Your personal daily activity chart for the last N days (always ephemeral). |
| `/voice_next_milestone` | How close you are to your next time threshold and an ETA based on your 7-day pace. Always ephemeral. |
| `/voice_best_day` | Your top 5 personal best days by voice time ever. Always ephemeral. |

### 🏆 Server leaderboards
| Command | Description |
|---|---|
| `/voice_server_overview [private]` | All-time snapshot: total hours, sessions, unique users, avg session length, most popular channel, currently active count. |
| `/voice_top [days] [private]` | Top 50 users by voice time in the last N days (default 7). |
| `/voice_leaderboard_chart [days] [private]` | 📊 Top 15 users as a horizontal bar chart — coloured by rank. Default 30 days. |
| `/voice_streak_board [private]` | 📊 Current daily voice streaks leaderboard with colour-coded bar chart. |
| `/voice_consistency [days] [min_days] [private]` | Ranked by % of days active — rewards regulars over grinders. Default 30 days. |
| `/voice_binge [days] [private]` | Biggest single calendar day per user — who went hardest on one day. Default 30 days. |
| `/voice_solo [days] [private]` | Top 50 users by time spent *alone* in a channel (occupancy = 1). |
| `/voice_night_owl [days] [start_hour] [end_hour] [private]` | Leaderboard for time during late-night hours (default 23:00–04:00, wraps midnight). |
| `/voice_early_bird [days] [start_hour] [end_hour] [private]` | Leaderboard for time during early-morning hours (default 05:00–09:00). |
| `/voice_marathon [days] [private]` | Top 10 longest single sessions on the server. |
| `/voice_ghost [private]` | Members who have been absent from voice the longest. |
| `/voice_milestones [limit] [private]` | Recent milestone awards (1h, 5h, 10h, 25h, …, 10 000h). |

### ⚔️ Head-to-head
| Command | Description |
|---|---|
| `/voice_rivalry @opponent [days] [private]` | Side-by-side embed: you vs another member — lifetime, recent, solo time, streak, rank, and time together. Winner of each stat is bolded. |
| `/voice_bestfriends [days] [private]` | Top 10 pairs of users by shared voice time. |
| `/voice_together @user1 @user2 [private]` | *Admin only.* Total time two specific members have spent in voice together. |

### 📈 Charts & Trends
All chart commands produce a PNG image. Use `public: false` or `private: true` to post quietly.

| Command | Description |
|---|---|
| `/voice_heatmap [days] [public] [private]` | Total voice hours by hour of day (00–23). |
| `/voice_weekdays [days] [public] [private]` | Total voice hours by day of week (Mon–Sun). |
| `/voice_daily [days] [public] [private]` | Total server voice hours per day (trend). |
| `/voice_daily_unique [days] [public] [private]` | Unique participants per day. |
| `/voice_peak [days] [public] [private]` | Peak concurrent users per day. |
| `/voice_growth [days] [public] [private]` | 📈 Cumulative voice hours — see if the community is growing. |
| `/voice_session_count [days] [public] [private]` | Sessions started per day — frequency, not just duration. |

### 🔍 Snapshot
| Command | Description |
|---|---|
| `/voice_current [private]` | Who is in voice right now, grouped by channel. |
| `/voice_channel_stats [days] [private]` | Per-channel total hours and unique user count, ranked. |

### 🔒 Admin only
| Command | Description |
|---|---|
| `/voice_recap [month]` | Manually post the monthly voice recap (or a specific past month in `YYYY-MM` format). |

### ⚙️ Special user only
| Command | Description |
|---|---|
| `/pi_storage [path]` | Disk usage for a given path on the host (default `/`). Always ephemeral. |

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
