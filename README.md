# 🎙️ Discord Voice Stats Bot

A lightweight Discord bot that tracks **voice channel activity** and provides stats via **slash commands**.  
Built with [discord.py](https://github.com/Rapptz/discord.py) and [aiosqlite](https://pypi.org/project/aiosqlite/).  
Runs on a Raspberry Pi (or any Linux host) as a systemd service.

---

## ✨ Features

- Tracks **voice joins, leaves, and switches** into an SQLite database  
- Ignores time spent in the **AFK channel** (if configured)  
- Provides easy-to-use **slash commands**:
  - `/voice_report [days]` → Show your voice time in the last X days (default 7) *(ephemeral)*  
  - `/voice_total` → Show your lifetime voice time *(ephemeral)*  
  - `/voice_current` → See who’s currently in voice channels  
  - `/voice_top [days]` → Leaderboard of top 10 voice users in the last X days
  -  `/voice_heatmap [days] [public]` → Anonymized activity by hour of day (server-wide)
  -  `/voice_weekday [days] [public]` → Anonymized activity by weekday (server-wide)

---

## 📦 Requirements

- Python 3.11+
- Discord bot token
- SQLite (built into Python)
- Raspberry Pi or Linux server
- matplotlib (for charts)

---
🤝 Contributing
PRs welcome! This project is intentionally minimal — no moderation, leveling, music or spam detection.
If you want extra features, fork it and hack away 🎉
