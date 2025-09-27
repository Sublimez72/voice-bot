import os, time, aiosqlite
from datetime import datetime, timezone, timedelta
import discord
from discord import app_commands
from dotenv import load_dotenv
import io
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


# -------- Env --------
load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")
TZ_NAME = os.getenv("TIMEZONE", "Europe/Stockholm")
AFK_CHANNEL_ID = int(os.getenv("AFK_CHANNEL_ID", "0"))
GUILD_ID = os.getenv("GUILD_ID")  # optional: fast guild-only sync

# -------- Intents (no message content needed) --------
intents = discord.Intents.default()
intents.guilds = True
intents.voice_states = True

# Use plain Client + CommandTree (no prefix commands at all)
client = discord.Client(intents=intents)
tree = app_commands.CommandTree(client)

DB_PATH = "bot.db"

# -------- Utils --------
def now_ts() -> int: return int(time.time())

def fmt_duration(seconds: int) -> str:
    seconds = int(seconds or 0); h = seconds // 3600; m = (seconds % 3600) // 60
    return f"{h}h {m}m"

def ts_to_local(ts: int) -> str:
    try:
        import zoneinfo
        tz = zoneinfo.ZoneInfo(TZ_NAME)
        return datetime.fromtimestamp(ts, tz=tz).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

def afk_filter_clause():
    if AFK_CHANNEL_ID: return " AND channel_id != ? ", [AFK_CHANNEL_ID]
    return " ", []

# -------- DB --------
async def ensure_schema():
    async with aiosqlite.connect(DB_PATH) as cx:
        await cx.executescript("""
        PRAGMA journal_mode = WAL;
        CREATE TABLE IF NOT EXISTS voice_sessions (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          user_id INTEGER NOT NULL,
          channel_id INTEGER NOT NULL,
          joined_ts INTEGER NOT NULL,
          left_ts INTEGER
        );
        CREATE INDEX IF NOT EXISTS idx_voice_open ON voice_sessions(user_id) WHERE left_ts IS NULL;
        CREATE INDEX IF NOT EXISTS idx_voice_join ON voice_sessions(joined_ts);
        """)
        await cx.commit()

# -------- Startup --------
@client.event
async def on_ready():
    await ensure_schema()

    # Fast guild-only sync if GUILD_ID is set, otherwise global sync
    try:
        if GUILD_ID:
            guild = discord.Object(id=int(GUILD_ID))
            await tree.sync(guild=guild)
            print(f"✅ Synced slash commands to guild {GUILD_ID}")
        else:
            synced = await tree.sync()
            print(f"✅ Synced {len(synced)} global slash commands")
    except Exception as e:
        print(f"❌ Slash command sync failed: {e}")

    print(f"Bot online as {client.user}")

# -------- Voice tracking --------
@client.event
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
    now = now_ts()
    if before.channel is None and after.channel is not None:
        async with aiosqlite.connect(DB_PATH) as cx:
            await cx.execute("INSERT INTO voice_sessions(user_id, channel_id, joined_ts) VALUES(?,?,?)",
                             (member.id, after.channel.id, now))
            await cx.commit()
    elif before.channel is not None and after.channel is None:
        async with aiosqlite.connect(DB_PATH) as cx:
            await cx.execute("UPDATE voice_sessions SET left_ts=? WHERE user_id=? AND channel_id=? AND left_ts IS NULL",
                             (now, member.id, before.channel.id))
            await cx.commit()
    elif before.channel and after.channel and before.channel.id != after.channel.id:
        async with aiosqlite.connect(DB_PATH) as cx:
            await cx.execute("UPDATE voice_sessions SET left_ts=? WHERE user_id=? AND channel_id=? AND left_ts IS NULL",
                             (now, member.id, before.channel.id))
            await cx.execute("INSERT INTO voice_sessions(user_id, channel_id, joined_ts) VALUES(?,?,?)",
                             (member.id, after.channel.id, now))
            await cx.commit()

# -------- Slash commands --------
@tree.command(name="voice_report", description="Show YOUR voice time in the last X days (default 7).")
async def voice_report(inter: discord.Interaction, days: app_commands.Range[int, 1, 3650] = 7):
    since = now_ts() - days * 86400
    extra, params = afk_filter_clause()
    async with aiosqlite.connect(DB_PATH) as cx:
        async with cx.execute(
            f"SELECT SUM(COALESCE(left_ts, strftime('%s','now')) - joined_ts) "
            f"FROM voice_sessions WHERE user_id=? AND joined_ts>=? {extra}",
            [inter.user.id, since] + params
        ) as cur:
            total = (await cur.fetchone())[0]
    await inter.response.send_message(f"🎧 {inter.user.mention}: last {days}d **{fmt_duration(total)}**", ephemeral=True)

@tree.command(name="voice_total", description="Show YOUR lifetime total voice time.")
async def voice_total(inter: discord.Interaction):
    extra, params = afk_filter_clause()
    async with aiosqlite.connect(DB_PATH) as cx:
        async with cx.execute(
            f"SELECT SUM(COALESCE(left_ts, strftime('%s','now')) - joined_ts) "
            f"FROM voice_sessions WHERE user_id=? {extra}",
            [inter.user.id] + params
        ) as cur:
            total = (await cur.fetchone())[0]
    await inter.response.send_message(f"📊 {inter.user.mention}: lifetime **{fmt_duration(total)}**", ephemeral=True)

@tree.command(name="voice_current", description="List users currently in voice channels.")
async def voice_current(inter: discord.Interaction):
    lines = []
    for vc in inter.guild.voice_channels:
        if vc.members:
            names = ", ".join(m.display_name for m in vc.members)
            lines.append(f"🔊 **{vc.name}**: {names}")
    msg = "\n".join(lines) if lines else "No one is in voice right now."
    await inter.response.send_message(msg)

@tree.command(name="voice_top", description="Top 10 users by voice time in the last X days (default 7).")
async def voice_top(inter: discord.Interaction, days: app_commands.Range[int, 1, 3650] = 7):
    # --- Restrict >7 days to administrators only ---
    if days > 7 and not inter.user.guild_permissions.administrator:
        await inter.response.send_message(
            "You can only use more than 7 days for this command if you're an **administrator**.",
            ephemeral=True
        )
        return
    # ------------------------------------------------

    since = now_ts() - days * 86400
    extra, params = afk_filter_clause()
    async with aiosqlite.connect(DB_PATH) as cx:
        async with cx.execute(
            f"SELECT user_id, SUM(COALESCE(left_ts, strftime('%s','now')) - joined_ts) AS total "
            f"FROM voice_sessions WHERE joined_ts>=? {extra} "
            f"GROUP BY user_id ORDER BY total DESC LIMIT 10",
            [since] + params
        ) as cur:
            rows = await cur.fetchall()
    if not rows:
        await inter.response.send_message(f"No voice activity in the last {days}d.")
        return
    lines = []
    for i, (uid, total) in enumerate(rows, start=1):
        m = inter.guild.get_member(uid)
        name = m.mention if m else f"<@{uid}>"
        lines.append(f"{i}. {name} — **{fmt_duration(total)}**")
    await inter.response.send_message(f"**Top voice time (last {days}d):**\n" + "\n".join(lines))

# -------- Run --------
if __name__ == "__main__":
    if not TOKEN:
        raise SystemExit("DISCORD_TOKEN missing in .env")
    client.run(TOKEN)
