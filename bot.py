import os, time, aiosqlite
from datetime import datetime, timezone
import discord
from discord.ext import commands
from dotenv import load_dotenv

# ---------------- Env / Intents ----------------
load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")
TZ_NAME = os.getenv("TIMEZONE", "Europe/Stockholm")
AFK_CHANNEL_ID = int(os.getenv("AFK_CHANNEL_ID", "0"))  # set AFK channel ID or 0 to disable

intents = discord.Intents.default()
intents.guilds = True
intents.members = True
intents.voice_states = True

# Respond to mention OR the '!v' prefix (with or without a trailing space)
bot = commands.Bot(
    command_prefix=commands.when_mentioned_or("!v ", "!v"),
    intents=intents
)
DB_PATH = "bot.db"

# ---------------- Utils ----------------
def now_ts() -> int:
    return int(time.time())

def fmt_duration(seconds: int) -> str:
    seconds = int(seconds or 0)
    h = seconds // 3600
    m = (seconds % 3600) // 60
    return f"{h}h {m}m"

def ts_to_local(ts: int) -> str:
    try:
        import zoneinfo
        tz = zoneinfo.ZoneInfo(TZ_NAME)
        return datetime.fromtimestamp(ts, tz=tz).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

def afk_filter_clause():
    # Returns (SQL_snippet, params) to exclude AFK channel if configured
    if AFK_CHANNEL_ID:
        return " AND channel_id != ? ", [AFK_CHANNEL_ID]
    return " ", []

# ---------------- DB Setup ----------------
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

@bot.event
async def on_ready():
    await ensure_schema()
    print(f"✅ Logged in as {bot.user.name} — prefix: !v  (mention also works)")

# ---------------- Voice session tracking ----------------
@bot.event
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
    now = now_ts()

    # Join: open a session
    if before.channel is None and after.channel is not None:
        async with aiosqlite.connect(DB_PATH) as cx:
            await cx.execute(
                "INSERT INTO voice_sessions(user_id, channel_id, joined_ts) VALUES(?,?,?)",
                (member.id, after.channel.id, now)
            )
            await cx.commit()

    # Leave: close the session
    elif before.channel is not None and after.channel is None:
        async with aiosqlite.connect(DB_PATH) as cx:
            await cx.execute(
                "UPDATE voice_sessions SET left_ts=? WHERE user_id=? AND channel_id=? AND left_ts IS NULL",
                (now, member.id, before.channel.id)
            )
            await cx.commit()

    # Switch: close old, open new
    elif before.channel and after.channel and before.channel.id != after.channel.id:
        async with aiosqlite.connect(DB_PATH) as cx:
            await cx.execute(
                "UPDATE voice_sessions SET left_ts=? WHERE user_id=? AND channel_id=? AND left_ts IS NULL",
                (now, member.id, before.channel.id)
            )
            await cx.execute(
                "INSERT INTO voice_sessions(user_id, channel_id, joined_ts) VALUES(?,?,?)",
                (member.id, after.channel.id, now)
            )
            await cx.commit()

# ---------------- Voice commands ----------------
@bot.group(
    name="voice",
    invoke_without_command=True,
    help="Voice analytics. Usage: !v report [days], !v total, !v history [limit], !v current, !v top [days], !v channel_top #ch [days], !v channels_top [days]"
)
async def voice_group(ctx):
    await ctx.send(
        "Usage:\n"
        "`!v report [days]`\n"
        "`!v total`\n"
        "`!v history [limit]`\n"
        "`!v current`\n"
        "`!v top [days]`\n"
        "`!v channel_top #channel [days]`\n"
        "`!v channels_top [days]`"
    )

# Self-only: report
@voice_group.command(name="report", help="Show YOUR voice time in the last X days (default 7). Usage: !v report [days]")
async def voice_report(ctx, days: int = 7):
    since = now_ts() - days * 86400
    extra, params = afk_filter_clause()
    async with aiosqlite.connect(DB_PATH) as cx:
        async with cx.execute(
            f"""SELECT SUM(COALESCE(left_ts, strftime('%s','now')) - joined_ts)
                FROM voice_sessions
                WHERE user_id=? AND joined_ts>=? {extra}""",
            [ctx.author.id, since] + params
        ) as cur:
            total = (await cur.fetchone())[0]
    await ctx.send(f"🎧 {ctx.author.mention}: last {days}d **{fmt_duration(total)}**")

# Self-only: lifetime total
@voice_group.command(name="total", help="Show YOUR lifetime total voice time. Usage: !v total")
async def voice_total(ctx):
    extra, params = afk_filter_clause()
    async with aiosqlite.connect(DB_PATH) as cx:
        async with cx.execute(
            f"""SELECT SUM(COALESCE(left_ts, strftime('%s','now')) - joined_ts)
                FROM voice_sessions
                WHERE user_id=? {extra}""",
            [ctx.author.id] + params
        ) as cur:
            total = (await cur.fetchone())[0]
    await ctx.send(f"📊 {ctx.author.mention}: lifetime **{fmt_duration(total)}**")

# Self-only: recent session history
@voice_group.command(name="history", help="List YOUR last N voice sessions (default 5, max 20). Usage: !v history [limit]")
async def voice_history(ctx, limit: int = 5):
    limit = max(1, min(20, limit))
    extra, params = afk_filter_clause()
    async with aiosqlite.connect(DB_PATH) as cx:
        async with cx.execute(
            f"""SELECT channel_id, joined_ts, left_ts
                FROM voice_sessions
                WHERE user_id=? {extra}
                ORDER BY joined_ts DESC
                LIMIT ?""",
            [ctx.author.id] + params + [limit]
        ) as cur:
            rows = await cur.fetchall()
    if not rows:
        await ctx.send("No sessions found.")
        return
    lines = []
    for ch_id, joined, left in rows:
        ch = ctx.guild.get_channel(ch_id)
        name = ch.name if ch else f"#{ch_id}"
        start = ts_to_local(joined)
        dur = (left or now_ts()) - joined
        lines.append(f"• **{name}** — {start} ({fmt_duration(dur)})")
    await ctx.send("📜 Your recent sessions:\n" + "\n".join(lines))

# Global: who’s in voice right now
@voice_group.command(name="current", help="List users currently in voice channels.")
async def voice_current(ctx):
    lines = []
    for vc in ctx.guild.voice_channels:
        if vc.members:
            names = ", ".join(m.display_name for m in vc.members)
            lines.append(f"🔊 **{vc.name}**: {names}")
    if not lines:
        await ctx.send("No one is in voice right now.")
    else:
        await ctx.send("\n".join(lines))

# Global: top users overall
@voice_group.command(name="top", help="Top 10 users by voice time in the last X days (default 7). Usage: !v top [days]")
async def voice_top(ctx, days: int = 7):
    since = now_ts() - days * 86400
    extra, params = afk_filter_clause()
    async with aiosqlite.connect(DB_PATH) as cx:
        async with cx.execute(
            f"""SELECT user_id, SUM(COALESCE(left_ts, strftime('%s','now')) - joined_ts) AS total
                FROM voice_sessions
                WHERE joined_ts>=? {extra}
                GROUP BY user_id
                ORDER BY total DESC LIMIT 10""",
            [since] + params
        ) as cur:
            rows = await cur.fetchall()
    if not rows:
        await ctx.send(f"No voice activity in the last {days}d.")
        return
    lines = []
    for i, (uid, total) in enumerate(rows, start=1):
        member = ctx.guild.get_member(uid)
        name = member.mention if member else f"<@{uid}>"
        lines.append(f"{i}. {name} — **{fmt_duration(total)}**")
    await ctx.send(f"**Top voice time (last {days}d):**\n" + "\n".join(lines))

# Global: top users in a specific channel
@voice_group.command(name="channel_top", help="Top 10 users by voice time in a channel. Usage: !v channel_top #channel [days]")
async def voice_channel_top(ctx, channel: discord.VoiceChannel, days: int = 7):
    since = now_ts() - days * 86400
    if AFK_CHANNEL_ID and channel.id == AFK_CHANNEL_ID:
        await ctx.send(f"AFK channel **{channel.name}** is excluded from stats.")
        return
    async with aiosqlite.connect(DB_PATH) as cx:
        async with cx.execute(
            """SELECT user_id, SUM(COALESCE(left_ts, strftime('%s','now')) - joined_ts) AS total
               FROM voice_sessions
               WHERE channel_id=? AND joined_ts>=?
               GROUP BY user_id
               ORDER BY total DESC LIMIT 10""",
            (channel.id, since)
        ) as cur:
            rows = await cur.fetchall()
    if not rows:
        await ctx.send(f"No activity in {channel.mention} in the last {days}d.")
        return
    lines = []
    for i, (uid, total) in enumerate(rows, start=1):
        member = ctx.guild.get_member(uid)
        name = member.mention if member else f"<@{uid}>"
        lines.append(f"{i}. {name} — **{fmt_duration(total)}**")
    await ctx.send(f"**Top voice in {channel.mention} (last {days}d):**\n" + "\n".join(lines))

# Global: top channels overall
@voice_group.command(name="channels_top", help="Top 10 voice channels by total time in the last X days. Usage: !v channels_top [days]")
async def voice_channels_top(ctx, days: int = 7):
    since = now_ts() - days * 86400
    extra, params = afk_filter_clause()
    async with aiosqlite.connect(DB_PATH) as cx:
        async with cx.execute(
            f"""SELECT channel_id, SUM(COALESCE(left_ts, strftime('%s','now')) - joined_ts) AS total
                FROM voice_sessions
                WHERE joined_ts>=? {extra}
                GROUP BY channel_id
                ORDER BY total DESC LIMIT 10""",
            [since] + params
        ) as cur:
            rows = await cur.fetchall()
    if not rows:
        await ctx.send(f"No voice activity in the last {days}d.")
        return
    lines = []
    for i, (ch_id, total) in enumerate(rows, start=1):
        ch = ctx.guild.get_channel(ch_id)
        name = ch.mention if ch else f"#<deleted:{ch_id}>"
        lines.append(f"{i}. {name} — **{fmt_duration(total)}**")
    await ctx.send(f"**Top channels (last {days}d):**\n" + "\n".join(lines))

# ---------------- Friendly error for missing args ----------------
@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(f"⚠️ Missing argument: `{error.param.name}`\nUse `!v help {ctx.command}`.")
    else:
        raise error

# ---------------- Entry ----------------
if __name__ == "__main__":
    if not TOKEN:
        raise SystemExit("DISCORD_TOKEN is missing in your .env")
    bot.run(TOKEN)
