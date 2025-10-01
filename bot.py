import os, time, aiosqlite
from datetime import datetime, timezone, timedelta
import discord
from discord import app_commands
from dotenv import load_dotenv
import io
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import shutil
from discord.errors import NotFound, Forbidden
from discord.utils import escape_markdown


# -------- Env --------
load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")
TZ_NAME = os.getenv("TIMEZONE", "Europe/Stockholm")
AFK_CHANNEL_ID = int(os.getenv("AFK_CHANNEL_ID", "0"))
GUILD_ID = os.getenv("GUILD_ID")
GUILD_OBJ = discord.Object(id=int(GUILD_ID)) if GUILD_ID else None
VOICE_TOP_PRIVATE_USER = int(os.getenv("VOICE_TOP_PRIVATE_USER", "0"))
VOICE_COMMAND_CHANNEL = int(os.getenv("VOICE_COMMAND_CHANNEL", "0"))
BOT_CHANNEL = int(os.getenv("VOICE_BOT_CHANNEL", "0"))
ALLOWED_CHANNELS = {cid for cid in (VOICE_COMMAND_CHANNEL, BOT_CHANNEL) if cid}

# -------- Intents (no message content needed) --------
intents = discord.Intents.default()
intents.guilds = True
intents.voice_states = True

# Use plain Client + CommandTree (no prefix commands at all)
client = discord.Client(intents=intents)
# Restrict all slash commands to specific channels
class RestrictedTree(app_commands.CommandTree):
    async def interaction_check(self, inter: discord.Interaction) -> bool:
        ch = inter.channel
        cid = getattr(inter, "channel_id", None) or (ch.id if ch else None)
        parent_id = getattr(ch, "parent_id", None) if isinstance(ch, discord.Thread) else None

        if cid in ALLOWED_CHANNELS or parent_id in ALLOWED_CHANNELS:
            return True

        # Send a friendly ephemeral denial message
        where = " or ".join(f"<#{cid}>" for cid in ALLOWED_CHANNELS) or "the designated channels"
        try:
            if inter.response.is_done():
                await inter.followup.send(
                    f"⛔ This command can only be used in {where}.", ephemeral=True
                )
            else:
                await inter.response.send_message(
                    f"⛔ This command can only be used in {where}.", ephemeral=True
                )
        except Exception:
            pass
        return False
tree = RestrictedTree(client)
DB_PATH = "bot.db"

# Same size for all plots
PLOT_SIZE = (15, 5)


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

def aggregate_seconds_by_hour(rows, since_ts: int, now_ts_: int, tz_name: str, afk_channel_id: int | None):
    """rows: list of (joined_ts, left_ts, channel_id). Returns [sec_per_hour_0..23]."""
    try:
        import zoneinfo
        tz = zoneinfo.ZoneInfo(tz_name)
    except Exception:
        tz = timezone.utc

    buckets = [0] * 24  # seconds per hour-of-day
    for joined_ts, left_ts, ch_id in rows:
        if afk_channel_id and ch_id == afk_channel_id:
            continue
        # clamp to window
        start = max(joined_ts, since_ts)
        end = min(left_ts or now_ts_, now_ts_)
        if end <= start:
            continue

        cur = start
        while cur < end:
            cur_dt = datetime.fromtimestamp(cur, tz=tz)
            # boundary = next top-of-hour
            next_hour = (cur_dt.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
            boundary = min(int(next_hour.timestamp()), end)
            span = boundary - cur
            buckets[cur_dt.hour] += span
            cur = boundary
    return buckets

def aggregate_seconds_by_weekday(rows, since_ts: int, now_ts_: int, tz_name: str, afk_channel_id: int | None):
    """rows: list of (joined_ts, left_ts, channel_id). Returns [sec_per_day Mon..Sun]."""
    try:
        import zoneinfo
        tz = zoneinfo.ZoneInfo(tz_name)
    except Exception:
        tz = timezone.utc

    # Python weekday(): Monday=0 .. Sunday=6
    buckets = [0] * 7
    for joined_ts, left_ts, ch_id in rows:
        if afk_channel_id and ch_id == afk_channel_id:
            continue

        start = max(joined_ts, since_ts)
        end = min(left_ts or now_ts_, now_ts_)
        if end <= start:
            continue

        cur = start
        while cur < end:
            cur_dt = datetime.fromtimestamp(cur, tz=tz)
            # end of this calendar day
            next_day = (cur_dt.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1))
            boundary = min(int(next_day.timestamp()), end)
            span = boundary - cur
            buckets[cur_dt.weekday()] += span
            cur = boundary
    return buckets

def aggregate_seconds_by_day(rows, since_ts: int, now_ts_: int, tz_name: str, afk_channel_id: int | None):
    """
    rows: list of (joined_ts, left_ts, channel_id)
    returns: dict {date_str 'YYYY-MM-DD' -> seconds}
    """
    try:
        import zoneinfo
        tz = zoneinfo.ZoneInfo(tz_name)
    except Exception:
        tz = timezone.utc

    buckets = {}  # date_str -> seconds
    for joined_ts, left_ts, ch_id in rows:
        if afk_channel_id and ch_id == afk_channel_id:
            continue
        start = max(joined_ts, since_ts)
        end = min(left_ts or now_ts_, now_ts_)
        if end <= start:
            continue

        cur = start
        while cur < end:
            cur_dt = datetime.fromtimestamp(cur, tz=tz)
            next_midnight = (cur_dt.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1))
            boundary = min(int(next_midnight.timestamp()), end)
            span = boundary - cur
            day_key = cur_dt.strftime("%Y-%m-%d")
            buckets[day_key] = buckets.get(day_key, 0) + span
            cur = boundary
    return buckets

async def fetch_sessions_window(since_ts: int):
    """Return rows (user_id, channel_id, joined_ts, left_ts) overlapping the window [since_ts, now]."""
    now = now_ts()
    async with aiosqlite.connect(DB_PATH) as cx:
        async with cx.execute(
            """
            SELECT user_id, channel_id, joined_ts, left_ts
            FROM voice_sessions
            WHERE joined_ts < ? AND COALESCE(left_ts, strftime('%s','now')) > ?
            """,
            (now, since_ts)
        ) as cur:
            rows = await cur.fetchall()
    return rows

def aggregate_unique_users_by_day(rows, since_ts: int, tz_name: str, afk_channel_id: int|None):
    """rows: (user_id, channel_id, joined_ts, left_ts). Returns {YYYY-MM-DD: set(user_ids)}."""
    try:
        import zoneinfo
        tz = zoneinfo.ZoneInfo(tz_name)
    except Exception:
        tz = timezone.utc

    day_users = {}  # date_str -> set of user ids
    now_ = now_ts()
    for user_id, ch_id, joined_ts, left_ts in rows:
        if afk_channel_id and ch_id == afk_channel_id:
            continue
        start = max(joined_ts, since_ts)
        end = min(left_ts or now_, now_)
        if end <= start:
            continue

        cur = start
        while cur < end:
            cur_dt = datetime.fromtimestamp(cur, tz=tz)
            next_midnight = (cur_dt.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1))
            boundary = min(int(next_midnight.timestamp()), end)
            day_key = cur_dt.strftime("%Y-%m-%d")
            s = day_users.get(day_key)
            if s is None:
                s = set()
                day_users[day_key] = s
            s.add(user_id)
            cur = boundary
    return day_users

def peak_concurrency(rows, since_ts: int, tz_name: str, afk_channel_id: int|None):
    """Return overall peak count and per-day peaks {YYYY-MM-DD: peak}."""
    try:
        import zoneinfo
        tz = zoneinfo.ZoneInfo(tz_name)
    except Exception:
        tz = timezone.utc

    now_ = now_ts()
    events = []  # (timestamp, delta)
    for _uid, ch_id, joined_ts, left_ts in rows:
        if afk_channel_id and ch_id == afk_channel_id:
            continue
        start = max(joined_ts, since_ts)
        end = min(left_ts or now_, now_)
        if end <= start:
            continue
        events.append((start, +1))
        events.append((end, -1))
    events.sort()

    overall_peak = 0
    cur = 0
    per_day_peak = {}  # date_str -> peak
    last_ts = None

    for ts, delta in events:
        # update day peak at this boundary (assign current 'cur' to the day of ts)
        cur += delta
        if cur > overall_peak:
            overall_peak = cur
        day_key = datetime.fromtimestamp(ts, tz=tz).strftime("%Y-%m-%d")
        prev = per_day_peak.get(day_key, 0)
        if cur > prev:
            per_day_peak[day_key] = cur
        last_ts = ts
    return overall_peak, per_day_peak

def solo_seconds_per_user(rows, since_ts: int, tz_name: str, afk_channel_id: int | None):
    """
    rows: list of (user_id, channel_id, joined_ts, left_ts) overlapping the window.
    Returns dict {user_id: solo_seconds} where 'solo' means channel occupancy == 1.
    """
    try:
        import zoneinfo
        tz = zoneinfo.ZoneInfo(tz_name)
    except Exception:
        tz = timezone.utc

    now_ = now_ts()
    # Build per-channel event streams: (timestamp, user_id, delta)
    per_ch: dict[int, list[tuple[int, int, int]]] = {}
    for uid, ch_id, joined_ts, left_ts in rows:
        if afk_channel_id and ch_id == afk_channel_id:
            continue
        start = max(joined_ts, since_ts)
        end = min(left_ts or now_, now_)
        if end <= start:
            continue
        per_ch.setdefault(ch_id, []).append((start, uid, +1))
        per_ch[ch_id].append((end, uid, -1))

    solo_totals: dict[int, int] = {}
    for ch_id, events in per_ch.items():
        # Sort by time; process leaves (-1) before joins (+1) at the same timestamp
        events.sort(key=lambda x: (x[0], x[2]))
        present: set[int] = set()
        prev_t: int | None = None

        for t, uid, delta in events:
            if prev_t is not None and len(present) == 1:
                # Attribute the interval to the only user present
                only_uid = next(iter(present))
                solo_totals[only_uid] = solo_totals.get(only_uid, 0) + (t - prev_t)
            # Apply event
            if delta == +1:
                present.add(uid)
            else:
                present.discard(uid)
            prev_t = t
    return solo_totals


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
async def setup_hook():
    # Show what commands the tree has BEFORE syncing
    print("DEBUG pre-sync commands:", [c.name for c in tree.get_commands(guild=GUILD_OBJ)])

@client.event
async def on_ready():
    await ensure_schema()
    try:
        if GUILD_OBJ:
            # 1) Ensure guild commands (the ones you actually use)
            await tree.sync(guild=GUILD_OBJ)
            print(f"✅ Synced slash commands to guild {GUILD_ID}")

            # 2) One-time: delete any old GLOBAL commands (causing duplicates)
            # Since your tree defines NO global commands, syncing globals now
            # tells Discord to clear them.
            cleared = await tree.sync()  # global sync with empty set
            print("🧹 Cleared global commands (now none).")
        else:
            # If you don't use GUILD_ID, you can’t do the cleanup this way.
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
@tree.command(
    name="pi_storage",
    description="Show disk usage on your Pi, ephemerally and restricted.",
    guild=GUILD_OBJ
)
@app_commands.describe(
    path="Filesystem path to check (default '/')"
)
async def pi_storage(inter: discord.Interaction, path: str = "/"):
    # Restrict to the special user only
    if inter.user.id != VOICE_TOP_PRIVATE_USER:
        await inter.response.send_message("⛔ This command is restricted.", ephemeral=True)
        return

    # Always reply ephemerally for privacy
    try:
        usage = shutil.disk_usage(path)
        total, used, free = usage.total, usage.used, usage.free
        pct_used = (used / total * 100) if total else 0.0

        def fmt_bytes(n: int) -> str:
            for unit in ("B", "KB", "MB", "GB", "TB", "PB"):
                if n < 1024:
                    return f"{n:.2f} {unit}"
                n /= 1024
            return f"{n:.2f} EB"

        msg = (
            f"💾 **Disk usage for** `{path}`\n"
            f"• Total: **{fmt_bytes(total)}**\n"
            f"• Used: **{fmt_bytes(used)}** ({pct_used:.1f}%)\n"
            f"• Free: **{fmt_bytes(free)}**"
        )
        await inter.response.send_message(msg, ephemeral=True)
    except Exception as e:
        await inter.response.send_message(
            f"❌ Couldn't read disk usage for `{path}`: `{e}`",
            ephemeral=True
        )

@tree.command(name="voice_report", description="Show YOUR voice time in the last X days (default 7).", guild=GUILD_OBJ)
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

@tree.command(name="voice_weekdays",
              description="Anonymized total voice time by weekday (Mon–Sun), server-wide.",
              guild=GUILD_OBJ)
@app_commands.describe(
    days="How many days back to include (default 30)",
    public="Set to false to post privately (default: true)"
)
async def voice_weekdays(inter: discord.Interaction,
                         days: app_commands.Range[int, 1, 3650] = 30,
                         public: bool = True):
    since = now_ts() - days * 86400

    # Avoid interaction timeout during plotting
    await inter.response.defer(thinking=True, ephemeral=not public)
    
    extra, params = afk_filter_clause()
    # Load all sessions overlapping the window (server-wide, anonymized)
    async with aiosqlite.connect(DB_PATH) as cx:
        async with cx.execute(
            """
            SELECT joined_ts, left_ts, channel_id
            FROM voice_sessions
            WHERE joined_ts < ? AND COALESCE(left_ts, strftime('%s','now')) > ?
            """,
            (now_ts(), since)
        ) as cur:
            rows = await cur.fetchall()

    buckets = aggregate_seconds_by_weekday(rows, since, now_ts(), TZ_NAME, AFK_CHANNEL_ID or None)
    # Convert to hours and label Mon..Sun
    labels = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    values_hours = [s / 3600.0 for s in buckets]

    # Build plot
    plt.figure(figsize=PLOT_SIZE)
    plt.bar(range(7), values_hours)
    plt.xticks(range(7), labels)
    subtitle = " (AFK excluded)" if AFK_CHANNEL_ID else ""
    plt.title(f"Voice activity by weekday (last {days}d){subtitle}")
    plt.ylabel("Total hours")
    plt.xlabel("Weekday")
    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=150)
    plt.close()
    buf.seek(0)

    file = discord.File(buf, filename="voice_weekdays.png")
    await inter.followup.send(
        content=f"Anonymized server-wide weekday breakdown for last **{days}d**.",
        file=file,
        ephemeral=not public
    )

@tree.command(
    name="voice_solo",
    description="Top 50 users by time spent alone in voice (occupancy == 1).",
    guild=GUILD_OBJ
)
@app_commands.describe(
    days="How many days back (default 7; >7 requires admin)",
    private="Only available to special user; defaults to false"
)
async def voice_solo(
    inter: discord.Interaction,
    days: app_commands.Range[int, 1, 3650] = 7,
    private: bool = False
):
    # Guard for long ranges
    if days > 7 and not (inter.user.guild_permissions.administrator or inter.user.guild_permissions.manage_guild):
        await inter.response.send_message("⛔ Only admins can request more than 7 days.", ephemeral=True)
        return

    # Decide ephemerality once, then DEFER right away
    is_ephemeral = (private and inter.user.id == VOICE_TOP_PRIVATE_USER)
    await inter.response.defer(thinking=True, ephemeral=is_ephemeral)

    since = now_ts() - days * 86400

    # Fetch sessions overlapping the window (server-wide)
    async with aiosqlite.connect(DB_PATH) as cx:
        async with cx.execute(
            """
            SELECT user_id, channel_id, joined_ts, left_ts
            FROM voice_sessions
            WHERE joined_ts < ? AND COALESCE(left_ts, strftime('%s','now')) > ?
            """,
            (now_ts(), since)
        ) as cur:
            rows = await cur.fetchall()

    # Compute per-user solo seconds (AFK excluded if configured)
    solo_totals = solo_seconds_per_user(rows, since, TZ_NAME, AFK_CHANNEL_ID or None)

    if not solo_totals:
        await inter.followup.send("No solo voice time recorded in that window.", ephemeral=is_ephemeral)
        return

    # Resolve server nicknames with a tiny cache; fetch member if not cached
    member_cache: dict[int, str] = {}

    async def label_for(uid: int) -> str:
        if uid in member_cache:
            return member_cache[uid]
        m = inter.guild.get_member(uid)
        if m is None:
            try:
                m = await inter.guild.fetch_member(uid)
            except (NotFound, Forbidden, Exception):
                member_cache[uid] = f"User {uid}"
                return member_cache[uid]
        name = escape_markdown((m.nick or m.name) or str(uid))  # server nickname > username
        member_cache[uid] = name
        return name

    # Top 50
    top = sorted(solo_totals.items(), key=lambda kv: kv[1], reverse=True)[:50]

    lines = []
    for i, (uid, seconds) in enumerate(top, start=1):
        name = await label_for(uid)
        lines.append(f"{i}. {name} — {fmt_duration(seconds)}")

    text = (
        f"**Top 50 solo voice time (last {days}d)**"
        f"{' (AFK excluded)' if AFK_CHANNEL_ID else ''}:\n" + "\n".join(lines)
    )

    await inter.followup.send(
        text,
        ephemeral=is_ephemeral,
        allowed_mentions=discord.AllowedMentions.none()
    )



@tree.command(name="voice_peak",
              description="Anonymized peak concurrent users (overall + per-day chart).",
              guild=GUILD_OBJ)
@app_commands.describe(days="How many days back (default 7; >7 requires admin)",
                       public="Set to false to post privately (default: true)")
async def voice_peak(inter: discord.Interaction,
                     days: app_commands.Range[int, 1, 3650] = 7,
                     public: bool = True):
    if days > 7 and not (inter.user.guild_permissions.administrator or inter.user.guild_permissions.manage_guild):
        await inter.response.send_message("⛔ Only admins can request more than 7 days.", ephemeral=True)
        return

    since = now_ts() - days * 86400
    await inter.response.defer(thinking=True, ephemeral=not public)

    rows = await fetch_sessions_window(since)
    overall_peak, per_day_peak = peak_concurrency(rows, since, TZ_NAME, AFK_CHANNEL_ID or None)

    # ordered days
    try:
        import zoneinfo
        tz = zoneinfo.ZoneInfo(TZ_NAME)
    except Exception:
        tz = timezone.utc
    base = datetime.fromtimestamp(since, tz=tz).replace(hour=0, minute=0, second=0, microsecond=0)
    labels = [(base + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days)]
    values = [per_day_peak.get(d, 0) for d in labels]

    plt.figure(figsize=PLOT_SIZE)
    x = range(len(labels))
    plt.bar(x, values)
    plt.xticks(x, labels, rotation=45, ha="right")
    plt.title(f"Peak concurrent users per day (last {days}d){' (AFK excluded)' if AFK_CHANNEL_ID else ''}")
    plt.ylabel("Peak users")
    plt.xlabel("Day")
    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=150); plt.close(); buf.seek(0)

    await inter.followup.send(
        content=f"**Overall peak** in last {days}d: **{overall_peak}** users.",
        file=discord.File(buf, "voice_peak.png"),
        ephemeral=not public
    )


@tree.command(name="voice_daily_unique",
              description="Anonymized unique participants per day (last N days).",
              guild=GUILD_OBJ)
@app_commands.describe(days="How many days back (default 7; >7 requires admin)",
                       public="Set to false to post privately (default: true)")
async def voice_daily_unique(inter: discord.Interaction,
                             days: app_commands.Range[int, 1, 3650] = 7,
                             public: bool = True):
    if days > 7 and not (inter.user.guild_permissions.administrator or inter.user.guild_permissions.manage_guild):
        await inter.response.send_message("⛔ Only admins can request more than 7 days.", ephemeral=True)
        return

    since = now_ts() - days * 86400
    await inter.response.defer(thinking=True, ephemeral=not public)

    rows = await fetch_sessions_window(since)
    day_users = aggregate_unique_users_by_day(rows, since, TZ_NAME, AFK_CHANNEL_ID or None)

    # Build ordered days list
    try:
        import zoneinfo
        tz = zoneinfo.ZoneInfo(TZ_NAME)
    except Exception:
        tz = timezone.utc
    base = datetime.fromtimestamp(since, tz=tz).replace(hour=0, minute=0, second=0, microsecond=0)
    labels = [(base + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days)]
    values = [len(day_users.get(d, set())) for d in labels]

    plt.figure(figsize=PLOT_SIZE)
    x = range(len(labels))
    plt.bar(x, values)
    plt.xticks(x, labels, rotation=45, ha="right")
    plt.title(f"Unique voice participants per day (last {days}d){' (AFK excluded)' if AFK_CHANNEL_ID else ''}")
    plt.ylabel("Unique users")
    plt.xlabel("Day")
    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=150); plt.close(); buf.seek(0)
    await inter.followup.send(file=discord.File(buf, "voice_daily_unique.png"),
                              content=f"Unique participants per day (last **{days}d**).",
                              ephemeral=not public)


@tree.command(
    name="voice_heatmap",
    description="Anonymized activity by hour of day (server-wide).",
    guild=GUILD_OBJ
)
@app_commands.describe(
    days="How many days back to include (default 7)",
    public="Set to false to post privately (default: true)"
)
async def voice_heatmap(
    inter: discord.Interaction,
    days: app_commands.Range[int, 1, 3650] = 7,
    public: bool = True  # <-- Default is now public
):
    since = now_ts() - days * 86400
    extra, params = afk_filter_clause()
    # Load all sessions overlapping the window (server-wide, no per-user)
    async with aiosqlite.connect(DB_PATH) as cx:
        async with cx.execute(
            """
            SELECT joined_ts, left_ts, channel_id
            FROM voice_sessions
            WHERE joined_ts < ? AND COALESCE(left_ts, strftime('%s','now')) > ?
            """,
            (now_ts(), since)
        ) as cur:
            rows = await cur.fetchall()

    # Aggregate seconds per hour-of-day (AFK excluded if configured)
    buckets = aggregate_seconds_by_hour(rows, since, now_ts(), TZ_NAME, AFK_CHANNEL_ID or None)
    hours = list(range(24))
    values_hours = [s / 3600.0 for s in buckets]

    # Build plot
    plt.figure(figsize=PLOT_SIZE)
    plt.bar(hours, values_hours)
    plt.xticks(hours, [f"{h:02d}" for h in hours])
    subtitle = f"(AFK excluded)" if AFK_CHANNEL_ID else ""
    plt.title(f"Voice activity by hour (last {days}d) {subtitle}")
    plt.ylabel("Total hours")
    plt.xlabel("Hour of day")
    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=150)
    plt.close()
    buf.seek(0)

    file = discord.File(buf, filename="voice_heatmap.png")

    await inter.response.send_message(
        content=f"Anonymized server-wide heatmap for last **{days}d**.",
        file=file,
        ephemeral=not public
    )

@tree.command(
    name="voice_daily",
    description="Anonymized total server voice hours per day (trend).",
    guild=GUILD_OBJ
)
@app_commands.describe(
    days="How many days back (default 7; >7 requires admin)",
    public="Set false to post privately (default: true)"
)
async def voice_daily(
    inter: discord.Interaction,
    days: app_commands.Range[int, 1, 3650] = 7,
    public: bool = True
):
    # Privacy/permission rule: >7 days requires admin or manage_guild
    if days > 7:
        perms = inter.user.guild_permissions
        if not (perms.administrator or perms.manage_guild):
            await inter.response.send_message(
                "⛔ Only admins can request more than 7 days.",
                ephemeral=True
            )
            return

    since = now_ts() - days * 86400

    # Defer so we have time to compute & render the plot
    await inter.response.defer(thinking=True, ephemeral=not public)
    extra, params = afk_filter_clause()
    # Load all sessions overlapping window (server-wide, anonymized)
    async with aiosqlite.connect(DB_PATH) as cx:
        async with cx.execute(
            """
            SELECT joined_ts, left_ts, channel_id
            FROM voice_sessions
            WHERE joined_ts < ? AND COALESCE(left_ts, strftime('%s','now')) > ?
            """,
            (now_ts(), since)
        ) as cur:
            rows = await cur.fetchall()

    # Aggregate per day
    buckets = aggregate_seconds_by_day(rows, since, now_ts(), TZ_NAME, AFK_CHANNEL_ID or None)

    # Build ordered x-axis for the last N days (oldest -> newest)
    try:
        import zoneinfo
        tz = zoneinfo.ZoneInfo(TZ_NAME)
    except Exception:
        tz = timezone.utc

    days_list = []
    base = datetime.fromtimestamp(since, tz=tz).replace(hour=0, minute=0, second=0, microsecond=0)
    for i in range(days):
        d = base + timedelta(days=i)
        days_list.append(d.strftime("%Y-%m-%d"))

    values_hours = [(buckets.get(day, 0) / 3600.0) for day in days_list]

    # Plot
    plt.figure(figsize=PLOT_SIZE)
    x = list(range(len(days_list)))
    plt.bar(x, values_hours)
    # Show only a subset of x labels if many; rotate for readability
    plt.xticks(x, days_list, rotation=45, ha="right")
    subtitle = " (AFK excluded)" if AFK_CHANNEL_ID else ""
    plt.title(f"Daily voice activity (last {days}d){subtitle}")
    plt.ylabel("Hours")
    plt.xlabel("Day")
    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=150)
    plt.close()
    buf.seek(0)

    file = discord.File(buf, filename="voice_daily.png")
    await inter.followup.send(
        content=f"Anonymized server-wide daily totals for last **{days}d**.",
        file=file,
        ephemeral=not public
    )


@tree.command(name="voice_total", description="Show YOUR lifetime total voice time.", guild=GUILD_OBJ)
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

@tree.command(name="voice_current", description="List users currently in voice channels.", guild=GUILD_OBJ)
async def voice_current(inter: discord.Interaction):
    lines = []
    for vc in inter.guild.voice_channels:
        if vc.members:
            names = ", ".join(m.display_name for m in vc.members)
            lines.append(f"🔊 **{vc.name}**: {names}")
    msg = "\n".join(lines) if lines else "No one is in voice right now."
    await inter.response.send_message(msg)

@tree.command(name="voice_top", description="Leaderboard of top 50 voice users in the last N days", guild=GUILD_OBJ)
@app_commands.describe(
    days="How many days back (default 7)",
    private="Only available to special user; defaults to false"
)
async def voice_top(
    inter: discord.Interaction,
    days: app_commands.Range[int, 1, 3650] = 7,
    private: bool = False
):
    # Decide ephemerality once, then DEFER right away to avoid 10062
    is_ephemeral = (private and inter.user.id == VOICE_TOP_PRIVATE_USER)
    await inter.response.defer(thinking=True, ephemeral=is_ephemeral)

    since = now_ts() - days * 86400
    extra, params = afk_filter_clause()
    async with aiosqlite.connect(DB_PATH) as cx:
        async with cx.execute(f"""
            SELECT user_id,
                   SUM(COALESCE(left_ts, strftime('%s','now')) - joined_ts) AS total
            FROM voice_sessions
            WHERE joined_ts >= ?{extra}
            GROUP BY user_id
            ORDER BY total DESC
            LIMIT 50
        """, [since] + params) as cur:
            rows = await cur.fetchall()

    if not rows:
        await inter.followup.send("No voice activity in that window.", ephemeral=is_ephemeral)
        return

    # Resolve server nicknames; fetch on cache miss
    member_cache: dict[int, str] = {}

    async def label_for(uid: int) -> str:
        if uid in member_cache:
            return member_cache[uid]
        m = inter.guild.get_member(uid)
        if m is None:
            try:
                m = await inter.guild.fetch_member(uid)
            except (NotFound, Forbidden, Exception):
                member_cache[uid] = f"User {uid}"
                return member_cache[uid]
        name = escape_markdown((m.nick or m.name) or str(uid))
        member_cache[uid] = name
        return name

    lines = []
    for i, (uid, total) in enumerate(rows, start=1):
        name = await label_for(uid)
        lines.append(f"{i}. {name} — {fmt_duration(total)}")

    text = f"**Top 50 voice users (last {days}d):**\n" + "\n".join(lines)

    await inter.followup.send(
        text,
        ephemeral=is_ephemeral,
        allowed_mentions=discord.AllowedMentions.none()
    )




# -------- Run --------
if __name__ == "__main__":
    if not TOKEN:
        raise SystemExit("DISCORD_TOKEN missing in .env")
    client.run(TOKEN)
