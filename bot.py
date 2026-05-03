import os, time, aiosqlite, signal, asyncio
from datetime import datetime, timezone, timedelta
import discord
from discord import app_commands
from discord.ext import tasks
from dotenv import load_dotenv
import io
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import shutil
from discord.errors import NotFound, Forbidden
from discord.utils import escape_markdown
from collections import defaultdict


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

# Milestone thresholds in hours.
# Early game: 1, 5, 10, 25, 50, 100
# Then every 250h up to 5000
# Then every 500h after 5000 (up to 10 000)
def _build_milestone_hours() -> list[int]:
    ms = [1, 5, 10, 25, 50, 100]
    h = 250
    while h <= 5000:
        ms.append(h)
        h += 250
    h = 5500
    while h <= 10000:
        ms.append(h)
        h += 500
    return sorted(set(ms))

MILESTONE_HOURS = _build_milestone_hours()

# Max believable single session length. Any orphaned open session (from an
# ungraceful shutdown) gets capped at joined_ts + this many hours on restart.
MAX_SESSION_HOURS = int(os.getenv("MAX_SESSION_HOURS", "6"))

# -------- Intents --------
# FIX: intents.members=True ensures reliable member cache for small servers
intents = discord.Intents.default()
intents.guilds = True
intents.voice_states = True
intents.members = True  # requires "Server Members Intent" enabled in Dev Portal

client = discord.Client(intents=intents)


# Restrict all slash commands to specific channels
class RestrictedTree(app_commands.CommandTree):
    async def interaction_check(self, inter: discord.Interaction) -> bool:
        ch = inter.channel
        cid = getattr(inter, "channel_id", None) or (ch.id if ch else None)
        parent_id = getattr(ch, "parent_id", None) if isinstance(ch, discord.Thread) else None

        if cid in ALLOWED_CHANNELS or parent_id in ALLOWED_CHANNELS:
            return True

        where = " or ".join(f"<#{c}>" for c in ALLOWED_CHANNELS) or "the designated channels"
        try:
            if inter.response.is_done():
                await inter.followup.send(f"⛔ This command can only be used in {where}.", ephemeral=True)
            else:
                await inter.response.send_message(f"⛔ This command can only be used in {where}.", ephemeral=True)
        except Exception:
            pass
        return False


tree = RestrictedTree(client)
DB_PATH = "bot.db"
PLOT_SIZE = (15, 5)


# -------- Utils --------
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
    if AFK_CHANNEL_ID:
        return " AND channel_id != ? ", [AFK_CHANNEL_ID]
    return " ", []


def aggregate_seconds_by_hour(rows, since_ts: int, now_ts_: int, tz_name: str, afk_channel_id: int | None):
    """rows: list of (joined_ts, left_ts, channel_id). Returns [sec_per_hour_0..23]."""
    try:
        import zoneinfo
        tz = zoneinfo.ZoneInfo(tz_name)
    except Exception:
        tz = timezone.utc

    buckets = [0] * 24
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
            next_hour = (cur_dt.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
            boundary = min(int(next_hour.timestamp()), end)
            buckets[cur_dt.hour] += boundary - cur
            cur = boundary
    return buckets


def aggregate_seconds_by_weekday(rows, since_ts: int, now_ts_: int, tz_name: str, afk_channel_id: int | None):
    """rows: list of (joined_ts, left_ts, channel_id). Returns [sec_per_day Mon..Sun]."""
    try:
        import zoneinfo
        tz = zoneinfo.ZoneInfo(tz_name)
    except Exception:
        tz = timezone.utc

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
            next_day = (cur_dt.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1))
            boundary = min(int(next_day.timestamp()), end)
            buckets[cur_dt.weekday()] += boundary - cur
            cur = boundary
    return buckets


def aggregate_seconds_by_day(rows, since_ts: int, now_ts_: int, tz_name: str, afk_channel_id: int | None):
    """rows: list of (joined_ts, left_ts, channel_id). Returns dict {YYYY-MM-DD -> seconds}."""
    try:
        import zoneinfo
        tz = zoneinfo.ZoneInfo(tz_name)
    except Exception:
        tz = timezone.utc

    buckets = {}
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
            day_key = cur_dt.strftime("%Y-%m-%d")
            buckets[day_key] = buckets.get(day_key, 0) + (boundary - cur)
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


def aggregate_unique_users_by_day(rows, since_ts: int, tz_name: str, afk_channel_id: int | None):
    """rows: (user_id, channel_id, joined_ts, left_ts). Returns {YYYY-MM-DD: set(user_ids)}."""
    try:
        import zoneinfo
        tz = zoneinfo.ZoneInfo(tz_name)
    except Exception:
        tz = timezone.utc

    day_users = {}
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
            s = day_users.setdefault(day_key, set())
            s.add(user_id)
            cur = boundary
    return day_users


def peak_concurrency(rows, since_ts: int, tz_name: str, afk_channel_id: int | None):
    """Return overall peak count and per-day peaks {YYYY-MM-DD: peak}."""
    try:
        import zoneinfo
        tz = zoneinfo.ZoneInfo(tz_name)
    except Exception:
        tz = timezone.utc

    now_ = now_ts()
    events = []
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
    per_day_peak = {}

    for ts, delta in events:
        cur += delta
        if cur > overall_peak:
            overall_peak = cur
        day_key = datetime.fromtimestamp(ts, tz=tz).strftime("%Y-%m-%d")
        if cur > per_day_peak.get(day_key, 0):
            per_day_peak[day_key] = cur
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
        events.sort(key=lambda x: (x[0], x[2]))
        present: set[int] = set()
        prev_t: int | None = None

        for t, uid, delta in events:
            if prev_t is not None and len(present) == 1:
                only_uid = next(iter(present))
                solo_totals[only_uid] = solo_totals.get(only_uid, 0) + (t - prev_t)
            if delta == +1:
                present.add(uid)
            else:
                present.discard(uid)
            prev_t = t
    return solo_totals


def aggregate_night_seconds_per_user(
    rows, since_ts: int, now_ts_: int, tz_name: str,
    afk_channel_id: int | None, start_hour: int, end_hour: int
) -> dict[int, int]:
    """
    rows: (user_id, channel_id, joined_ts, left_ts).
    Returns {user_id: seconds} counting only time within [start_hour, end_hour).
    Handles midnight wraparound (e.g. start=23, end=4 covers 23:00–04:00).
    """
    try:
        import zoneinfo
        tz = zoneinfo.ZoneInfo(tz_name)
    except Exception:
        tz = timezone.utc

    def in_window(h: int) -> bool:
        if start_hour < end_hour:
            return start_hour <= h < end_hour
        elif start_hour > end_hour:          # wraps midnight
            return h >= start_hour or h < end_hour
        return True                          # start == end → full day

    user_secs: dict[int, int] = defaultdict(int)
    for uid, ch_id, joined_ts, left_ts in rows:
        if afk_channel_id and ch_id == afk_channel_id:
            continue
        start = max(joined_ts, since_ts)
        end = min(left_ts or now_ts_, now_ts_)
        if end <= start:
            continue
        cur = start
        while cur < end:
            cur_dt = datetime.fromtimestamp(cur, tz=tz)
            next_hour = cur_dt.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
            boundary = min(int(next_hour.timestamp()), end)
            if in_window(cur_dt.hour):
                user_secs[uid] += boundary - cur
            cur = boundary
    return dict(user_secs)


# -------- Admin guard --------
async def admin_guard(inter: discord.Interaction) -> bool:
    """
    Reusable guard for privacy-sensitive commands.
    Returns True if the user is an admin or has manage_guild.
    Sends an ephemeral denial and returns False otherwise.
    """
    perms = inter.user.guild_permissions
    if perms.administrator or perms.manage_guild:
        return True
    await inter.response.send_message(
        "⛔ This command is restricted to server administrators.", ephemeral=True
    )
    return False


# -------- Stat helpers --------
async def compute_streak(user_id: int) -> int:
    """Return the current consecutive-day streak (days with any non-AFK voice activity)."""
    try:
        import zoneinfo
        tz = zoneinfo.ZoneInfo(TZ_NAME)
    except Exception:
        tz = timezone.utc

    now = now_ts()
    afk_cond = " AND channel_id != ?" if AFK_CHANNEL_ID else ""
    params: list = [now, user_id]
    if AFK_CHANNEL_ID:
        params.append(AFK_CHANNEL_ID)

    async with aiosqlite.connect(DB_PATH) as cx:
        async with cx.execute(
            f"SELECT joined_ts, COALESCE(left_ts, ?) FROM voice_sessions WHERE user_id=?{afk_cond}",
            params
        ) as cur:
            sessions = await cur.fetchall()

    if not sessions:
        return 0

    active_days: set[str] = set()
    for joined_ts, left_ts in sessions:
        c = joined_ts
        while c < left_ts:
            dt_ = datetime.fromtimestamp(c, tz=tz)
            active_days.add(dt_.strftime("%Y-%m-%d"))
            c = int((dt_.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)).timestamp())

    today = datetime.now(tz).replace(hour=0, minute=0, second=0, microsecond=0)
    streak = 0
    d = today
    while d.strftime("%Y-%m-%d") in active_days:
        streak += 1
        d -= timedelta(days=1)
    return streak


async def compute_rank(user_id: int) -> tuple[int, int]:
    """Returns (rank, total_active_users) sorted by all-time voice time."""
    extra, params = afk_filter_clause()
    async with aiosqlite.connect(DB_PATH) as cx:
        async with cx.execute(
            f"SELECT user_id FROM voice_sessions WHERE 1=1{extra} "
            f"GROUP BY user_id "
            f"ORDER BY SUM(COALESCE(left_ts, strftime('%s','now')) - joined_ts) DESC",
            params
        ) as cur:
            rows = await cur.fetchall()
    uids = [r[0] for r in rows]
    total = len(uids)
    rank = (uids.index(user_id) + 1) if user_id in uids else total + 1
    return rank, total


async def compute_together_seconds(user1_id: int, user2_id: int) -> int:
    """Total seconds user1 and user2 spent in the same voice channel simultaneously."""
    now = now_ts()
    async with aiosqlite.connect(DB_PATH) as cx:
        async with cx.execute(
            "SELECT channel_id, joined_ts, COALESCE(left_ts, ?) FROM voice_sessions WHERE user_id=?",
            (now, user1_id)
        ) as cur:
            sessions1 = await cur.fetchall()
        async with cx.execute(
            "SELECT channel_id, joined_ts, COALESCE(left_ts, ?) FROM voice_sessions WHERE user_id=?",
            (now, user2_id)
        ) as cur:
            sessions2 = await cur.fetchall()

    by_ch1: dict[int, list[tuple[int, int]]] = defaultdict(list)
    by_ch2: dict[int, list[tuple[int, int]]] = defaultdict(list)
    for ch, s, e in sessions1:
        by_ch1[ch].append((s, e))
    for ch, s, e in sessions2:
        by_ch2[ch].append((s, e))

    total = 0
    for ch in set(by_ch1) & set(by_ch2):
        for s1, e1 in by_ch1[ch]:
            for s2, e2 in by_ch2[ch]:
                overlap = min(e1, e2) - max(s1, s2)
                if overlap > 0:
                    total += overlap
    return total


async def compute_all_pairs(since_ts: int | None = None) -> dict[tuple[int, int], int]:
    """
    Returns {(uid_a, uid_b): seconds_together} for every unique pair of users
    who shared a voice channel. uid_a < uid_b always so (a,b) == (b,a).
    Optionally restricted to sessions overlapping [since_ts, now].
    """
    now = now_ts()
    since_ts = since_ts or 0

    async with aiosqlite.connect(DB_PATH) as cx:
        async with cx.execute(
            """SELECT user_id, channel_id, joined_ts, COALESCE(left_ts, ?)
               FROM voice_sessions
               WHERE COALESCE(left_ts, ?) > ?""",
            (now, now, since_ts)
        ) as cur:
            rows = await cur.fetchall()

    # Group sessions by channel, clamping to window
    by_ch: dict[int, list[tuple[int, int, int]]] = defaultdict(list)
    for uid, ch_id, s, e in rows:
        if AFK_CHANNEL_ID and ch_id == AFK_CHANNEL_ID:
            continue
        by_ch[ch_id].append((uid, max(s, since_ts), e))

    pairs: dict[tuple[int, int], int] = defaultdict(int)
    for sessions in by_ch.values():
        for i in range(len(sessions)):
            uid1, s1, e1 = sessions[i]
            for j in range(i + 1, len(sessions)):
                uid2, s2, e2 = sessions[j]
                if uid1 == uid2:
                    continue
                overlap = min(e1, e2) - max(s1, s2)
                if overlap > 0:
                    pair = (min(uid1, uid2), max(uid1, uid2))
                    pairs[pair] += overlap

    return dict(pairs)


async def compute_together_all(user_id: int, since_ts: int | None = None) -> dict[int, int]:
    """
    Returns {other_user_id: seconds_together} for every user who has shared
    a voice channel with user_id. Single DB round-trip — efficient for small servers.
    Optionally restricted to sessions overlapping [since_ts, now].
    """
    now = now_ts()
    since_ts = since_ts or 0

    async with aiosqlite.connect(DB_PATH) as cx:
        async with cx.execute(
            """SELECT channel_id, joined_ts, COALESCE(left_ts, ?)
               FROM voice_sessions
               WHERE user_id=? AND COALESCE(left_ts, ?) > ?""",
            (now, user_id, now, since_ts)
        ) as cur:
            my_sessions = await cur.fetchall()
        async with cx.execute(
            """SELECT user_id, channel_id, joined_ts, COALESCE(left_ts, ?)
               FROM voice_sessions
               WHERE user_id != ? AND COALESCE(left_ts, ?) > ?""",
            (now, user_id, now, since_ts)
        ) as cur:
            all_sessions = await cur.fetchall()

    # Group my sessions by channel, clamped to the window
    my_by_ch: dict[int, list[tuple[int, int]]] = defaultdict(list)
    for ch, s, e in my_sessions:
        my_by_ch[ch].append((max(s, since_ts), e))

    together: dict[int, int] = defaultdict(int)
    for other_uid, ch, s2, e2 in all_sessions:
        s2c = max(s2, since_ts)
        for s1, e1 in my_by_ch.get(ch, []):
            overlap = min(e1, e2) - max(s1, s2c)
            if overlap > 0:
                together[other_uid] += overlap

    return dict(together)


# -------- Shutdown & reconciliation --------
async def close_open_sessions():
    """
    Close every session that still has left_ts IS NULL right now.
    Called on graceful shutdown (SIGTERM / Ctrl+C) so planned downtime
    doesn't leave orphaned sessions that accumulate time forever.
    """
    now = now_ts()
    async with aiosqlite.connect(DB_PATH) as cx:
        result = await cx.execute(
            "UPDATE voice_sessions SET left_ts=? WHERE left_ts IS NULL", (now,)
        )
        await cx.commit()
        count = result.rowcount
    if count:
        print(f"🔒 Closed {count} open session(s) on shutdown.")


async def reconcile_open_sessions(guild: discord.Guild):
    """
    Called once in on_ready, after every restart (graceful or not).

    For each session that survived with left_ts IS NULL:
      - User IS still in that channel → close the old row (it includes any
        downtime gap) and open a clean new session starting right now.
      - User is NOT in voice   → cap left_ts at joined_ts + MAX_SESSION_HOURS
        so a 10-minute session before a 3-day outage doesn't count as 3 days.
    """
    now = now_ts()
    max_secs = MAX_SESSION_HOURS * 3600

    async with aiosqlite.connect(DB_PATH) as cx:
        async with cx.execute(
            "SELECT id, user_id, channel_id, joined_ts FROM voice_sessions WHERE left_ts IS NULL"
        ) as cur:
            orphans = await cur.fetchall()

    if not orphans:
        return

    print(f"🔧 Reconciling {len(orphans)} orphaned session(s) from before restart...")

    # Map (user_id, channel_id) → True for everyone currently in voice
    live: set[tuple[int, int]] = set()
    for vc in guild.voice_channels:
        for m in vc.members:
            live.add((m.id, vc.id))

    async with aiosqlite.connect(DB_PATH) as cx:
        for sess_id, user_id, channel_id, joined_ts in orphans:
            if (user_id, channel_id) in live:
                # Still in voice: close the gap-infected row, open a fresh one
                await cx.execute(
                    "UPDATE voice_sessions SET left_ts=? WHERE id=?", (now, sess_id)
                )
                await cx.execute(
                    "INSERT INTO voice_sessions(user_id, channel_id, joined_ts) VALUES(?,?,?)",
                    (user_id, channel_id, now)
                )
                print(f"  ↻ uid={user_id} still in vc={channel_id}: session reset")
            else:
                # Left while bot was down: cap to MAX_SESSION_HOURS
                capped = min(now, joined_ts + max_secs)
                await cx.execute(
                    "UPDATE voice_sessions SET left_ts=? WHERE id=?", (capped, sess_id)
                )
                print(
                    f"  ✂ uid={user_id} left vc={channel_id} while offline: "
                    f"capped at {fmt_duration(capped - joined_ts)}"
                )
        await cx.commit()

    print("✅ Session reconciliation complete.")


async def graceful_shutdown():
    """Flush open sessions then close the Discord client."""
    print("⚠️  Shutdown signal received — flushing open sessions...")
    await close_open_sessions()
    await client.close()


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
        CREATE TABLE IF NOT EXISTS milestones (
          user_id  INTEGER NOT NULL,
          hours    INTEGER NOT NULL,
          awarded_ts INTEGER NOT NULL,
          PRIMARY KEY (user_id, hours)
        );
        CREATE INDEX IF NOT EXISTS idx_voice_open ON voice_sessions(user_id) WHERE left_ts IS NULL;
        CREATE INDEX IF NOT EXISTS idx_voice_join ON voice_sessions(joined_ts);
        """)
        await cx.commit()


# -------- Background tasks --------
@tasks.loop(seconds=60)
async def milestone_check():
    """
    Real-time milestone detection.
    Runs every 60 seconds, checks all users currently in voice,
    and posts to BOT_CHANNEL the first time each threshold is crossed.
    """
    if not GUILD_ID or not BOT_CHANNEL:
        return
    guild = client.get_guild(int(GUILD_ID))
    if not guild:
        return
    channel = guild.get_channel(BOT_CHANNEL)
    if not channel:
        return

    now = now_ts()
    extra, afk_params = afk_filter_clause()

    async with aiosqlite.connect(DB_PATH) as cx:
        async with cx.execute(
            "SELECT user_id, channel_id FROM voice_sessions WHERE left_ts IS NULL"
        ) as cur:
            active = await cur.fetchall()

    for user_id, channel_id in active:
        if AFK_CHANNEL_ID and channel_id == AFK_CHANNEL_ID:
            continue

        # Cumulative total including the ongoing session
        async with aiosqlite.connect(DB_PATH) as cx:
            async with cx.execute(
                f"SELECT SUM(COALESCE(left_ts, ?) - joined_ts) FROM voice_sessions WHERE user_id=?{extra}",
                [now, user_id] + afk_params
            ) as cur:
                row = await cur.fetchone()

        total_seconds = row[0] or 0
        total_hours = total_seconds / 3600

        for threshold in MILESTONE_HOURS:
            if total_hours >= threshold:
                async with aiosqlite.connect(DB_PATH) as cx:
                    ins = await cx.execute(
                        "INSERT OR IGNORE INTO milestones(user_id, hours, awarded_ts) VALUES(?,?,?)",
                        (user_id, threshold, now)
                    )
                    await cx.commit()
                    if ins.rowcount > 0:
                        member = guild.get_member(user_id)
                        name = escape_markdown(member.display_name if member else f"User {user_id}")
                        await channel.send(
                            f"🏆 **{name}** just hit **{threshold}h** of voice time! "
                            f"(all-time: {fmt_duration(total_seconds)})"
                        )


async def _build_and_send_recap(guild: discord.Guild, channel, month_override: str | None = None):
    """
    Core logic for the monthly recap. Extracted so both the scheduled task
    and the manual /voice_recap command can call it.
    month_override: optional 'YYYY-MM' string to recap a specific past month.
    """
    try:
        import zoneinfo
        tz = zoneinfo.ZoneInfo(TZ_NAME)
    except Exception:
        tz = timezone.utc

    now_local = datetime.now(tz)

    if month_override:
        # Parse 'YYYY-MM' and build the window for that specific month
        year, month = map(int, month_override.split("-"))
        first_last_month = datetime(year, month, 1, tzinfo=tz)
        if month == 12:
            first_this_month = datetime(year + 1, 1, 1, tzinfo=tz)
        else:
            first_this_month = datetime(year, month + 1, 1, tzinfo=tz)
    else:
        first_this_month = now_local.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if first_this_month.month == 1:
            first_last_month = first_this_month.replace(year=first_this_month.year - 1, month=12)
        else:
            first_last_month = first_this_month.replace(month=first_this_month.month - 1)

    since = int(first_last_month.timestamp())
    until = int(first_this_month.timestamp())
    month_label = first_last_month.strftime("%B %Y")

    async with aiosqlite.connect(DB_PATH) as cx:
        async with cx.execute(
            """
            SELECT user_id, channel_id, joined_ts, left_ts
            FROM voice_sessions
            WHERE joined_ts < ? AND COALESCE(left_ts, ?) > ?
            """,
            (until, until, since)
        ) as cur:
            rows = await cur.fetchall()

    user_secs: dict[int, int] = defaultdict(int)
    total_secs = 0
    for uid, ch_id, joined_ts, left_ts in rows:
        if AFK_CHANNEL_ID and ch_id == AFK_CHANNEL_ID:
            continue
        start = max(joined_ts, since)
        end = min(left_ts or until, until)
        if end > start:
            dur = end - start
            user_secs[uid] += dur
            total_secs += dur

    day_rows = [(joined_ts, left_ts, ch_id) for _, ch_id, joined_ts, left_ts in rows]
    day_buckets = aggregate_seconds_by_day(day_rows, since, until, TZ_NAME, AFK_CHANNEL_ID or None)
    best_day = max(day_buckets, key=day_buckets.get) if day_buckets else None
    best_day_str = f"{best_day} ({fmt_duration(day_buckets[best_day])})" if best_day else "N/A"

    overall_peak, _ = peak_concurrency(rows, since, TZ_NAME, AFK_CHANNEL_ID or None)
    unique_count = len(user_secs)

    medals = ["🥇", "🥈", "🥉", "4.", "5."]
    top5 = sorted(user_secs.items(), key=lambda x: x[1], reverse=True)[:5]
    leaderboard_lines = []
    for i, (uid, secs) in enumerate(top5):
        m = guild.get_member(uid)
        name = escape_markdown(m.display_name if m else f"User {uid}")
        leaderboard_lines.append(f"{medals[i]} **{name}** — {fmt_duration(secs)}")
    leaderboard_str = "\n".join(leaderboard_lines) if leaderboard_lines else "No activity"

    embed = discord.Embed(
        title=f"📅  {month_label} — Voice Recap",
        color=discord.Color.blurple()
    )
    embed.add_field(name="🕐 Total Voice Time", value=fmt_duration(total_secs), inline=True)
    embed.add_field(name="👥 Participants",      value=str(unique_count),        inline=True)
    embed.add_field(name="📈 Peak Concurrent",   value=str(overall_peak),        inline=True)
    embed.add_field(name="🔥 Most Active Day",   value=best_day_str,             inline=False)
    embed.add_field(name="🏆 Top Members",       value=leaderboard_str,          inline=False)
    embed.set_footer(text=f"{guild.name} • Generated on {datetime.now(tz).strftime('%d %b %Y')}")

    await channel.send(embed=embed)
    print(f"✅ Monthly recap posted for {month_label}")


@tasks.loop(hours=24)
async def monthly_recap():
    """Posts a server-wide voice recap on the 1st of every month covering the previous month."""
    if not GUILD_ID or not BOT_CHANNEL:
        return
    try:
        import zoneinfo
        tz = zoneinfo.ZoneInfo(TZ_NAME)
    except Exception:
        tz = timezone.utc

    now_local = datetime.now(tz)
    if now_local.day != 1:  # only fire on the 1st of the month
        return

    guild = client.get_guild(int(GUILD_ID))
    if not guild:
        print("⚠️  monthly_recap: guild not found")
        return
    channel = guild.get_channel(BOT_CHANNEL)
    if not channel:
        print(f"⚠️  monthly_recap: BOT_CHANNEL {BOT_CHANNEL} not found")
        return

    # Window: midnight on the 1st of last month → midnight on the 1st of this month
    first_this_month = now_local.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if first_this_month.month == 1:
        first_last_month = first_this_month.replace(year=first_this_month.year - 1, month=12)
    else:
        first_last_month = first_this_month.replace(month=first_this_month.month - 1)

    since = int(first_last_month.timestamp())
    until = int(first_this_month.timestamp())
    month_label = first_last_month.strftime("%B %Y")  # e.g. "April 2026"

    # Fetch sessions overlapping last month's window
    async with aiosqlite.connect(DB_PATH) as cx:
        async with cx.execute(
            """
            SELECT user_id, channel_id, joined_ts, left_ts
            FROM voice_sessions
            WHERE joined_ts < ? AND COALESCE(left_ts, ?) > ?
            """,
            (until, until, since)
        ) as cur:
            rows = await cur.fetchall()

    user_secs: dict[int, int] = defaultdict(int)
    total_secs = 0

    for uid, ch_id, joined_ts, left_ts in rows:
        if AFK_CHANNEL_ID and ch_id == AFK_CHANNEL_ID:
            continue
        start = max(joined_ts, since)
        end = min(left_ts or until, until)
        if end > start:
            dur = end - start
            user_secs[uid] += dur
            total_secs += dur

    # Most active day
    day_rows = [(joined_ts, left_ts, ch_id) for _, ch_id, joined_ts, left_ts in rows]
    day_buckets = aggregate_seconds_by_day(day_rows, since, until, TZ_NAME, AFK_CHANNEL_ID or None)
    best_day = max(day_buckets, key=day_buckets.get) if day_buckets else None
    best_day_str = f"{best_day} ({fmt_duration(day_buckets[best_day])})" if best_day else "N/A"

    overall_peak, _ = peak_concurrency(rows, since, TZ_NAME, AFK_CHANNEL_ID or None)
    unique_count = len(user_secs)

    await _build_and_send_recap(guild, channel)


@monthly_recap.error
async def monthly_recap_error(error: Exception):
    print(f"❌ monthly_recap task crashed: {error!r} — restarting task")
    if not monthly_recap.is_running():
        monthly_recap.start()


# -------- Startup --------
@client.event
async def setup_hook():
    print("DEBUG pre-sync commands:", [c.name for c in tree.get_commands(guild=GUILD_OBJ)])


@client.event
async def on_ready():
    await ensure_schema()

    # Register graceful-shutdown handlers (SIGTERM = systemd stop, SIGINT = Ctrl+C)
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, lambda: asyncio.ensure_future(graceful_shutdown()))
        except NotImplementedError:
            pass  # Windows doesn't support add_signal_handler; Pi is fine

    # Fix any sessions orphaned by a previous ungraceful shutdown
    guild = client.get_guild(int(GUILD_ID)) if GUILD_ID else None
    if guild:
        await reconcile_open_sessions(guild)

    try:
        if GUILD_OBJ:
            await tree.sync(guild=GUILD_OBJ)
            print(f"✅ Synced slash commands to guild {GUILD_ID}")
            await tree.sync()  # clear any stale global commands
            print("🧹 Cleared global commands.")
        else:
            synced = await tree.sync()
            print(f"✅ Synced {len(synced)} global slash commands")
    except Exception as e:
        print(f"❌ Slash command sync failed: {e}")

    # Start background tasks
    if not milestone_check.is_running():
        milestone_check.start()
    if not monthly_recap.is_running():
        monthly_recap.start()

    print(f"Bot online as {client.user}")


# -------- Voice tracking --------
@client.event
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
    now = now_ts()
    if before.channel is None and after.channel is not None:
        async with aiosqlite.connect(DB_PATH) as cx:
            await cx.execute(
                "INSERT INTO voice_sessions(user_id, channel_id, joined_ts) VALUES(?,?,?)",
                (member.id, after.channel.id, now)
            )
            await cx.commit()
    elif before.channel is not None and after.channel is None:
        async with aiosqlite.connect(DB_PATH) as cx:
            await cx.execute(
                "UPDATE voice_sessions SET left_ts=? WHERE user_id=? AND channel_id=? AND left_ts IS NULL",
                (now, member.id, before.channel.id)
            )
            await cx.commit()
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


# -------- Slash commands --------

@tree.command(
    name="voice_recap",
    description="[Admin only] Manually post the monthly voice recap. Use if the scheduled post was missed.",
    guild=GUILD_OBJ
)
@app_commands.describe(
    month="Month to recap in YYYY-MM format (default: last month, e.g. 2026-04)"
)
async def voice_recap(inter: discord.Interaction, month: str | None = None):
    if not await admin_guard(inter):
        return

    # Validate optional month override
    if month:
        try:
            year, mon = map(int, month.split("-"))
            if not (1 <= mon <= 12):
                raise ValueError
        except (ValueError, AttributeError):
            await inter.response.send_message(
                "❌ Invalid format. Use `YYYY-MM`, e.g. `2026-04`.", ephemeral=True
            )
            return

    await inter.response.defer(thinking=True)

    channel = inter.guild.get_channel(BOT_CHANNEL) or inter.channel
    try:
        await _build_and_send_recap(inter.guild, channel, month_override=month)
        await inter.followup.send("✅ Recap posted.", ephemeral=True)
    except Exception as e:
        await inter.followup.send(f"❌ Failed to post recap: `{e}`", ephemeral=True)


@tree.command(
    name="pi_storage",
    description="Show disk usage on your Pi, ephemerally and restricted.",
    guild=GUILD_OBJ
)
@app_commands.describe(path="Filesystem path to check (default '/')")
async def pi_storage(inter: discord.Interaction, path: str = "/"):
    if inter.user.id != VOICE_TOP_PRIVATE_USER:
        await inter.response.send_message("⛔ This command is restricted.", ephemeral=True)
        return
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
            f"❌ Couldn't read disk usage for `{path}`: `{e}`", ephemeral=True
        )


@tree.command(
    name="voice_me",
    description="Your personal voice dashboard: lifetime, recent, rank, and streak.",
    guild=GUILD_OBJ
)
async def voice_me(inter: discord.Interaction):
    await inter.response.defer(thinking=True, ephemeral=True)

    now = now_ts()
    uid = inter.user.id
    extra, params = afk_filter_clause()

    async with aiosqlite.connect(DB_PATH) as cx:
        # Lifetime total
        async with cx.execute(
            f"SELECT SUM(COALESCE(left_ts, ?) - joined_ts) FROM voice_sessions WHERE user_id=?{extra}",
            [now, uid] + params
        ) as cur:
            lifetime = (await cur.fetchone())[0] or 0

        # Last 7 days
        since_7 = now - 7 * 86400
        async with cx.execute(
            f"SELECT SUM(COALESCE(left_ts, ?) - joined_ts) FROM voice_sessions "
            f"WHERE user_id=? AND joined_ts >= ?{extra}",
            [now, uid, since_7] + params
        ) as cur:
            last7 = (await cur.fetchone())[0] or 0

        # Last 30 days
        since_30 = now - 30 * 86400
        async with cx.execute(
            f"SELECT SUM(COALESCE(left_ts, ?) - joined_ts) FROM voice_sessions "
            f"WHERE user_id=? AND joined_ts >= ?{extra}",
            [now, uid, since_30] + params
        ) as cur:
            last30 = (await cur.fetchone())[0] or 0

    rank, total_users = await compute_rank(uid)
    streak = await compute_streak(uid)
    streak_str = f"{streak} day{'s' if streak != 1 else ''} 🔥" if streak > 0 else "0 days"

    # Time spent with each other user — last 30 days only
    together_map = await compute_together_all(uid, since_ts=since_30)
    medals = ["🥇", "🥈", "🥉", "4.", "5."]
    top_together = sorted(together_map.items(), key=lambda x: x[1], reverse=True)[:5]
    together_lines = []
    for i, (other_uid, secs) in enumerate(top_together):
        m = inter.guild.get_member(other_uid)
        name = escape_markdown(m.display_name if m else f"User {other_uid}")
        together_lines.append(f"{medals[i]} **{name}** — {fmt_duration(secs)}")
    together_str = "\n".join(together_lines) if together_lines else "No shared voice time yet"

    embed = discord.Embed(
        title=f"📊  {escape_markdown(inter.user.display_name)}",
        color=discord.Color.blurple()
    )
    embed.add_field(name="⏱️ Lifetime",      value=fmt_duration(lifetime), inline=True)
    embed.add_field(name="📅 Last 7 days",   value=fmt_duration(last7),    inline=True)
    embed.add_field(name="🗓️ Last 30 days",  value=fmt_duration(last30),   inline=True)
    embed.add_field(name="🏅 Server rank",   value=f"#{rank} of {total_users}", inline=True)
    embed.add_field(name="🔥 Streak",        value=streak_str,             inline=True)
    embed.add_field(name="​",           value="​",               inline=True)  # spacer
    embed.add_field(name="🎙️ Most time with (last 30d)", value=together_str, inline=False)

    await inter.followup.send(embed=embed, ephemeral=True)


@tree.command(
    name="voice_together",
    description="[Admin only] How much time two members have spent in voice together.",
    guild=GUILD_OBJ
)
@app_commands.describe(
    user1="First member",
    user2="Second member",
    private="Only available to special user; post privately (default: false)"
)
async def voice_together(inter: discord.Interaction, user1: discord.Member, user2: discord.Member, private: bool = False):
    if not await admin_guard(inter):
        return

    is_ephemeral = (private and inter.user.id == VOICE_TOP_PRIVATE_USER)
    await inter.response.defer(thinking=True, ephemeral=is_ephemeral)

    total = await compute_together_seconds(user1.id, user2.id)
    name1 = escape_markdown(user1.display_name)
    name2 = escape_markdown(user2.display_name)

    if total == 0:
        await inter.followup.send(
            f"No shared voice time found between **{name1}** and **{name2}**.",
            ephemeral=is_ephemeral
        )
    else:
        await inter.followup.send(
            f"🎙️ **{name1}** and **{name2}** have spent **{fmt_duration(total)}** in voice together.",
            ephemeral=is_ephemeral
        )


@tree.command(
    name="voice_bestfriends",
    description="Top pairs of users by time spent in voice together.",
    guild=GUILD_OBJ
)
@app_commands.describe(
    days="How many days back (default 30; >30 requires admin)",
    private="Only available to special user; post privately (default: false)"
)
async def voice_bestfriends(
    inter: discord.Interaction,
    days: app_commands.Range[int, 1, 3650] = 30,
    private: bool = False
):
    if days > 30 and not (inter.user.guild_permissions.administrator or inter.user.guild_permissions.manage_guild):
        await inter.response.send_message("⛔ Only admins can request more than 30 days.", ephemeral=True)
        return

    is_ephemeral = (private and inter.user.id == VOICE_TOP_PRIVATE_USER)
    await inter.response.defer(thinking=True, ephemeral=is_ephemeral)

    since = now_ts() - days * 86400
    pairs = await compute_all_pairs(since_ts=since)

    if not pairs:
        await inter.followup.send("No shared voice time recorded in that window.", ephemeral=is_ephemeral)
        return

    top = sorted(pairs.items(), key=lambda x: x[1], reverse=True)[:10]

    medals = ["🥇", "🥈", "🥉"] + [f"{i}." for i in range(4, 11)]
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
        name = escape_markdown(m.display_name)
        member_cache[uid] = name
        return name

    lines = []
    for i, ((uid_a, uid_b), secs) in enumerate(top):
        name_a = await label_for(uid_a)
        name_b = await label_for(uid_b)
        lines.append(f"{medals[i]} **{name_a}** & **{name_b}** — {fmt_duration(secs)}")

    await inter.followup.send(
        f"💞 **Best friends leaderboard (last {days}d):**\n" + "\n".join(lines),
        ephemeral=is_ephemeral,
        allowed_mentions=discord.AllowedMentions.none()
    )


@tree.command(
    name="voice_history",
    description="See your last 10 voice sessions with timestamps and duration.",
    guild=GUILD_OBJ
)
@app_commands.describe(
    private="Post privately — only works if you're the designated private user (default: false)"
)
async def voice_history(inter: discord.Interaction, private: bool = False):
    is_ephemeral = (private and inter.user.id == VOICE_TOP_PRIVATE_USER)
    await inter.response.defer(thinking=True, ephemeral=is_ephemeral)

    now = now_ts()
    async with aiosqlite.connect(DB_PATH) as cx:
        async with cx.execute(
            "SELECT channel_id, joined_ts, left_ts FROM voice_sessions "
            "WHERE user_id=? ORDER BY joined_ts DESC LIMIT 10",
            (inter.user.id,)
        ) as cur:
            rows = await cur.fetchall()

    if not rows:
        await inter.followup.send("No voice sessions recorded yet.", ephemeral=is_ephemeral)
        return

    lines = []
    for ch_id, joined_ts, left_ts in rows:
        channel = inter.guild.get_channel(ch_id)
        ch_name = channel.name if channel else "Unknown channel"
        duration = (left_ts or now) - joined_ts
        date_str = ts_to_local(joined_ts)
        ongoing = " 🔴 *ongoing*" if left_ts is None else ""
        lines.append(f"`{date_str}` **{ch_name}** — {fmt_duration(duration)}{ongoing}")

    await inter.followup.send(
        f"📋 **Last {len(rows)} sessions for {escape_markdown(inter.user.display_name)}:**\n"
        + "\n".join(lines),
        ephemeral=is_ephemeral
    )


@tree.command(
    name="voice_marathon",
    description="Top 10 longest single voice sessions on the server.",
    guild=GUILD_OBJ
)
@app_commands.describe(
    days="How many days back to search (default 7; >7 requires admin)",
    private="Post privately — only works if you're the designated private user (default: false)"
)
async def voice_marathon(
    inter: discord.Interaction,
    days: app_commands.Range[int, 1, 3650] = 7,
    private: bool = False
):
    if days > 7 and not (inter.user.guild_permissions.administrator or inter.user.guild_permissions.manage_guild):
        await inter.response.send_message("⛔ Only admins can request more than 7 days.", ephemeral=True)
        return

    is_ephemeral = (private and inter.user.id == VOICE_TOP_PRIVATE_USER)
    await inter.response.defer(thinking=True, ephemeral=is_ephemeral)

    since = now_ts() - days * 86400
    now = now_ts()
    afk_cond = " AND channel_id != ?" if AFK_CHANNEL_ID else ""
    afk_params = [AFK_CHANNEL_ID] if AFK_CHANNEL_ID else []

    async with aiosqlite.connect(DB_PATH) as cx:
        async with cx.execute(
            f"SELECT user_id, channel_id, joined_ts, "
            f"COALESCE(left_ts, ?) - joined_ts AS duration "
            f"FROM voice_sessions "
            f"WHERE joined_ts >= ?{afk_cond} "
            f"ORDER BY duration DESC LIMIT 10",
            [now, since] + afk_params
        ) as cur:
            rows = await cur.fetchall()

    if not rows:
        await inter.followup.send("No sessions recorded in that window.", ephemeral=is_ephemeral)
        return

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
        name = escape_markdown(m.display_name)
        member_cache[uid] = name
        return name

    medals = ["🥇", "🥈", "🥉"] + [f"{i}." for i in range(4, 11)]
    lines = []
    for i, (uid, ch_id, joined_ts, duration) in enumerate(rows):
        name = await label_for(uid)
        channel = inter.guild.get_channel(ch_id)
        ch_name = channel.name if channel else "Unknown"
        date_str = ts_to_local(joined_ts)
        lines.append(f"{medals[i]} **{name}** — {fmt_duration(duration)} in **{ch_name}** (`{date_str}`)")

    await inter.followup.send(
        f"🏃 **Longest voice sessions (last {days}d):**\n" + "\n".join(lines),
        ephemeral=is_ephemeral,
        allowed_mentions=discord.AllowedMentions.none()
    )


@tree.command(
    name="voice_rivalry",
    description="Head-to-head voice stats between you and another member.",
    guild=GUILD_OBJ
)
@app_commands.describe(
    opponent="The member to compare yourself against",
    days="How many days back for recent stats (default 7; >7 requires admin)",
    private="Post privately — only works if you're the designated private user (default: false)"
)
async def voice_rivalry(
    inter: discord.Interaction,
    opponent: discord.Member,
    days: app_commands.Range[int, 1, 3650] = 7,
    private: bool = False
):
    if days > 7 and not (inter.user.guild_permissions.administrator or inter.user.guild_permissions.manage_guild):
        await inter.response.send_message("⛔ Only admins can request more than 7 days.", ephemeral=True)
        return

    if opponent.id == inter.user.id:
        await inter.response.send_message("Pick someone else to rival.", ephemeral=True)
        return

    is_ephemeral = (private and inter.user.id == VOICE_TOP_PRIVATE_USER)
    await inter.response.defer(thinking=True, ephemeral=is_ephemeral)

    now = now_ts()
    since = now - days * 86400
    extra, params = afk_filter_clause()
    uid1, uid2 = inter.user.id, opponent.id

    async with aiosqlite.connect(DB_PATH) as cx:
        async def get_total(uid, since_ts=0):
            async with cx.execute(
                f"SELECT SUM(COALESCE(left_ts, ?) - joined_ts) FROM voice_sessions "
                f"WHERE user_id=? AND joined_ts >= ?{extra}",
                [now, uid, since_ts] + params
            ) as c:
                return (await c.fetchone())[0] or 0

        lifetime1 = await get_total(uid1)
        lifetime2 = await get_total(uid2)
        recent1   = await get_total(uid1, since)
        recent2   = await get_total(uid2, since)

    streak1 = await compute_streak(uid1)
    streak2 = await compute_streak(uid2)
    rank1, total_users = await compute_rank(uid1)
    rank2, _           = await compute_rank(uid2)

    # Solo time in the window
    rows = await fetch_sessions_window(since)
    solo_map = solo_seconds_per_user(rows, since, TZ_NAME, AFK_CHANNEL_ID or None)
    solo1 = solo_map.get(uid1, 0)
    solo2 = solo_map.get(uid2, 0)

    # Time together (all time)
    together = await compute_together_seconds(uid1, uid2)

    def stat_line(val1, val2, fmt):
        s1, s2 = fmt(val1), fmt(val2)
        if val1 > val2:
            return f"**{s1}** ✦", s2
        elif val2 > val1:
            return s1, f"**{s2}** ✦"
        return s1, s2

    name1 = escape_markdown(inter.user.display_name)
    name2 = escape_markdown(opponent.display_name)

    def dur(s): return fmt_duration(s)
    def rnk(r): return f"#{r} of {total_users}"
    def stk(s): return f"{s}d 🔥" if s else "0d"

    l1, l2   = stat_line(lifetime1, lifetime2, dur)
    r1, r2   = stat_line(recent1,   recent2,   dur)
    so1, so2 = stat_line(solo1,     solo2,     dur)
    rk1, rk2 = (rnk(rank1), rnk(rank2)) if rank1 < rank2 else (rnk(rank1), rnk(rank2))
    st1, st2 = stat_line(streak1, streak2, stk)

    embed = discord.Embed(
        title=f"⚔️  {name1}  vs.  {name2}",
        color=discord.Color.red()
    )
    embed.add_field(
        name=f"📊 {name1}",
        value=(
            f"⏱️ Lifetime: {l1}\n"
            f"📅 Last {days}d: {r1}\n"
            f"🎯 Solo: {so1}\n"
            f"🔥 Streak: {st1}\n"
            f"🏅 Rank: {rk1}"
        ),
        inline=True
    )
    embed.add_field(
        name=f"📊 {name2}",
        value=(
            f"⏱️ Lifetime: {l2}\n"
            f"📅 Last {days}d: {r2}\n"
            f"🎯 Solo: {so2}\n"
            f"🔥 Streak: {st2}\n"
            f"🏅 Rank: {rk2}"
        ),
        inline=True
    )
    embed.add_field(
        name="🎙️ Time together (all time)",
        value=fmt_duration(together) if together else "None recorded",
        inline=False
    )

    await inter.followup.send(embed=embed, ephemeral=is_ephemeral)


@tree.command(
    name="voice_ghost",
    description="Members who have been absent from voice the longest — call them out.",
    guild=GUILD_OBJ
)
@app_commands.describe(
    private="Post privately — only works if you're the designated private user (default: false)"
)
async def voice_ghost(inter: discord.Interaction, private: bool = False):
    is_ephemeral = (private and inter.user.id == VOICE_TOP_PRIVATE_USER)
    await inter.response.defer(thinking=True, ephemeral=is_ephemeral)

    now = now_ts()

    async with aiosqlite.connect(DB_PATH) as cx:
        async with cx.execute(
            "SELECT user_id, MAX(COALESCE(left_ts, joined_ts)) AS last_seen "
            "FROM voice_sessions GROUP BY user_id ORDER BY last_seen ASC"
        ) as cur:
            rows = await cur.fetchall()

    if not rows:
        await inter.followup.send("No voice history recorded yet.", ephemeral=is_ephemeral)
        return

    lines = []
    shown = 0
    for uid, last_seen in rows:
        if shown >= 10:
            break
        m = inter.guild.get_member(uid)
        if m is None:
            continue  # skip members who've left the server
        days_ago = (now - last_seen) // 86400
        name = escape_markdown(m.display_name)
        if days_ago == 0:
            label = "today"
        elif days_ago == 1:
            label = "yesterday"
        else:
            label = f"{days_ago} days ago"
        lines.append(f"👻 **{name}** — last seen **{label}** (`{ts_to_local(last_seen)}`)")
        shown += 1

    if not lines:
        await inter.followup.send("Couldn't find any absent members.", ephemeral=is_ephemeral)
        return

    await inter.followup.send(
        f"👻 **Most absent members:**\n" + "\n".join(lines),
        ephemeral=is_ephemeral,
        allowed_mentions=discord.AllowedMentions.none()
    )


@tree.command(
    name="voice_channel_stats",
    description="See which voice channels get the most traffic — total hours and unique users.",
    guild=GUILD_OBJ
)
@app_commands.describe(
    days="How many days back to include (default 7; >7 requires admin)",
    private="Post privately — only works if you're the designated private user (default: false)"
)
async def voice_channel_stats(
    inter: discord.Interaction,
    days: app_commands.Range[int, 1, 3650] = 7,
    private: bool = False
):
    if days > 7 and not (inter.user.guild_permissions.administrator or inter.user.guild_permissions.manage_guild):
        await inter.response.send_message("⛔ Only admins can request more than 7 days.", ephemeral=True)
        return

    is_ephemeral = (private and inter.user.id == VOICE_TOP_PRIVATE_USER)
    await inter.response.defer(thinking=True, ephemeral=is_ephemeral)

    since = now_ts() - days * 86400
    now = now_ts()

    async with aiosqlite.connect(DB_PATH) as cx:
        async with cx.execute(
            """
            SELECT channel_id,
                   SUM(MIN(COALESCE(left_ts, ?), ?) - MAX(joined_ts, ?)) AS total_secs,
                   COUNT(DISTINCT user_id) AS unique_users
            FROM voice_sessions
            WHERE joined_ts < ? AND COALESCE(left_ts, ?) > ?
            GROUP BY channel_id
            ORDER BY total_secs DESC
            """,
            (now, now, since, now, now, since)
        ) as cur:
            rows = await cur.fetchall()

    if not rows:
        await inter.followup.send("No voice activity in that window.", ephemeral=is_ephemeral)
        return

    lines = []
    for ch_id, total_secs, unique_users in rows:
        if AFK_CHANNEL_ID and ch_id == AFK_CHANNEL_ID:
            continue
        channel = inter.guild.get_channel(ch_id)
        ch_name = channel.name if channel else f"Channel {ch_id}"
        lines.append(
            f"🔊 **{ch_name}** — {fmt_duration(total_secs)} · {unique_users} unique user{'s' if unique_users != 1 else ''}"
        )

    if not lines:
        await inter.followup.send("No non-AFK voice activity in that window.", ephemeral=is_ephemeral)
        return

    await inter.followup.send(
        f"📡 **Channel stats (last {days}d):**\n" + "\n".join(lines),
        ephemeral=is_ephemeral
    )


@tree.command(
    name="voice_night_owl",
    description="Leaderboard of who racks up the most voice time during late-night hours.",
    guild=GUILD_OBJ
)
@app_commands.describe(
    days="How many days back to include (default 7; >7 requires admin)",
    start_hour="Start of the night window in 24h local time (default 23)",
    end_hour="End of the night window in 24h local time (default 4) — wraps midnight",
    private="Post privately — only works if you're the designated private user (default: false)"
)
async def voice_night_owl(
    inter: discord.Interaction,
    days: app_commands.Range[int, 1, 3650] = 7,
    start_hour: app_commands.Range[int, 0, 23] = 23,
    end_hour: app_commands.Range[int, 0, 23] = 4,
    private: bool = False
):
    if days > 7 and not (inter.user.guild_permissions.administrator or inter.user.guild_permissions.manage_guild):
        await inter.response.send_message("⛔ Only admins can request more than 7 days.", ephemeral=True)
        return

    is_ephemeral = (private and inter.user.id == VOICE_TOP_PRIVATE_USER)
    await inter.response.defer(thinking=True, ephemeral=is_ephemeral)

    since = now_ts() - days * 86400
    now = now_ts()

    rows = await fetch_sessions_window(since)  # (user_id, channel_id, joined_ts, left_ts)
    night_secs = aggregate_night_seconds_per_user(
        rows, since, now, TZ_NAME, AFK_CHANNEL_ID or None, start_hour, end_hour
    )

    if not night_secs:
        await inter.followup.send("No night-time voice activity in that window.", ephemeral=is_ephemeral)
        return

    top = sorted(night_secs.items(), key=lambda x: x[1], reverse=True)[:10]
    medals = ["🥇", "🥈", "🥉"] + [f"{i}." for i in range(4, 11)]

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
        name = escape_markdown(m.display_name)
        member_cache[uid] = name
        return name

    # Format window label e.g. "23:00–04:00"
    window_label = f"{start_hour:02d}:00–{end_hour:02d}:00"

    lines = []
    for i, (uid, secs) in enumerate(top):
        name = await label_for(uid)
        lines.append(f"{medals[i]} **{name}** — {fmt_duration(secs)}")

    await inter.followup.send(
        f"🦉 **Night owl leaderboard ({window_label}, last {days}d):**\n" + "\n".join(lines),
        ephemeral=is_ephemeral,
        allowed_mentions=discord.AllowedMentions.none()
    )


@tree.command(
    name="voice_report",
    description="Show YOUR voice time in the last X days (default 7).",
    guild=GUILD_OBJ
)
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
    await inter.response.send_message(
        f"🎧 {inter.user.mention}: last {days}d **{fmt_duration(total)}**", ephemeral=True
    )


@tree.command(
    name="voice_weekdays",
    description="Anonymized total voice time by weekday (Mon–Sun), server-wide.",
    guild=GUILD_OBJ
)
@app_commands.describe(
    days="How many days back to include (default 30)",
    public="Post publicly (default: true)",
    private="Post privately — only works if you're the designated private user (default: false)"
)
async def voice_weekdays(inter: discord.Interaction,
                         days: app_commands.Range[int, 1, 3650] = 30,
                         public: bool = True,
                         private: bool = False):
    since = now_ts() - days * 86400
    is_ephemeral = (not public) or (private and inter.user.id == VOICE_TOP_PRIVATE_USER)
    await inter.response.defer(thinking=True, ephemeral=is_ephemeral)

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
    labels = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    values_hours = [s / 3600.0 for s in buckets]

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

    await inter.followup.send(
        content=f"Anonymized server-wide weekday breakdown for last **{days}d**.",
        file=discord.File(buf, filename="voice_weekdays.png"),
        ephemeral=is_ephemeral
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
async def voice_solo(inter: discord.Interaction,
                     days: app_commands.Range[int, 1, 3650] = 7,
                     private: bool = False):
    if days > 7 and not (inter.user.guild_permissions.administrator or inter.user.guild_permissions.manage_guild):
        await inter.response.send_message("⛔ Only admins can request more than 7 days.", ephemeral=True)
        return

    is_ephemeral = (private and inter.user.id == VOICE_TOP_PRIVATE_USER)
    await inter.response.defer(thinking=True, ephemeral=is_ephemeral)

    since = now_ts() - days * 86400
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

    solo_totals = solo_seconds_per_user(rows, since, TZ_NAME, AFK_CHANNEL_ID or None)

    if not solo_totals:
        await inter.followup.send("No solo voice time recorded in that window.", ephemeral=is_ephemeral)
        return

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

    top = sorted(solo_totals.items(), key=lambda kv: kv[1], reverse=True)[:50]
    lines = []
    for i, (uid, seconds) in enumerate(top, start=1):
        name = await label_for(uid)
        lines.append(f"{i}. {name} — {fmt_duration(seconds)}")

    text = (
        f"**Top 50 solo voice time (last {days}d)**"
        f"{' (AFK excluded)' if AFK_CHANNEL_ID else ''}:\n" + "\n".join(lines)
    )
    await inter.followup.send(text, ephemeral=is_ephemeral, allowed_mentions=discord.AllowedMentions.none())


@tree.command(
    name="voice_peak",
    description="Anonymized peak concurrent users (overall + per-day chart).",
    guild=GUILD_OBJ
)
@app_commands.describe(
    days="How many days back (default 7; >7 requires admin)",
    public="Post publicly (default: true)",
    private="Post privately — only works if you're the designated private user (default: false)"
)
async def voice_peak(inter: discord.Interaction,
                     days: app_commands.Range[int, 1, 3650] = 7,
                     public: bool = True,
                     private: bool = False):
    if days > 7 and not (inter.user.guild_permissions.administrator or inter.user.guild_permissions.manage_guild):
        await inter.response.send_message("⛔ Only admins can request more than 7 days.", ephemeral=True)
        return

    is_ephemeral = (not public) or (private and inter.user.id == VOICE_TOP_PRIVATE_USER)
    since = now_ts() - days * 86400
    await inter.response.defer(thinking=True, ephemeral=is_ephemeral)

    rows = await fetch_sessions_window(since)
    overall_peak, per_day_peak = peak_concurrency(rows, since, TZ_NAME, AFK_CHANNEL_ID or None)

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
    plt.savefig(buf, format="png", dpi=150)
    plt.close()
    buf.seek(0)

    await inter.followup.send(
        content=f"**Overall peak** in last {days}d: **{overall_peak}** users.",
        file=discord.File(buf, "voice_peak.png"),
        ephemeral=is_ephemeral
    )


@tree.command(
    name="voice_daily_unique",
    description="Anonymized unique participants per day (last N days).",
    guild=GUILD_OBJ
)
@app_commands.describe(
    days="How many days back (default 7; >7 requires admin)",
    public="Post publicly (default: true)",
    private="Post privately — only works if you're the designated private user (default: false)"
)
async def voice_daily_unique(inter: discord.Interaction,
                             days: app_commands.Range[int, 1, 3650] = 7,
                             public: bool = True,
                             private: bool = False):
    if days > 7 and not (inter.user.guild_permissions.administrator or inter.user.guild_permissions.manage_guild):
        await inter.response.send_message("⛔ Only admins can request more than 7 days.", ephemeral=True)
        return

    is_ephemeral = (not public) or (private and inter.user.id == VOICE_TOP_PRIVATE_USER)
    since = now_ts() - days * 86400
    await inter.response.defer(thinking=True, ephemeral=is_ephemeral)

    rows = await fetch_sessions_window(since)
    day_users = aggregate_unique_users_by_day(rows, since, TZ_NAME, AFK_CHANNEL_ID or None)

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
    plt.savefig(buf, format="png", dpi=150)
    plt.close()
    buf.seek(0)

    await inter.followup.send(
        file=discord.File(buf, "voice_daily_unique.png"),
        content=f"Unique participants per day (last **{days}d**).",
        ephemeral=is_ephemeral
    )


@tree.command(
    name="voice_heatmap",
    description="Anonymized activity by hour of day (server-wide).",
    guild=GUILD_OBJ
)
@app_commands.describe(
    days="How many days back to include (default 7)",
    public="Post publicly (default: true)",
    private="Post privately — only works if you're the designated private user (default: false)"
)
async def voice_heatmap(inter: discord.Interaction,
                        days: app_commands.Range[int, 1, 3650] = 7,
                        public: bool = True,
                        private: bool = False):
    since = now_ts() - days * 86400
    is_ephemeral = (not public) or (private and inter.user.id == VOICE_TOP_PRIVATE_USER)
    await inter.response.defer(thinking=True, ephemeral=is_ephemeral)

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

    buckets = aggregate_seconds_by_hour(rows, since, now_ts(), TZ_NAME, AFK_CHANNEL_ID or None)
    hours = list(range(24))
    values_hours = [s / 3600.0 for s in buckets]

    plt.figure(figsize=PLOT_SIZE)
    plt.bar(hours, values_hours)
    plt.xticks(hours, [f"{h:02d}" for h in hours])
    subtitle = "(AFK excluded)" if AFK_CHANNEL_ID else ""
    plt.title(f"Voice activity by hour (last {days}d) {subtitle}")
    plt.ylabel("Total hours")
    plt.xlabel("Hour of day")
    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=150)
    plt.close()
    buf.seek(0)

    await inter.followup.send(
        content=f"Anonymized server-wide heatmap for last **{days}d**.",
        file=discord.File(buf, filename="voice_heatmap.png"),
        ephemeral=is_ephemeral
    )


@tree.command(
    name="voice_daily",
    description="Anonymized total server voice hours per day (trend).",
    guild=GUILD_OBJ
)
@app_commands.describe(
    days="How many days back (default 7; >7 requires admin)",
    public="Post publicly (default: true)",
    private="Post privately — only works if you're the designated private user (default: false)"
)
async def voice_daily(inter: discord.Interaction,
                      days: app_commands.Range[int, 1, 3650] = 7,
                      public: bool = True,
                      private: bool = False):
    if days > 7:
        perms = inter.user.guild_permissions
        if not (perms.administrator or perms.manage_guild):
            await inter.response.send_message("⛔ Only admins can request more than 7 days.", ephemeral=True)
            return

    is_ephemeral = (not public) or (private and inter.user.id == VOICE_TOP_PRIVATE_USER)
    since = now_ts() - days * 86400
    await inter.response.defer(thinking=True, ephemeral=is_ephemeral)

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

    buckets = aggregate_seconds_by_day(rows, since, now_ts(), TZ_NAME, AFK_CHANNEL_ID or None)

    try:
        import zoneinfo
        tz = zoneinfo.ZoneInfo(TZ_NAME)
    except Exception:
        tz = timezone.utc

    base = datetime.fromtimestamp(since, tz=tz).replace(hour=0, minute=0, second=0, microsecond=0)
    days_list = [(base + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days)]
    values_hours = [(buckets.get(day, 0) / 3600.0) for day in days_list]

    plt.figure(figsize=PLOT_SIZE)
    x = list(range(len(days_list)))
    plt.bar(x, values_hours)
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

    await inter.followup.send(
        content=f"Anonymized server-wide daily totals for last **{days}d**.",
        file=discord.File(buf, filename="voice_daily.png"),
        ephemeral=is_ephemeral
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
    await inter.response.send_message(
        f"📊 {inter.user.mention}: lifetime **{fmt_duration(total)}**", ephemeral=True
    )


@tree.command(name="voice_current", description="List users currently in voice channels.", guild=GUILD_OBJ)
@app_commands.describe(
    private="Post privately — only works if you're the designated private user (default: false)"
)
async def voice_current(inter: discord.Interaction, private: bool = False):
    is_ephemeral = (private and inter.user.id == VOICE_TOP_PRIVATE_USER)
    lines = []
    for vc in inter.guild.voice_channels:
        if vc.members:
            names = ", ".join(m.display_name for m in vc.members)
            lines.append(f"🔊 **{vc.name}**: {names}")
    msg = "\n".join(lines) if lines else "No one is in voice right now."
    await inter.response.send_message(msg, ephemeral=is_ephemeral)


@tree.command(
    name="voice_top",
    description="Leaderboard of top 50 voice users in the last N days",
    guild=GUILD_OBJ
)
@app_commands.describe(
    days="How many days back (default 7)",
    private="Only available to special user; defaults to false"
)
async def voice_top(inter: discord.Interaction,
                    days: app_commands.Range[int, 1, 3650] = 7,
                    private: bool = False):
    is_ephemeral = (private and inter.user.id == VOICE_TOP_PRIVATE_USER)
    await inter.response.defer(thinking=True, ephemeral=is_ephemeral)

    since = now_ts() - days * 86400
    extra, params = afk_filter_clause()

    # FIX: overlapping-window WHERE so sessions that started before 'since'
    # but were still active during the window are included
    async with aiosqlite.connect(DB_PATH) as cx:
        async with cx.execute(f"""
            SELECT user_id,
                   SUM(COALESCE(left_ts, strftime('%s','now')) - joined_ts) AS total
            FROM voice_sessions
            WHERE joined_ts < strftime('%s','now')
              AND COALESCE(left_ts, strftime('%s','now')) > ?
              {extra}
            GROUP BY user_id
            ORDER BY total DESC
            LIMIT 50
        """, [since] + params) as cur:
            rows = await cur.fetchall()

    if not rows:
        await inter.followup.send("No voice activity in that window.", ephemeral=is_ephemeral)
        return

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
    await inter.followup.send(text, ephemeral=is_ephemeral, allowed_mentions=discord.AllowedMentions.none())


@tree.command(
    name="voice_streak_board",
    description="Leaderboard of current daily voice streaks with a bar chart.",
    guild=GUILD_OBJ
)
@app_commands.describe(
    private="Post privately — only works if you're the designated private user (default: false)"
)
async def voice_streak_board(inter: discord.Interaction, private: bool = False):
    is_ephemeral = (private and inter.user.id == VOICE_TOP_PRIVATE_USER)
    await inter.response.defer(thinking=True, ephemeral=is_ephemeral)

    async with aiosqlite.connect(DB_PATH) as cx:
        async with cx.execute("SELECT DISTINCT user_id FROM voice_sessions") as cur:
            user_rows = await cur.fetchall()

    streak_data = []
    for (uid,) in user_rows:
        if inter.guild.get_member(uid) is None:
            continue
        streak = await compute_streak(uid)
        if streak > 0:
            streak_data.append((uid, streak))

    if not streak_data:
        await inter.followup.send("No active streaks right now.", ephemeral=is_ephemeral)
        return

    streak_data.sort(key=lambda x: x[1], reverse=True)
    top = streak_data[:15]

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
        member_cache[uid] = escape_markdown(m.display_name)
        return member_cache[uid]

    names = []
    streaks = []
    for uid, streak in top:
        names.append(await label_for(uid))
        streaks.append(streak)

    max_streak = max(streaks)
    colors = [plt.cm.RdYlGn(s / max_streak) for s in streaks]

    plt.figure(figsize=PLOT_SIZE)
    bars = plt.bar(range(len(names)), streaks, color=colors)
    plt.xticks(range(len(names)), names, rotation=30, ha="right")
    plt.ylabel("Streak (days)")
    plt.title("Current daily voice streaks 🔥")
    for bar, s in zip(bars, streaks):
        plt.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.1,
                 str(s), ha="center", va="bottom", fontsize=9)
    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=150)
    plt.close()
    buf.seek(0)

    medals = ["🥇", "🥈", "🥉"] + [f"{i}." for i in range(4, 16)]
    lines = [
        f"{medals[i]} **{names[i]}** — {streaks[i]} day{'s' if streaks[i] != 1 else ''} 🔥"
        for i in range(len(names))
    ]
    await inter.followup.send(
        content="🔥 **Voice streak leaderboard:**\n" + "\n".join(lines),
        file=discord.File(buf, "voice_streaks.png"),
        ephemeral=is_ephemeral,
        allowed_mentions=discord.AllowedMentions.none()
    )


@tree.command(
    name="voice_my_chart",
    description="Your personal daily voice activity chart for the last N days (always private).",
    guild=GUILD_OBJ
)
@app_commands.describe(days="How many days back to chart (default 14)")
async def voice_my_chart(inter: discord.Interaction, days: app_commands.Range[int, 1, 365] = 14):
    await inter.response.defer(thinking=True, ephemeral=True)

    since = now_ts() - days * 86400
    uid = inter.user.id
    extra, params = afk_filter_clause()

    async with aiosqlite.connect(DB_PATH) as cx:
        async with cx.execute(
            f"SELECT joined_ts, left_ts, channel_id FROM voice_sessions "
            f"WHERE user_id=? AND joined_ts < ? AND COALESCE(left_ts, strftime('%s','now')) > ?{extra}",
            [uid, now_ts(), since] + params
        ) as cur:
            rows = await cur.fetchall()

    buckets = aggregate_seconds_by_day(rows, since, now_ts(), TZ_NAME, AFK_CHANNEL_ID or None)

    try:
        import zoneinfo
        tz = zoneinfo.ZoneInfo(TZ_NAME)
    except Exception:
        tz = timezone.utc

    base = datetime.fromtimestamp(since, tz=tz).replace(hour=0, minute=0, second=0, microsecond=0)
    days_list = [(base + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days)]
    values_hours = [buckets.get(d, 0) / 3600.0 for d in days_list]

    plt.figure(figsize=PLOT_SIZE)
    x = list(range(len(days_list)))
    plt.fill_between(x, values_hours, alpha=0.35)
    plt.plot(x, values_hours, marker="o", markersize=4, linewidth=1.5)
    step = max(1, len(x) // 14)
    plt.xticks(x[::step], days_list[::step], rotation=45, ha="right")
    subtitle = " (AFK excluded)" if AFK_CHANNEL_ID else ""
    plt.title(f"{escape_markdown(inter.user.display_name)} — daily voice (last {days}d){subtitle}")
    plt.ylabel("Hours")
    plt.xlabel("Day")
    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=150)
    plt.close()
    buf.seek(0)

    total_h = sum(values_hours)
    avg_h = total_h / days if days else 0
    await inter.followup.send(
        content=(
            f"📈 **Your voice activity (last {days}d)** — "
            f"total: **{fmt_duration(int(total_h * 3600))}**, avg: **{avg_h:.1f}h/day**"
        ),
        file=discord.File(buf, "voice_my_chart.png"),
        ephemeral=True
    )


@tree.command(
    name="voice_growth",
    description="Cumulative server voice hours over the last N days — see if the community is growing.",
    guild=GUILD_OBJ
)
@app_commands.describe(
    days="How many days back (default 30; >30 requires admin)",
    public="Post publicly (default: true)",
    private="Post privately — only works if you're the designated private user (default: false)"
)
async def voice_growth(
    inter: discord.Interaction,
    days: app_commands.Range[int, 2, 3650] = 30,
    public: bool = True,
    private: bool = False
):
    if days > 30 and not (inter.user.guild_permissions.administrator or inter.user.guild_permissions.manage_guild):
        await inter.response.send_message("⛔ Only admins can request more than 30 days.", ephemeral=True)
        return

    is_ephemeral = (not public) or (private and inter.user.id == VOICE_TOP_PRIVATE_USER)
    await inter.response.defer(thinking=True, ephemeral=is_ephemeral)

    since = now_ts() - days * 86400

    async with aiosqlite.connect(DB_PATH) as cx:
        async with cx.execute(
            "SELECT joined_ts, left_ts, channel_id FROM voice_sessions "
            "WHERE joined_ts < ? AND COALESCE(left_ts, strftime('%s','now')) > ?",
            (now_ts(), since)
        ) as cur:
            rows = await cur.fetchall()

    daily = aggregate_seconds_by_day(rows, since, now_ts(), TZ_NAME, AFK_CHANNEL_ID or None)

    try:
        import zoneinfo
        tz = zoneinfo.ZoneInfo(TZ_NAME)
    except Exception:
        tz = timezone.utc

    base = datetime.fromtimestamp(since, tz=tz).replace(hour=0, minute=0, second=0, microsecond=0)
    days_list = [(base + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days)]

    cumulative = []
    running = 0.0
    for d in days_list:
        running += daily.get(d, 0) / 3600.0
        cumulative.append(running)

    step = max(1, len(days_list) // 10)
    plt.figure(figsize=PLOT_SIZE)
    x = list(range(len(days_list)))
    plt.fill_between(x, cumulative, alpha=0.25)
    plt.plot(x, cumulative, linewidth=2)
    plt.xticks(x[::step], days_list[::step], rotation=45, ha="right")
    subtitle = " (AFK excluded)" if AFK_CHANNEL_ID else ""
    plt.title(f"Cumulative voice hours (last {days}d){subtitle}")
    plt.ylabel("Cumulative hours")
    plt.xlabel("Day")
    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=150)
    plt.close()
    buf.seek(0)

    total_h = cumulative[-1] if cumulative else 0.0
    await inter.followup.send(
        content=(
            f"📈 **Cumulative voice hours (last {days}d):** **{total_h:.1f}h** total"
            f"{' (AFK excluded)' if AFK_CHANNEL_ID else ''}"
        ),
        file=discord.File(buf, "voice_growth.png"),
        ephemeral=is_ephemeral
    )


@tree.command(
    name="voice_milestones",
    description="Recent voice time milestone awards — see who crossed a big threshold.",
    guild=GUILD_OBJ
)
@app_commands.describe(
    limit="How many recent milestones to show (default 10)",
    private="Post privately — only works if you're the designated private user (default: false)"
)
async def voice_milestones(
    inter: discord.Interaction,
    limit: app_commands.Range[int, 1, 50] = 10,
    private: bool = False
):
    is_ephemeral = (private and inter.user.id == VOICE_TOP_PRIVATE_USER)
    await inter.response.defer(thinking=True, ephemeral=is_ephemeral)

    async with aiosqlite.connect(DB_PATH) as cx:
        async with cx.execute(
            "SELECT user_id, hours, awarded_ts FROM milestones ORDER BY awarded_ts DESC LIMIT ?",
            (limit,)
        ) as cur:
            rows = await cur.fetchall()

    if not rows:
        await inter.followup.send("No milestones have been awarded yet.", ephemeral=is_ephemeral)
        return

    lines = []
    for uid, hours, awarded_ts in rows:
        m = inter.guild.get_member(uid)
        name = escape_markdown(m.display_name if m else f"User {uid}")
        date_str = ts_to_local(awarded_ts)
        lines.append(f"🏆 **{name}** hit **{hours}h** — `{date_str}`")

    await inter.followup.send(
        "🏆 **Recent voice milestones:**\n" + "\n".join(lines),
        ephemeral=is_ephemeral,
        allowed_mentions=discord.AllowedMentions.none()
    )


@tree.command(
    name="voice_session_count",
    description="Voice sessions started per day — shows how often members join, not just how long.",
    guild=GUILD_OBJ
)
@app_commands.describe(
    days="How many days back (default 14; >14 requires admin)",
    public="Post publicly (default: true)",
    private="Post privately — only works if you're the designated private user (default: false)"
)
async def voice_session_count(
    inter: discord.Interaction,
    days: app_commands.Range[int, 1, 3650] = 14,
    public: bool = True,
    private: bool = False
):
    if days > 14 and not (inter.user.guild_permissions.administrator or inter.user.guild_permissions.manage_guild):
        await inter.response.send_message("⛔ Only admins can request more than 14 days.", ephemeral=True)
        return

    is_ephemeral = (not public) or (private and inter.user.id == VOICE_TOP_PRIVATE_USER)
    await inter.response.defer(thinking=True, ephemeral=is_ephemeral)

    since = now_ts() - days * 86400
    afk_cond = " AND channel_id != ?" if AFK_CHANNEL_ID else ""
    afk_params = [AFK_CHANNEL_ID] if AFK_CHANNEL_ID else []

    async with aiosqlite.connect(DB_PATH) as cx:
        async with cx.execute(
            f"SELECT joined_ts FROM voice_sessions WHERE joined_ts >= ?{afk_cond}",
            [since] + afk_params
        ) as cur:
            rows = await cur.fetchall()

    try:
        import zoneinfo
        tz = zoneinfo.ZoneInfo(TZ_NAME)
    except Exception:
        tz = timezone.utc

    counts: dict[str, int] = {}
    for (joined_ts,) in rows:
        day_key = datetime.fromtimestamp(joined_ts, tz=tz).strftime("%Y-%m-%d")
        counts[day_key] = counts.get(day_key, 0) + 1

    base = datetime.fromtimestamp(since, tz=tz).replace(hour=0, minute=0, second=0, microsecond=0)
    days_list = [(base + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days)]
    values = [counts.get(d, 0) for d in days_list]

    plt.figure(figsize=PLOT_SIZE)
    x = list(range(len(days_list)))
    plt.bar(x, values)
    plt.xticks(x, days_list, rotation=45, ha="right")
    subtitle = " (AFK excluded)" if AFK_CHANNEL_ID else ""
    plt.title(f"Voice sessions started per day (last {days}d){subtitle}")
    plt.ylabel("Sessions")
    plt.xlabel("Day")
    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=150)
    plt.close()
    buf.seek(0)

    total_s = sum(values)
    avg_s = total_s / days if days else 0
    await inter.followup.send(
        content=(
            f"📊 **Sessions per day (last {days}d):** "
            f"**{total_s}** total, avg **{avg_s:.1f}/day**"
            f"{' (AFK excluded)' if AFK_CHANNEL_ID else ''}"
        ),
        file=discord.File(buf, "voice_session_count.png"),
        ephemeral=is_ephemeral
    )


@tree.command(
    name="voice_help",
    description="Show what every voice command does.",
    guild=GUILD_OBJ
)
async def voice_help(inter: discord.Interaction):
    await inter.response.defer(thinking=True, ephemeral=True)

    embed = discord.Embed(
        title="🎙️  Voice Bot — Command Reference",
        description=(
            "All commands are restricted to designated channels.\n"
            "The `private` flag (where shown) lets the special user see output only to themselves.\n"
            "Commands marked 📊/📈 include a chart image."
        ),
        color=discord.Color.blurple()
    )

    personal = (
        "`/voice_me` — Full personal dashboard: lifetime, 7d/30d, rank, streak, top partners.\n"
        "`/voice_total` — Your lifetime voice total.\n"
        "`/voice_report [days]` — Your voice time in the last N days.\n"
        "`/voice_history [private]` — Your last 10 sessions with timestamps.\n"
        "`/voice_my_chart [days]` — 📈 Your personal daily activity chart (always private)."
    )
    embed.add_field(name="👤 Personal", value=personal, inline=False)

    server_stats = (
        "`/voice_top [days] [private]` — Top 50 users by voice time.\n"
        "`/voice_streak_board [private]` — 📊 Current daily voice streaks leaderboard.\n"
        "`/voice_solo [days] [private]` — Top users by time spent alone in voice.\n"
        "`/voice_night_owl [days] [start_hour] [end_hour] [private]` — Late-night voice leaderboard.\n"
        "`/voice_marathon [days] [private]` — Top 10 longest single sessions.\n"
        "`/voice_ghost [private]` — Members absent from voice the longest.\n"
        "`/voice_milestones [limit] [private]` — Recent milestone awards (1h, 5h, 10h, 25h…)."
    )
    embed.add_field(name="🏆 Server Stats", value=server_stats, inline=False)

    charts = (
        "`/voice_heatmap [days] [public] [private]` — 📊 Activity by hour of day.\n"
        "`/voice_weekdays [days] [public] [private]` — 📊 Activity by weekday (Mon–Sun).\n"
        "`/voice_daily [days] [public] [private]` — 📊 Total server hours per day.\n"
        "`/voice_daily_unique [days] [public] [private]` — 📊 Unique participants per day.\n"
        "`/voice_peak [days] [public] [private]` — 📊 Peak concurrent users per day.\n"
        "`/voice_growth [days] [public] [private]` — 📈 Cumulative voice hours over time.\n"
        "`/voice_session_count [days] [public] [private]` — 📊 Sessions started per day."
    )
    embed.add_field(name="📈 Charts & Trends", value=charts, inline=False)

    social = (
        "`/voice_bestfriends [days] [private]` — Top pairs by shared voice time.\n"
        "`/voice_rivalry <opponent> [days] [private]` — Head-to-head stats vs another member.\n"
        "`/voice_channel_stats [days] [private]` — Which channels get the most traffic.\n"
        "`/voice_current [private]` — Who's in voice right now."
    )
    embed.add_field(name="🤝 Social", value=social, inline=False)

    admin_cmds = (
        "`/voice_together <user1> <user2> [private]` — [Admin] Time two members spent in voice together.\n"
        "`/voice_recap [month]` — [Admin] Manually post the monthly voice recap."
    )
    embed.add_field(name="🔒 Admin Only", value=admin_cmds, inline=False)

    embed.add_field(
        name="⚙️ Special User",
        value="`/pi_storage [path]` — Disk usage on the Pi.",
        inline=False
    )

    embed.set_footer(text="Chart commands default to public. Use private=True (special user) or public=False to post only to yourself.")
    await inter.followup.send(embed=embed, ephemeral=True)


# -------- Run --------
if __name__ == "__main__":
    if not TOKEN:
        raise SystemExit("DISCORD_TOKEN missing in .env")
    client.run(TOKEN)