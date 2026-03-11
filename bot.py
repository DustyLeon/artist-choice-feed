"""
community-artist-choice-feed bot
---------------------------------
Slash commands:
  /setlastfm <username>  — link your Last.fm account
  /add <artist>          — manually add an artist to the pool
  /random                — trigger an extra post right now

Scheduled jobs:
  Daily  @ 06:00 UTC — refresh artist pool from Last.fm + manual list
  12-hourly           — post a YouTube link from a random pool artist
"""

import os
import random
import sqlite3
import logging
import time
from datetime import time as dtime

DUPLICATE_WINDOW_DAYS = 30

import discord
from discord import app_commands
from discord.ext import tasks
import pylast
from googleapiclient.discovery import build

# ── Logging ────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
log = logging.getLogger(__name__)

# ── Config (set these as Railway environment variables) ────────────────────────

DISCORD_TOKEN   = os.environ['DISCORD_TOKEN']
LASTFM_API_KEY  = os.environ['LASTFM_API_KEY']
LASTFM_SECRET   = os.environ.get('LASTFM_SECRET', '')
YT_API_KEY      = os.environ['YOUTUBE_API_KEY']
FEED_CHANNEL_ID = int(os.environ['FEED_CHANNEL_ID'])

# Railway persistent volume should be mounted at /data
# Set DB_PATH env var if you want a different location
DB_PATH = os.environ.get('DB_PATH', '/data/artists.db')

# ── Database helpers ───────────────────────────────────────────────────────────

def open_db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    return conn

def init_db():
    conn = open_db()
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS lastfm_users (
            discord_id      TEXT PRIMARY KEY,
            lastfm_username TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS manual_artists (
            artist_name TEXT PRIMARY KEY COLLATE NOCASE
        );
        CREATE TABLE IF NOT EXISTS artist_pool (
            artist_name TEXT PRIMARY KEY COLLATE NOCASE
        );
        CREATE TABLE IF NOT EXISTS posted_videos (
            video_id    TEXT PRIMARY KEY,
            posted_at   INTEGER NOT NULL  -- Unix timestamp
        );
    ''')
    conn.commit()
    conn.close()
    log.info('Database initialised')

# ── Last.fm ────────────────────────────────────────────────────────────────────

def fetch_weekly_artists(username: str, network: pylast.LastFMNetwork) -> list[str]:
    """Return artist names from a user's rolling 7-day chart."""
    try:
        user = network.get_user(username)
        chart = user.get_weekly_artist_charts()
        return [entry.item.name for entry in chart]
    except Exception as e:
        log.warning(f'Last.fm fetch failed for "{username}": {e}')
        return []

def rebuild_pool(network: pylast.LastFMNetwork):
    """Pull fresh Last.fm data for all registered users and rebuild the pool."""
    conn = open_db()
    users   = conn.execute('SELECT lastfm_username FROM lastfm_users').fetchall()
    manual  = conn.execute('SELECT artist_name FROM manual_artists').fetchall()
    conn.close()

    artists: set[str] = {row['artist_name'] for row in manual}
    for row in users:
        weekly = fetch_weekly_artists(row['lastfm_username'], network)
        artists.update(weekly)
        log.info(f'  {row["lastfm_username"]}: {len(weekly)} artists')

    conn = open_db()
    conn.execute('DELETE FROM artist_pool')
    conn.executemany(
        'INSERT OR IGNORE INTO artist_pool (artist_name) VALUES (?)',
        [(a,) for a in artists if a]
    )
    conn.commit()
    conn.close()
    log.info(f'Pool rebuilt — {len(artists)} artists total')

# ── YouTube ────────────────────────────────────────────────────────────────────

# Search strategies in priority order
YT_QUERIES = [
    '{artist} official audio',
    '{artist} official video',
    '{artist} official',
    '{artist}',
]

def get_recent_video_ids() -> set[str]:
    """Return video IDs posted within the duplicate window."""
    cutoff = int(time.time()) - DUPLICATE_WINDOW_DAYS * 86400
    conn = open_db()
    rows = conn.execute(
        'SELECT video_id FROM posted_videos WHERE posted_at > ?', (cutoff,)
    ).fetchall()
    conn.close()
    return {row['video_id'] for row in rows}

def record_video(video_id: str):
    """Save a posted video ID with the current timestamp."""
    conn = open_db()
    conn.execute(
        'INSERT OR REPLACE INTO posted_videos (video_id, posted_at) VALUES (?, ?)',
        (video_id, int(time.time()))
    )
    # Clean up entries older than the window while we're here
    cutoff = int(time.time()) - DUPLICATE_WINDOW_DAYS * 86400
    conn.execute('DELETE FROM posted_videos WHERE posted_at <= ?', (cutoff,))
    conn.commit()
    conn.close()

def search_youtube(artist: str, yt, recent_ids: set[str]) -> tuple[str, str] | tuple[None, None]:
    """Search YouTube for official audio/video, skipping recently posted videos.
    Returns (video_id, url) or (None, None)."""
    for template in YT_QUERIES:
        query = template.format(artist=artist)
        try:
            resp = yt.search().list(
                q=query,
                part='snippet',
                type='video',
                videoCategoryId='10',
                maxResults=5,  # Increased to give more candidates for dedup
            ).execute()

            items = resp.get('items', [])
            if not items:
                continue

            artist_lower = artist.lower()

            # Separate preferred (official channel heuristic) from fallback
            preferred = []
            fallback = []
            for item in items:
                vid_id = item['id']['videoId']
                if vid_id in recent_ids:
                    log.info(f'Skipping recent duplicate: {vid_id}')
                    continue
                channel = item['snippet']['channelTitle'].lower()
                if artist_lower in channel or 'vevo' in channel or 'official' in channel:
                    preferred.append(vid_id)
                else:
                    fallback.append(vid_id)

            for vid_id in preferred + fallback:
                url = f'https://www.youtube.com/watch?v={vid_id}'
                log.info(f'YT selected: {vid_id} for "{artist}"')
                return vid_id, url

        except Exception as e:
            log.warning(f'YouTube search error for "{query}": {e}')

    return None, None

# ── Bot ────────────────────────────────────────────────────────────────────────

class ArtistFeedBot(discord.Client):
    def __init__(self):
        intents = discord.Intents.default()
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)
        self.lastfm = pylast.LastFMNetwork(
            api_key=LASTFM_API_KEY,
            api_secret=LASTFM_SECRET
        )
        self.yt = build('youtube', 'v3', developerKey=YT_API_KEY)

    async def setup_hook(self):
        init_db()
        await self.tree.sync()
        log.info('Slash commands synced')
        self.refresh_pool_job.start()
        self.post_job.start()

    async def on_ready(self):
        log.info(f'Ready — logged in as {self.user} (ID: {self.user.id})')

    async def post_to_feed(self):
        """Pick a random artist from the pool and post a YouTube link."""
        conn = open_db()
        rows = conn.execute('SELECT artist_name FROM artist_pool').fetchall()
        conn.close()

        if not rows:
            log.warning('Artist pool is empty — skipping post')
            return None

        recent_ids = get_recent_video_ids()
        candidates = list(rows)
        random.shuffle(candidates)

        # Try up to 5 artists before giving up
        for row in candidates[:5]:
            artist = row['artist_name']
            log.info(f'Trying artist: {artist}')
            vid_id, url = search_youtube(artist, self.yt, recent_ids)
            if not url:
                log.warning(f'No fresh YouTube result for "{artist}", trying next artist')
                continue

            channel = self.get_channel(FEED_CHANNEL_ID)
            if not channel:
                log.error(f'Could not find channel {FEED_CHANNEL_ID}')
                return None

            record_video(vid_id)
            await channel.send(f'🎵 **{artist}**\n{url}')
            log.info(f'Posted: {artist} — {url}')
            return artist

        log.warning('Could not find a fresh video after 5 attempts')
        return None

    # Refresh pool once a day at 06:00 UTC
    @tasks.loop(time=dtime(hour=6, minute=0))
    async def refresh_pool_job(self):
        await self.wait_until_ready()
        log.info('Running daily pool refresh...')
        rebuild_pool(self.lastfm)

    # Post every 12 hours
    @tasks.loop(hours=12)
    async def post_job(self):
        await self.wait_until_ready()
        await self.post_to_feed()


bot = ArtistFeedBot()

# ── Slash commands ─────────────────────────────────────────────────────────────

@bot.tree.command(
    name='setlastfm',
    description='Link your Last.fm account so your weekly artists feed the pool'
)
@app_commands.describe(username='Your Last.fm username')
async def setlastfm(interaction: discord.Interaction, username: str):
    conn = open_db()
    conn.execute(
        'INSERT OR REPLACE INTO lastfm_users (discord_id, lastfm_username) VALUES (?, ?)',
        (str(interaction.user.id), username)
    )
    conn.commit()
    conn.close()
    await interaction.response.send_message(
        f'✅ Linked **{username}** to your account. '
        'Your weekly artists will be included in tomorrow\'s pool refresh.',
        ephemeral=True
    )

@bot.tree.command(
    name='add',
    description='Add an artist to the community pool (for members without Last.fm)'
)
@app_commands.describe(artist='Artist name to add')
async def add_artist(interaction: discord.Interaction, artist: str):
    conn = open_db()
    conn.execute(
        'INSERT OR IGNORE INTO manual_artists (artist_name) VALUES (?)',
        (artist,)
    )
    conn.commit()

    # Also add directly to the live pool so it can be picked immediately
    conn.execute(
        'INSERT OR IGNORE INTO artist_pool (artist_name) VALUES (?)',
        (artist,)
    )
    conn.commit()
    conn.close()

    await interaction.response.send_message(
        f'✅ Added **{artist}** to the community pool.',
        ephemeral=True
    )

@bot.tree.command(
    name='random',
    description='Post a bonus track from the pool right now'
)
async def random_post(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    artist = await bot.post_to_feed()
    if artist:
        await interaction.followup.send(f'✅ Posted a track from **{artist}**!', ephemeral=True)
    else:
        await interaction.followup.send(
            '⚠️ Couldn\'t post — pool may be empty or YouTube search failed. Check bot logs.',
            ephemeral=True
        )

# ── Run ────────────────────────────────────────────────────────────────────────

bot.run(DISCORD_TOKEN)
