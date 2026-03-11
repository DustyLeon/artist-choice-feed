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
from datetime import time as dtime

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

def search_youtube(artist: str, yt) -> str | None:
    """Search YouTube for official audio/video. Returns a URL or None."""
    for template in YT_QUERIES:
        query = template.format(artist=artist)
        try:
            resp = yt.search().list(
                q=query,
                part='snippet',
                type='video',
                videoCategoryId='10',   # Music category
                maxResults=3,
            ).execute()

            items = resp.get('items', [])
            if not items:
                continue

            # Prefer results whose channel title contains the artist name
            # (catches VEVO channels and official artist channels)
            artist_lower = artist.lower()
            for item in items:
                channel = item['snippet']['channelTitle'].lower()
                if artist_lower in channel or 'vevo' in channel or 'official' in channel:
                    vid_id = item['id']['videoId']
                    log.info(f'YT match (channel heuristic): {item["snippet"]["channelTitle"]}')
                    return f'https://www.youtube.com/watch?v={vid_id}'

            # Fall back to the first result if heuristic finds nothing
            vid_id = items[0]['id']['videoId']
            log.info(f'YT fallback result for "{query}"')
            return f'https://www.youtube.com/watch?v={vid_id}'

        except Exception as e:
            log.warning(f'YouTube search error for "{query}": {e}')

    return None

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

        artist = random.choice(rows)['artist_name']
        log.info(f'Selected artist: {artist}')

        url = search_youtube(artist, self.yt)
        if not url:
            log.warning(f'No YouTube result found for "{artist}"')
            return None

        channel = self.get_channel(FEED_CHANNEL_ID)
        if not channel:
            log.error(f'Could not find channel {FEED_CHANNEL_ID}')
            return None

        await channel.send(f'🎵 **{artist}**\n{url}')
        log.info(f'Posted: {artist} — {url}')
        return artist

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
