"""
media_utils.py — Image Processing and AVIF Thumbnail Generation
"""
import os
import io
import re
import hashlib
import logging
import asyncio
import httpx
from PIL import Image
import pillow_avif  # registers AVIF codec with Pillow

from database import get_db

logger = logging.getLogger("shomaj.media_utils")

def get_youtube_thumbnail_url(video_url: str) -> str:
    """
    Extracts the YouTube video ID from a URL and returns its standard thumbnail image URL.
    Returns empty string if not a YouTube URL.
    """
    yt_match = re.search(r'(?:youtube\.com/(?:embed/|watch\?v=)|youtu\.be/)([^?&/]+)', video_url)
    if yt_match:
        video_id = yt_match.group(1)
        return f"https://img.youtube.com/vi/{video_id}/0.jpg"
    return ""

async def get_local_avif_thumbnail(url: str) -> str:
    """
    Downloads, resizes, and saves an image to a local AVIF file.
    Returns the local served URL path, or the original URL on failure.
    """
    if not url or not url.startswith(("http://", "https://")):
        return url
    try:
        h = hashlib.md5(url.encode("utf-8")).hexdigest()
        os.makedirs("static/thumbnails", exist_ok=True)
        local_path = os.path.join("static/thumbnails", f"{h}.avif")
        local_url = f"/static/thumbnails/{h}.avif"

        if os.path.exists(local_path):
            return local_url

        async with httpx.AsyncClient(follow_redirects=True, timeout=10.0) as client:
            resp = await client.get(url)
            if resp.status_code != 200:
                return url
            img_data = resp.content

        def _process():
            with Image.open(io.BytesIO(img_data)) as img:
                if img.mode not in ("RGB", "L"):
                    img = img.convert("RGB")
                img.thumbnail((200, 200))
                # speed=6 is fast encoding, quality=50 is standard compression quality
                img.save(local_path, "AVIF", speed=6, quality=50)

        await asyncio.to_thread(_process)
        return local_url
    except Exception as exc:
        logger.debug("Failed to create AVIF thumbnail for %s: %s", url, exc)
        return url


async def process_media_thumbnail_bg(media_url: str, src_image_url: str):
    """
    Background worker task to generate AVIF thumbnail and update the database.
    """
    if not src_image_url:
        # Check if we can extract YouTube thumbnail
        src_image_url = get_youtube_thumbnail_url(media_url)
        if not src_image_url:
            return

    # Wait a bit to let the main transaction commit
    await asyncio.sleep(0.5)
    local_url = await get_local_avif_thumbnail(src_image_url)
    if local_url != src_image_url:
        conn = get_db()
        try:
            conn.execute(
                "UPDATE media_index SET thumbnail_url = ? WHERE media_url = ?",
                (local_url, media_url)
            )
            conn.commit()
            logger.info("Successfully updated thumbnail to AVIF: %s -> %s", media_url, local_url)
        except Exception as e:
            logger.warning("Failed to save background thumbnail update: %s", e)
