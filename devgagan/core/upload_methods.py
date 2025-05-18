import os
import time
from typing import Optional
from pyrogram.types import Message
from pyrogram.enums import ParseMode
from telethon.tl.types import DocumentAttributeVideo
from config import LOG_GROUP
from devgagan.core.func import *

async def pyrogram_download(
    client,
    msg,
    file_name: Optional[str] = None,
    progress_callback: Optional[callable] = None,
    progress_args: Optional[tuple] = None
) -> str:
    """Download media using Pyrogram"""
    file = await client.download_media(
        msg,
        file_name=file_name,
        progress=progress_callback,
        progress_args=progress_args
    )
    return file

async def telethon_download(
    client,
    msg,
    sender: int,
    progress_callback: Optional[callable] = None
) -> str:
    """Download media using Telethon"""
    progress_message = await client.send_message(sender, "**__Downloading...__**")
    file = await fast_download(
        client, msg,
        reply=progress_message,
        progress_bar_function=lambda done, total: progress_callback(done, total, sender)
    )
    await progress_message.delete()
    return file

async def pyrogram_upload(
    client,
    sender: int,
    target_chat_id: int,
    file: str,
    caption: Optional[str] = None,
    edit: Optional[Message] = None,
    topic_id: Optional[int] = None
) -> Message:
    """Upload media using Pyrogram"""
    metadata = video_metadata(file)
    width, height, duration = metadata['width'], metadata['height'], metadata['duration']
    thumb_path = await screenshot(file, duration, sender)

    file_ext = file.split('.')[-1].lower()
    
    if file_ext in {'mp4', 'mkv', 'avi', 'mov'}:
        msg = await client.send_video(
            chat_id=target_chat_id,
            video=file,
            caption=caption,
            height=height,
            width=width,
            duration=duration,
            thumb=thumb_path,
            reply_to_message_id=topic_id,
            parse_mode=ParseMode.MARKDOWN,
            progress=progress_bar,
            progress_args=("╭─────────────────────╮\n│      **__Pyro Uploader__**\n├─────────────────────", edit, time.time())
        )
    elif file_ext in {'jpg', 'png', 'jpeg'}:
        msg = await client.send_photo(
            chat_id=target_chat_id,
            photo=file,
            caption=caption,
            parse_mode=ParseMode.MARKDOWN,
            progress=progress_bar,
            reply_to_message_id=topic_id,
            progress_args=("╭─────────────────────╮\n│      **__Pyro Uploader__**\n├─────────────────────", edit, time.time())
        )
    else:
        msg = await client.send_document(
            chat_id=target_chat_id,
            document=file,
            caption=caption,
            thumb=thumb_path,
            reply_to_message_id=topic_id,
            progress=progress_bar,
            parse_mode=ParseMode.MARKDOWN,
            progress_args=("╭─────────────────────╮\n│      **__Pyro Uploader__**\n├─────────────────────", edit, time.time())
        )
    
    if thumb_path and os.path.exists(thumb_path):
        os.remove(thumb_path)
    
    return msg

async def telethon_upload(
    client,
    sender: int,
    target_chat_id: int,
    file: str,
    caption: Optional[str] = None,
    topic_id: Optional[int] = None
) -> Message:
    """Upload media using Telethon"""
    metadata = video_metadata(file)
    duration, width, height = metadata['duration'], metadata['width'], metadata['height']
    thumb_path = await screenshot(file, duration, sender)
    
    progress_message = await client.send_message(sender, "**__Uploading...__**")
    
    uploaded = await fast_upload(
        client, file,
        reply=progress_message,
        progress_bar_function=lambda done, total: progress_callback(done, total, sender)
    )
    
    attributes = [
        DocumentAttributeVideo(
            duration=duration,
            w=width,
            h=height,
            supports_streaming=True
        )
    ] if file.split('.')[-1].lower() in {'mp4', 'mkv', 'avi', 'mov'} else []

    msg = await client.send_file(
        target_chat_id,
        uploaded,
        caption=caption,
        attributes=attributes,
        reply_to=topic_id,
        thumb=thumb_path
    )
    
    await progress_message.delete()
    if thumb_path and os.path.exists(thumb_path):
        os.remove(thumb_path)
    
    return msg
