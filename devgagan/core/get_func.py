# ---------------------------------------------------
# File Name: get_func.py
# Description: A Pyrogram bot for downloading files from Telegram channels or groups 
#              and uploading them back to Telegram.
# Author: Gagan
# GitHub: https://github.com/devgaganin/
# Telegram: https://t.me/team_spy_pro
# YouTube: https://youtube.com/@dev_gagan
# Created: 2025-01-11
# Last Modified: 2025-02-01
# Version: 2.0.5
# License: MIT License
# Improved logic handles
# ---------------------------------------------------

import asyncio
import time
import gc
import os
import re
import logging
from typing import Callable
from telethon import TelegramClient, events, Button, types
from telethon.errors import (
    ChannelInvalidError, 
    ChannelPrivateError, 
    ChatIdInvalidError, 
    ChatInvalidError,
    FileMigrateError,
    AuthBytesInvalidError,
    FloodWaitError
)
from devgagan.core.fasthelper import fast_upload, fast_download, safe_turbo_download
from telethon import functions, types
from telethon.tl.types import DocumentAttributeVideo, Message
from telethon.sessions import StringSession
import pymongo
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import ChannelBanned, ChannelInvalid, ChannelPrivate, ChatIdInvalid, ChatInvalid
from pyrogram.enums import MessageMediaType, ParseMode
from pyrogram.errors import RPCError
from pyrogram.types import Message
from config import MONGO_DB as MONGODB_CONNECTION_STRING, LOG_GROUP, OWNER_ID, STRING, API_ID, CONTACT, API_HASH
from devgagan.core.mongo.db import set_session, remove_session, get_data
from devgagantools import fast_upload as fast_uploads
#from devgagantools import fast_download
from devgagan.core.func import *
from devgagan.modules.shrink import is_user_verified, get_pyro_bot, get_tele_bot
from telethon import TelegramClient, events, Button
from devgagan import app
from devgagan import telethon_user_client  as gf
from telethon.tl.types import MessageMediaDocument
from pyrogram import filters, Client

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def thumbnail(sender):
    """Get thumbnail path for user if exists"""
    thumb_path = f'{sender}.jpg'
    return thumb_path if os.path.exists(thumb_path) else None

# MongoDB database name and collection name
DB_NAME = "smart_users"
COLLECTION_NAME = "super_user"

VIDEO_EXTENSIONS = ['mp4', 'mov', 'avi', 'mkv', 'flv', 'wmv', 'webm', 'mpg', 'mpeg', '3gp', 'ts', 'm4v', 'f4v', 'vob']
DOCUMENT_EXTENSIONS = ['pdf', 'docs']

mongo_app = pymongo.MongoClient(MONGODB_CONNECTION_STRING)
db = mongo_app[DB_NAME]
collection = db[COLLECTION_NAME]

if STRING:
    from devgagan import pro
    logger.info("App imported from package.")
else:
    pro = None
    logger.info("STRING is not available. 'app' is set to None.")



import os
from telethon import types
from telethon.errors import (
    ChannelInvalidError,
    ChannelPrivateError,
    ChatIdInvalidError,
    ChatInvalidError,
)

async def initialize_userbot(user_id): # this ensure the single startup .. even if logged in or not
    """Initialize the userbot session for the given user."""
    data = await db.get_data(user_id)
    if data and data.get("session"):
        try:
            device = 'iPhone 16 Pro' # added gareebi text
            userbot = Client(
                "userbot",
                api_id=API_ID,
                api_hash=API_HASH,
                device_model=device,
                session_string=data.get("session")
            )
            await userbot.start()
            return userbot
        except Exception:
            return None
    return None


import time
from typing import Callable
import yt_dlp

async def download_with_progress(
    url: str,
    output_path: str = "downloads/",
    progress_callback: Callable = None,
    update_interval: int = 3
) -> str:
    """
    Download from any URL with progress updates every N seconds
    Supports:
    - Direct HTTP downloads
    - YouTube videos (via yt-dlp)
    - Streamable media (via FFmpeg)
    """
    
    os.makedirs(output_path, exist_ok=True)
    last_update = 0
    downloaded = 0
    
    async def update_progress(current: int, total: int):
        nonlocal last_update, downloaded
        downloaded = current
        now = time.time()
        if now - last_update >= update_interval:
            if progress_callback:
                await progress_callback(current, total)
            last_update = now
    
    # YouTube Downloader
    if "youtube.com" in url or "youtu.be" in url:
        ydl_opts = {
            'format': 'best',
            'outtmpl': os.path.join(output_path, '%(title)s.%(ext)s'),
            'progress_hooks': [lambda d: asyncio.create_task(
                update_progress(d['downloaded_bytes'], d['total_bytes'] if d['total_bytes'] else 1)
            )],
        }
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            return ydl.prepare_filename(info)
    
    # Direct HTTP Download
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            total = int(response.headers.get('content-length', 0))
            filename = os.path.join(output_path, url.split('/')[-1].split('?')[0])
            
            with open(filename, 'wb') as f:
                async for chunk in response.content.iter_chunked(1024):
                    f.write(chunk)
                    downloaded += len(chunk)
                    await update_progress(downloaded, total)
            
            return filename

async def progress_callback(current: int, total: int, message: Message):
    """Example callback for Telegram progress updates"""
    percent = (current / total) * 100
    await message.edit_text(
        f"**Download Progress:**\n"
        f"`{human_readable_size(current)} / {human_readable_size(total)}`\n"
        f"**{percent:.1f}%**\n"
        f"╰ {'▰' * int(percent//10)}{'▱' * (10 - int(percent//10))}"
    )

def human_readable_size(size: int) -> str:
    """Convert bytes to human-readable format"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} TB"



async def get_telegram_direct_url(client: Client, file: Message) -> str:
    """Get direct download URL for Telegram file"""
    logger.info("Handles message get_telegram_direct_url, start")
    if file.document or file.video or file.audio:
        file_id = file.document.file_id if file.document else file.video.file_id if file.video else file.audio.file_id
        logger.info("Handles message processing using Pyrogram client, file_id")
    else:
        logger.warning("The provided message does not contain a supported file type (document, video, or audio).")
        return None
    

    try:
        file_info = await client.get_file(file_id)
        # ... continue processing
    except FileIdInvalid:
        logger.error("The file_id '%s' is invalid. Skipping.", file_id)
    except Exception as e:
        logger.error("An unexpected error occurred for file_id '%s': %s", file_id, e)
    logger.info("Handles message processing using Pyrogram client, file_info")
    # Construct direct download URL
    if hasattr(file_info, 'file_path'):
        # For newer files (CDN)
        return f"https://api.telegram.org/file/bot{client.token}/{file_info.file_path}"
    else:
        # For older files (legacy)
        return f"https://api.telegram.org/file/bot{client.token}/{file_id}"


async def get_msg_direct(userbot, sender, edit_id, msg_link, i, message):
    try:
        logger.info("Handles message processing using Pyrogram client, get_msg_direct")
        # Sanitize the message link
        msg_link = msg_link.split("?single")[0]
        chat, msg_id = None, None
        saved_channel_ids = load_saved_channel_ids()
        size_limit = 2 * 1024 * 1024 * 1024  # 1.99 GB size limit
        file = ''
        edit = ''
        # Extract chat and message ID for valid Telegram links
        if 't.me/c/' in msg_link or 't.me/b/' in msg_link:
            parts = msg_link.split("/")
            if 't.me/b/' in msg_link:
                chat = parts[-2]
                msg_id = int(parts[-1]) + i # fixed bot problem
            else:
                chat = int('-100' + parts[parts.index('c') + 1])
                msg_id = int(parts[-1]) + i

            if chat in saved_channel_ids:
                await app.edit_message_text(
                    message.chat.id, edit_id,
                    "Sorry! This channel is protected by **Admin**."
                )
                return

        elif '/s/' in msg_link: # fixed story typo
            await edit.delete(2)
            return

        else:
            await edit.delete(2)
            return

        # Fetch the target message
        msg = await userbot.get_messages(chat, msg_id)
        if not msg or msg.service or msg.empty or msg.sticker or msg.text or msg.media == MessageMediaType.WEB_PAGE_PREVIEW:
            return

        target_chat_id = user_chat_ids.get(message.chat.id, message.chat.id)
        topic_id = None
        if '/' in str(target_chat_id):
            target_chat_id, topic_id = map(int, target_chat_id.split('/', 1))

        # Handle file media (photo, document, video)
        file_size = get_message_file_size(msg)

        if file_size and file_size > size_limit and pro is None:
            await app.edit_message_text(sender, edit_id, "**❌ 4GB Uploader not found**")
            return

        file_name = await get_media_filename(msg)
        edit = await app.edit_message_text(sender, edit_id, "**Downloading...**")
        logger.info("Handles message get_telegram_direct_url, Downloading")


        direct_url = await get_telegram_direct_url(userbot, msg)
        file = None
        if direct_url:
            logger.info("Handles message direct_url, Downloading")
            try:
                file = await download_with_progress(direct_url,
                                                         progress_callback=lambda c, t: progress_callback(c, t, edit)
                                                        )
                await edit.edit(f"✅ Download complete!\nSaved to: `{file_path}`")
            except Exception as e:
                await edit.edit(f"❌ Download failed:\n`{str(e)}`")
        else:
            print("Could not generate a direct URL for this file type.")
        
        caption = await get_final_caption(msg, sender)

        # Rename file
        file = await rename_file(file, sender)
        
        # Upload media
        # await edit.edit("**Checking file...**")
        if file_size > size_limit:
            await handle_large_file(file, sender, edit, caption)
        else:
            result = await upload_media_telethon(sender, target_chat_id, file, caption, topic_id)
            if result:
                await result.copy(LOG_GROUP)

    except (ChannelBanned, ChannelInvalid, ChannelPrivate, ChatIdInvalid, ChatInvalid):
        await app.edit_message_text(sender, edit_id, "Have you joined the channel?")
    except Exception as e:
        # await app.edit_message_text(sender, edit_id, f"Failed to save: `{msg_link}`\n\nError: {str(e)}")
        #print(f"Error: {e}")
        pass
    finally:
        # Clean up
        if file and os.path.exists(file):
            os.remove(file)
        if edit:
            await edit.delete(2)




async def get_msg_telethon(telethon_userbot, sender, edit_id, msg_link, i, message):
    """
    Handles message processing using Telethon client.
    """
    logger.info("Handles message processing using Telethon client, get_msg_telethon")
    file = ''
    edit = None
    try:
        msg_link = msg_link.split("?single")[0]
        chat, msg_id = None, None
        saved_channel_ids = load_saved_channel_ids()
        size_limit = 2 * 1024 * 1024 * 1024  # 2 GB size limit

        if 't.me/c/' in msg_link or 't.me/b/' in msg_link:
            parts = msg_link.split("/")
            if 't.me/b/' in msg_link:
                chat = parts[-2]
                msg_id = int(parts[-1]) + i
            else:
                chat = int('-100' + parts[parts.index('c') + 1])
                msg_id = int(parts[-1]) + i

            if chat in saved_channel_ids:
                await app.edit_message_text(
                    message.chat.id, edit_id,
                    "Sorry! This channel is protected by **Admin**."
                )
                return

        elif '/s/' in msg_link:
            edit = await app.edit_message_text(sender, edit_id, "Story Link Detected...")
            if telethon_userbot is None:
                await edit.edit("Login in bot to save stories...")
                return
            parts = msg_link.split("/")
            chat = parts[3]

            if chat.isdigit():
                chat = f"-100{chat}"

            msg_id = int(parts[-1])
            await download_user_stories_telethon(telethon_userbot, chat, msg_id, edit, sender)
            await edit.delete()
            return

        else:
            edit = await app.edit_message_text(sender, edit_id, "Public link detected...")
            chat = msg_link.split("t.me/")[1].split("/")[0]
            msg_id = int(msg_link.split("/")[-1])
            await copy_message_with_chat_id_telethon(app, telethon_userbot, sender, chat, msg_id, edit)
            await edit.delete()
            return

        msg = await telethon_userbot.get_messages(chat, ids=msg_id)
        if not msg or isinstance(msg, types.MessageService):
            return

        target_chat_id = user_chat_ids.get(message.chat.id, message.chat.id)
        topic_id = None
        if isinstance(target_chat_id, str) and '/' in target_chat_id:
            target_chat_id, topic_id = map(int, target_chat_id.split('/', 1))

        if not hasattr(msg, 'media') or msg.media is None or isinstance(msg.media, types.MessageMediaWebPage):
            if msg.text:
                await clone_message(app, msg, target_chat_id, topic_id, edit_id, LOG_GROUP)
            return

        if msg.sticker:
            await handle_sticker(app, msg, target_chat_id, topic_id, edit_id, LOG_GROUP)
            return

        file_size = get_message_file_size_telethon(msg)

        if file_size and file_size > size_limit and pro is None:
            await app.edit_message_text(sender, edit_id, "**❌ 4GB Uploader not found**")
            return

        #edit = await app.edit_message_text(sender, edit_id, "")
        progress_message = await gf.send_message(sender, "**__Downloading__...**")

        try:
            logger.info("__Downloading__  media using Telethon client, fast_download")
            file = await fast_download(
                telethon_userbot, msg,
                reply=progress_message,
                progress_bar_function=lambda done, total: dl_progress_callback(done, total, sender)
            )
            await progress_message.delete()

        except FileMigrateError as e:
            # Fall back to get_msg if DC migration happens
            await progress_message.delete()
            logger.warning(f"File migrated to DC {e.new_dc}, falling back to get_msg")
            userbot = await initialize_userbot(sender)
            return await get_msg(userbot, sender, edit_id, msg_link, i, message)

        except AuthBytesInvalidError:
            await progress_message.edit("⚠️ Authorization failed. Retrying with new connection...")
            await asyncio.sleep(2)
            userbot = await initialize_userbot(sender)
            return await get_msg(userbot, sender, edit_id, msg_link, i, message)
        except Exception as e:
            await progress_message.edit(f"Error downloading with Telethon: {e}")
            #await progress_message.delete()
            return

        caption = await get_final_caption(msg, sender)
        file = await rename_file(file, sender)

        result = None
        if isinstance(msg.media, types.MessageMediaPhoto):
            if sender not in OWNER_ID:
                bot_client_tele = await get_tele_bot()
                result = await bot_client_tele.send_photo(target_chat_id, file, caption=caption, reply_to_message_id=topic_id)

            else:
                result = await app.send_photo(target_chat_id, file, caption=caption, reply_to_message_id=topic_id)
        else:
            # Fallback for other media types or if file size exceeds limit
            if file_size and file_size > size_limit:
                await handle_large_file(file, sender, edit, caption)
            else:
                # This ensures any remaining media types also use upload_media_telethon
                result = await upload_media_telethon(sender, target_chat_id, file, caption, topic_id)
            

        if result:
            await result.copy(LOG_GROUP)
        
        # Ensure edit is deleted if it was created
        if edit:
            await edit.delete()

    except (ChannelInvalidError, ChannelPrivateError, ChatIdInvalidError, ChatInvalidError) as e:
        logger.error(f"Channel error: {e}")
        await app.edit_message_text(sender, edit_id, "Have you joined the channel?")
    except Exception as e:
        logger.error(f"Error in get_msg_telethon: {e}")
    finally:
        if file and os.path.exists(file):
            os.remove(file)
        if edit and hasattr(edit, 'delete'): 
            await edit.delete()


async def upload_media_telethon(sender, target_chat_id, file, caption, topic_id):
    try:
        print("UPLOADING MEDIA TELETHON")
        # Get file metadata
        metadata = video_metadata(file)
        width, height, duration = metadata['width'], metadata['height'], metadata['duration']
        thumb_path = await screenshot(file, duration, sender)

        video_formats = {'mp4', 'mkv', 'avi', 'mov'}
        document_formats = {'pdf', 'docx', 'txt', 'epub'}
        image_formats = {'jpg', 'png', 'jpeg'}

        # Delete the edit message since we'll use our own progress
        #await edit.delete()
        progress_message = await gf.send_message(sender, "**__Uploading...__**")

        bot_client = gf
        if sender not in OWNER_ID:
            bot_client = await get_tele_bot()
        # Upload with floodwait handling
        try:
            uploaded = await fast_uploads(
                bot_client, file,
                reply=progress_message,
                name=None,
                progress_bar_function=lambda done, total: progress_callback(done, total, sender)
            )
        except FloodWaitError as fw:
            await progress_message.edit(f"⏳ FloodWait: Sleeping for {fw.seconds} seconds...")
            await asyncio.sleep(fw.seconds)
            # Retry after floodwait
            uploaded = await fast_upload(
                bot_client, file,
                reply=progress_message,
                name=None,
                progress_bar_function=lambda done, total: progress_callback(done, total, sender)
            )

        await progress_message.delete()

        # Prepare attributes based on file type
        attributes = []
        if file.split('.')[-1].lower() in video_formats:
            attributes.append(DocumentAttributeVideo(
                duration=duration,
                w=width,
                h=height,
                supports_streaming=True
            ))

        # Send to target chat
        await bot_client.send_file(
            target_chat_id,
            uploaded,
            caption=caption,
            attributes=attributes,
            reply_to=topic_id,
            thumb=thumb_path
        )

        # Send to log group
        await gf.send_file(
            LOG_GROUP,
            uploaded,
            caption=caption,
            attributes=attributes,
            thumb=thumb_path
        )

    except Exception as e:
        await gf.send_message(LOG_GROUP, f"**Upload Failed:** {str(e)}")
        print(f"Error during media upload: {e}")
        raise  # Re-raise the exception for higher level handling

    finally:
        # Cleanup
        if thumb_path and os.path.exists(thumb_path):
            os.remove(thumb_path)
        if 'progress_message' in locals():
            try:
                await progress_message.delete()
            except:
                pass
        gc.collect()














async def upload_media_telethondl(sender, target_chat_id, file, caption, topic_id):
    try:
        print("UPLOADING MEDIA TELETHON")
        # Get file metadata
        metadata = video_metadata(file)
        width, height, duration = metadata['width'], metadata['height'], metadata['duration']
        thumb_path = await screenshot(file, duration, sender)

        video_formats = {'mp4', 'mkv', 'avi', 'mov'}
        document_formats = {'pdf', 'docx', 'txt', 'epub'}
        image_formats = {'jpg', 'png', 'jpeg'}

        # Delete the edit message since we'll use our own progress
        #await edit.delete()
        progress_message = await gf.send_message(sender, "**__Uploading...__**")

        # Upload with floodwait handling
        try:
            uploaded = await fast_uploads(
                gf, file,
                reply=progress_message,
                name=None,
                progress_bar_function=lambda done, total: progress_callback(done, total, sender)
            )
        except FloodWaitError as fw:
            await progress_message.edit(f"⏳ FloodWait: Sleeping for {fw.seconds} seconds...")
            await asyncio.sleep(fw.seconds)
            # Retry after floodwait
            uploaded = await fast_upload(
                gf, file,
                reply=progress_message,
                name=None,
                progress_bar_function=lambda done, total: progress_callback(done, total, sender)
            )

        await progress_message.delete()

        # Prepare attributes based on file type
        attributes = []
        if file.split('.')[-1].lower() in video_formats:
            attributes.append(DocumentAttributeVideo(
                duration=duration,
                w=width,
                h=height,
                supports_streaming=True
            ))

        # Send to target chat
        await gf.send_file(
            target_chat_id,
            uploaded,
            caption=caption,
            attributes=attributes,
            reply_to=topic_id,
            thumb=thumb_path
        )

        # Send to log group
        await gf.send_file(
            LOG_GROUP,
            uploaded,
            caption=caption,
            attributes=attributes,
            thumb=thumb_path
        )
        return True

    except Exception as e:
        await gf.send_message(LOG_GROUP, f"**Upload Failed:** {str(e)}")
        print(f"Error during media upload: {e}")
        raise  # Re-raise the exception for higher level handling
        return False

    finally:
        # Cleanup
        if thumb_path and os.path.exists(thumb_path):
            os.remove(thumb_path)
        if 'progress_message' in locals():
            try:
                await progress_message.delete()
            except:
                pass
        gc.collect()










async def get_media_filename_telethon(msg):
    """Get filename from media message"""
    if isinstance(msg.media, types.MessageMediaDocument):
        for attr in msg.media.document.attributes:
            if isinstance(attr, types.DocumentAttributeFilename):
                return attr.file_name
        return "document"
    if isinstance(msg.media, types.MessageMediaPhoto):
        return "photo.jpg"
    return "unknown_file"

def get_message_file_size_telethon(msg):
    """Get file size from message"""
    if isinstance(msg.media, types.MessageMediaDocument):
        return msg.media.document.size
    if isinstance(msg.media, types.MessageMediaPhoto):
        return None  # Photos don't have size in Telethon
    return 1



async def download_user_stories_telethon(telethon_userbot, chat_id, msg_id, edit, sender):
    """Download user stories using Telethon (1.40.0+ compatible)"""
    try:
        # Try modern approach first (Telethon 2.x)
        try:
            from telethon.tl.functions.stories import GetStoriesRequest
            stories = await telethon_userbot(GetStoriesRequest(peer=chat_id, id=[msg_id]))
        except (ImportError, AttributeError):
            # Fallback to legacy method (Telethon 1.x)
            story = await telethon_userbot.get_messages(chat_id, ids=msg_id)
            if not story or not story.media:
                await edit.edit("No story available or no media found")
                return
            stories = type('Stories', (), {'stories': [story]})()  # Create dummy stories object

        if not stories or not stories.stories:
            await edit.edit("No story available for this user.")
            return
            
        story = stories.stories[0]
        if not story.media:
            await edit.edit("The story doesn't contain any media.")
            return
            
        await edit.edit("⬇️ Downloading Story...")
        file_path = await telethon_userbot.download_media(
            story.media,
            progress_callback=lambda d, t: asyncio.create_task(
                edit.edit(f"⬇️ Downloading... {d * 100 / t:.1f}%")
            ) if d and t else None
        )
        
        if not file_path or not os.path.exists(file_path):
            raise Exception("Download failed or file not found")
            
        logger.info(f"Story downloaded: {file_path}")

        await edit.edit("⬆️ Uploading Story...")
        if isinstance(story.media, types.MessageMediaPhoto):
            await app.send_photo(
                sender,
                file_path,
                progress=progress_bar,
                progress_args=("Uploading story photo...", edit)
            )
        elif isinstance(story.media, types.MessageMediaDocument):
            await app.send_document(
                sender,
                file_path,
                progress=progress_bar,
                progress_args=("Uploading story document...", edit)
            )
            
        await edit.edit("✅ Story processed successfully")
        
    except FloodWaitError as fw:
        await edit.edit(f"⏳ FloodWait: Please wait {fw.seconds} seconds")
        await asyncio.sleep(fw.seconds)
        # Optionally add retry logic here
    except Exception as e:
        logger.error(f"Story processing failed: {str(e)}", exc_info=True)
        await edit.edit(f"❌ Error: {str(e)}")
    finally:
        if 'file_path' in locals() and file_path and os.path.exists(file_path):
            os.remove(file_path)


async def copy_message_with_chat_id_telethon(app, telethon_userbot, sender, chat_id, message_id, edit):
    """Copy message with chat ID handling"""
    target_chat_id = user_chat_ids.get(sender, sender)
    file = None
    result = None
    size_limit = 2 * 1024 * 1024 * 1024  # 2 GB size limit

    try:
        msg = await app.get_messages(chat_id, message_id)
        custom_caption = get_user_caption_preference(sender)
        final_caption = format_caption(msg.caption or '', sender, custom_caption)

        # Parse target_chat_id and topic_id
        topic_id = None
        if '/' in str(target_chat_id):
            target_chat_id, topic_id = map(int, target_chat_id.split('/', 1))

        # Handle different media types
        if msg.media:
            result = await send_media_message(app, target_chat_id, msg, final_caption, topic_id)
            return
        elif msg.text:
            result = await app.copy_message(target_chat_id, chat_id, message_id, reply_to_message_id=topic_id)
            return

        # Fallback if result is None
        if result is None:
            await edit.edit("Trying if it is a group...")
            chat = await telethon_userbot.get_entity(f"@{chat_id}")
            msg = await telethon_userbot.get_messages(chat.id, ids=message_id)

            if not msg or isinstance(msg, types.MessageService) or not hasattr(msg, 'message'):
                return

            final_caption = format_caption(msg.message if msg.message else "", sender, custom_caption)
            file = await telethon_userbot.download_media(
                msg,
                progress_callback=lambda current, total: progress_callback(current, total, sender)
            )
            file = await rename_file(file, sender)

            if isinstance(msg.media, types.MessageMediaPhoto):
                result = await app.send_photo(target_chat_id, file, caption=final_caption, reply_to_message_id=topic_id)
            elif isinstance(msg.media, types.MessageMediaDocument):
                if await is_file_size_exceeding(file, size_limit):
                    await handle_large_file(file, sender, edit, final_caption)
                    return
                await upload_media_telethon(sender, target_chat_id, file, final_caption, edit, topic_id)
            elif isinstance(msg.media, types.MessageMediaAudio):
                if msg.media.audio.voice:
                    result = await app.send_voice(target_chat_id, file, reply_to_message_id=topic_id)
                else:
                    result = await app.send_audio(target_chat_id, file, caption=final_caption, reply_to_message_id=topic_id)
            elif msg.sticker:
                result = await app.send_sticker(target_chat_id, msg.sticker.id, reply_to_message_id=topic_id)
            else:
                await edit.edit("Unsupported media type.")

    except Exception as e:
        logger.error(f"Error in copy_message_with_chat_id_telethon: {e}")
    finally:
        if file and os.path.exists(file):
            os.remove(file)


async def upload_media(sender, target_chat_id, file, caption, edit, topic_id):
    
    thumb_path = None # Initialize thumb_path to None

    try:
        
        try:
            metadata = video_metadata(file)
            width, height, duration = metadata['width'], metadata['height'], metadata['duration']
            thumb_path = await screenshot(file, duration, sender)
        except Exception:
            # Handle cases where metadata or screenshot fails (e.g., non-video file)
            width, height, duration = None, None, None
            thumb_path = None # Ensure thumb_path is None if screenshot fails


        video_formats = {'mp4', 'mkv', 'avi', 'mov'}
        document_formats = {'pdf', 'docx', 'txt', 'epub'} # document_formats is defined but not used in the current logic
        image_formats = {'jpg', 'png', 'jpeg'}

        # Determine file extension
        file_extension = file.split('.')[-1].lower()

        bot_client = app
        if sender not in OWNER_ID:
            bot_client = await get_pyro_bot()
        # Check file format and upload accordingly
        if file_extension in video_formats:
            # Correctly indented block for video upload
            dm = await bot_client.send_video(
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
            if bot_client == app:
                await dm.copy(LOG_GROUP) # Copy to log group

            else:
                try:
                    await app.copy_message(chat_id=LOG_GROUP,
                                           from_chat_id=dm.chat.id,
                                           message_id=dm.id,
                                           )
                    
                except Exception as e:
                    logger.error(f"Copy failed: {e}")
            #await dm.copy(LOG_GROUP) # Copy to log group

        elif file_extension in image_formats:
            # Correctly indented block for image upload
            dm = await bot_client.send_photo(
                chat_id=target_chat_id,
                photo=file,
                caption=caption,
                parse_mode=ParseMode.MARKDOWN,
                progress=progress_bar,
                reply_to_message_id=topic_id,
                progress_args=("╭─────────────────────╮\n│      **__Pyro Uploader__**\n├─────────────────────", edit, time.time())
            )
            if bot_client == app:
                await dm.copy(LOG_GROUP) # Copy to log group

            else:
                try:
                    await app.copy_message(chat_id=LOG_GROUP,
                                           from_chat_id=dm.chat.id,
                                           message_id=dm.id,
                                           )
                    
                except Exception as e:
                    logger.error(f"Copy failed: {e}")
            #await dm.copy(LOG_GROUP) # Copy to log group

        else:
            # Correctly indented block for document upload (covers other formats)
            dm = await bot_client.send_document(
                chat_id=target_chat_id,
                document=file,
                caption=caption,
                thumb=thumb_path, # Using thumb for document might not be standard, check Pyrogram docs
                reply_to_message_id=topic_id,
                progress=progress_bar,
                parse_mode=ParseMode.MARKDOWN,
                progress_args=("╭─────────────────────╮\n│      **__Pyro Uploader__**\n├─────────────────────", edit, time.time())
            )
            await asyncio.sleep(2) # Added a small delay as in your original code
            if bot_client == app:
                await dm.copy(LOG_GROUP) # Copy to log group

            else:
                try:
                    await app.copy_message(chat_id=LOG_GROUP,
                                           from_chat_id=dm.chat.id,
                                           message_id=dm.id,
                                           )
                    
                except Exception as e:
                    logger.error(f"Copy failed: {e}")


    except Exception as e:
        # Catch any exceptions during the process and log/send an error message
        await app.send_message(LOG_GROUP, f"**Upload Failed:** {str(e)}")
        print(f"Error during media upload: {e}")

    finally:
        # Clean up the thumbnail file if it was created
        if thumb_path and os.path.exists(thumb_path):
            try:
                os.remove(thumb_path)
            except Exception as e:
                print(f"Error removing thumbnail file {thumb_path}: {e}")

        # Trigger garbage collection
        gc.collect()





async def get_msg(userbot, sender, edit_id, msg_link, i, message):
    try:
        logger.info("Handles message processing using Pyrogram client, get_msg")
        # Sanitize the message link
        msg_link = msg_link.split("?single")[0]
        chat, msg_id = None, None
        saved_channel_ids = load_saved_channel_ids()
        size_limit = 2 * 1024 * 1024 * 1024  # 1.99 GB size limit
        file = ''
        edit = ''
        # Extract chat and message ID for valid Telegram links
        if 't.me/c/' in msg_link or 't.me/b/' in msg_link:
            parts = msg_link.split("/")
            if 't.me/b/' in msg_link:
                chat = parts[-2]
                msg_id = int(parts[-1]) + i # fixed bot problem
            else:
                chat = int('-100' + parts[parts.index('c') + 1])
                msg_id = int(parts[-1]) + i

            if chat in saved_channel_ids:
                await app.edit_message_text(
                    message.chat.id, edit_id,
                    "Sorry! This channel is protected by **Admin**."
                )
                return

        elif '/s/' in msg_link: # fixed story typo
            edit = await app.edit_message_text(sender, edit_id, "Story Link Dictected...")
            if userbot is None:
                await edit.edit("Login in bot save stories...")
                return
            parts = msg_link.split("/")
            chat = parts[3]

            if chat.isdigit():    # this is for channel stories
                chat = f"-100{chat}"

            msg_id = int(parts[-1])
            await download_user_stories(userbot, chat, msg_id, edit, sender)
            await edit.delete(2)
            return

        else:
            edit = await app.edit_message_text(sender, edit_id, "Public link detected...")
            chat = msg_link.split("t.me/")[1].split("/")[0]
            msg_id = int(msg_link.split("/")[-1])
            await copy_message_with_chat_id(app, userbot, sender, chat, msg_id, edit)
            await edit.delete(2)
            return

        # Fetch the target message
        msg = await userbot.get_messages(chat, msg_id)
        if not msg or msg.service or msg.empty:
            return

        target_chat_id = user_chat_ids.get(message.chat.id, message.chat.id)
        topic_id = None
        if '/' in str(target_chat_id):
            target_chat_id, topic_id = map(int, target_chat_id.split('/', 1))

        # Handle different message types
        if msg.media == MessageMediaType.WEB_PAGE_PREVIEW:
            await clone_message(app, msg, target_chat_id, topic_id, edit_id, LOG_GROUP)
            return

        if msg.text:
            await clone_message(app, msg, target_chat_id, topic_id, edit_id, LOG_GROUP)
            return

        if msg.sticker:
            await handle_sticker(app, msg, target_chat_id, topic_id, edit_id, LOG_GROUP)
            return


        # Handle file media (photo, document, video)
        file_size = get_message_file_size(msg)

        if file_size and file_size > size_limit and pro is None:
            await app.edit_message_text(sender, edit_id, "**❌ 4GB Uploader not found**")
            return

        file_name = await get_media_filename(msg)
        edit = await app.edit_message_text(sender, edit_id, "**Downloading...**")

        # Pyrogram Download
        file = await userbot.download_media(
                msg,
                file_name=file_name,
                progress=progress_bar,
                progress_args=("╭─────────────────────╮\n│      **__Downloading__...**\n├─────────────────────", edit, time.time())
            )
        
        caption = await get_final_caption(msg, sender)

        # Rename file
        file = await rename_file(file, sender)
        if msg.audio:
            result = await app.send_audio(target_chat_id, file, caption=caption, reply_to_message_id=topic_id)
            await result.copy(LOG_GROUP)
            await edit.delete(2)
            return

        if msg.voice:
            result = await app.send_voice(target_chat_id, file, reply_to_message_id=topic_id)
            await result.copy(LOG_GROUP)
            await edit.delete(2)
            return

        if msg.photo:
            result = await app.send_photo(target_chat_id, file, caption=caption, reply_to_message_id=topic_id)
            await result.copy(LOG_GROUP)
            await edit.delete(2)
            return

        # Upload media
        # await edit.edit("**Checking file...**")
        if file_size > size_limit:
            await handle_large_file(file, sender, edit, caption)
        else:
            await upload_media(sender, target_chat_id, file, caption, edit, topic_id)

    except (ChannelBanned, ChannelInvalid, ChannelPrivate, ChatIdInvalid, ChatInvalid):
        await app.edit_message_text(sender, edit_id, "Have you joined the channel?")
    except Exception as e:
        # await app.edit_message_text(sender, edit_id, f"Failed to save: `{msg_link}`\n\nError: {str(e)}")
        #print(f"Error: {e}")
        pass
    finally:
        # Clean up
        if file and os.path.exists(file):
            os.remove(file)
        if edit:
            await edit.delete(2)


async def clone_message(app, msg, target_chat_id, topic_id, edit_id, log_group):
    edit = await app.edit_message_text(target_chat_id, edit_id, "Cloning text message...")
    if msg.text.markdown:
        devgaganin = await app.send_message(target_chat_id, msg.text.markdown, reply_to_message_id=topic_id)
    else:
        devgaganin = await app.send_message(target_chat_id, msg.message , reply_to_message_id=topic_id)
    await devgaganin.copy(log_group)
    await edit.delete()


async def handle_sticker(app, msg, target_chat_id, topic_id, edit_id, log_group):
    """
    Handles sticker messages, adapting for Telethon and Pyrogram message objects.
    """
    edit = await app.edit_message_text(target_chat_id, edit_id, "Handling sticker...")

    sticker_id = None
    if hasattr(msg.sticker, 'id'):  # Telethon
        sticker_id = msg.sticker.id
    elif hasattr(msg.sticker, 'file_id'):  # Pyrogram
        sticker_id = msg.sticker.file_id
        
    if sticker_id:
        result = await app.send_sticker(target_chat_id, sticker_id, reply_to_message_id=topic_id)
        await result.copy(log_group)
    else:
        # Handle cases where sticker ID isn't found (e.g., unexpected message object structure)
        await edit.edit("Could not retrieve sticker ID.")
        return


async def get_media_filename(msg):
    if msg.document:
        return msg.document.file_name
    if msg.video:
        return msg.video.file_name if msg.video.file_name else "temp.mp4"
    if msg.photo:
        return "temp.jpg"
    return "unknown_file"

def get_message_file_size(msg):
    if msg.document:
        return msg.document.file_size
    if msg.photo:
        return msg.photo.file_size
    if msg.video:
        return msg.video.file_size
    return 1

async def get_final_caption(msg, sender):
    """
    Generates the final caption for a message, handling both Pyrogram and Telethon message objects,
    and applying custom captions and replacements.
    """
    original_caption = ""
    # Check for caption from Pyrogram's Message object (msg.caption)
    if hasattr(msg, 'caption') and msg.caption:
        original_caption = msg.caption.markdown
    # Check for caption from Telethon's Message object (msg.message)
    elif hasattr(msg, 'message') and msg.message:
        original_caption = msg.message

    custom_caption = get_user_caption_preference(sender)

    # Combine original and custom captions
    final_caption = f"{original_caption}\n\n{custom_caption}" if custom_caption else original_caption

    # Apply word replacements
    replacements = load_replacement_words(sender)
    for word, replace_word in replacements.items():
        final_caption = final_caption.replace(word, replace_word)
        
    return final_caption if final_caption else None

async def download_user_stories(userbot, chat_id, msg_id, edit, sender):
    try:
        # Fetch the story using the provided chat ID and message ID
        story = await userbot.get_stories(chat_id, msg_id)
        if not story:
            await edit.edit("No story available for this user.")
            return  
        if not story.media:
            await edit.edit("The story doesn't contain any media.")
            return
        await edit.edit("Downloading Story...")
        file_path = await userbot.download_media(story)
        print(f"Story downloaded: {file_path}")
        # Send the downloaded story based on its type
        if story.media:
            await edit.edit("Uploading Story...")
            if story.media == MessageMediaType.VIDEO:
                await app.send_video(sender, file_path)
            elif story.media == MessageMediaType.DOCUMENT:
                await app.send_document(sender, file_path)
            elif story.media == MessageMediaType.PHOTO:
                await app.send_photo(sender, file_path)
        if file_path and os.path.exists(file_path):
            os.remove(file_path)  
        await edit.edit("Story processed successfully.")
    except RPCError as e:
        print(f"Failed to fetch story: {e}")
        await edit.edit(f"Error: {e}")
        
async def copy_message_with_chat_id(app, userbot, sender, chat_id, message_id, edit):
    target_chat_id = user_chat_ids.get(sender, sender)
    file = None
    result = None
    size_limit = 2 * 1024 * 1024 * 1024  # 2 GB size limit

    try:
        msg = await app.get_messages(chat_id, message_id)
        custom_caption = get_user_caption_preference(sender)
        final_caption = format_caption(msg.caption or '', sender, custom_caption)

        # Parse target_chat_id and topic_id
        topic_id = None
        if '/' in str(target_chat_id):
            target_chat_id, topic_id = map(int, target_chat_id.split('/', 1))

        # Handle different media types
        if msg.media:
            result = await send_media_message(app, target_chat_id, msg, final_caption, topic_id)
            return
        elif msg.text:
            result = await app.copy_message(target_chat_id, chat_id, message_id, reply_to_message_id=topic_id)
            return

        # Fallback if result is None
        if result is None:
            await edit.edit("Trying if it is a group...")
            chat_id = (await userbot.get_chat(f"@{chat_id}")).id
            msg = await userbot.get_messages(chat_id, message_id)

            if not msg or msg.service or msg.empty:
                return

            final_caption = format_caption(msg.caption.markdown if msg.caption else "", sender, custom_caption)
            file = await userbot.download_media(
                msg,
                progress=progress_bar,
                progress_args=("╭─────────────────────╮\n│      **__Downloading__...**\n├─────────────────────", edit, time.time())
            )
            file = await rename_file(file, sender)

            if msg.photo:
                result = await app.send_photo(target_chat_id, file, caption=final_caption, reply_to_message_id=topic_id)
            elif msg.video or msg.document:
                if await is_file_size_exceeding(file, size_limit):
                    await handle_large_file(file, sender, edit, final_caption)
                    return
                await upload_media(sender, target_chat_id, file, final_caption, edit, topic_id)
            elif msg.audio:
                result = await app.send_audio(target_chat_id, file, caption=final_caption, reply_to_message_id=topic_id)
            elif msg.voice:
                result = await app.send_voice(target_chat_id, file, reply_to_message_id=topic_id)
            elif msg.sticker:
                result = await app.send_sticker(target_chat_id, msg.sticker.file_id, reply_to_message_id=topic_id)
            else:
                await edit.edit("Unsupported media type.")

    except Exception as e:
        print(f"Error : {e}")
        pass
        #error_message = f"Error occurred while processing message: {str(e)}"
        # await app.send_message(sender, error_message)
        # await app.send_message(sender, f"Make Bot admin in your Channel - {target_chat_id} and restart the process after /cancel")

    finally:
        if file and os.path.exists(file):
            os.remove(file)


async def send_media_message(app, target_chat_id, msg, caption, topic_id):
    try:
        if msg.video:
            return await app.send_video(target_chat_id, msg.video.file_id, caption=caption, reply_to_message_id=topic_id)
        if msg.document:
            return await app.send_document(target_chat_id, msg.document.file_id, caption=caption, reply_to_message_id=topic_id)
        if msg.photo:
            return await app.send_photo(target_chat_id, msg.photo.file_id, caption=caption, reply_to_message_id=topic_id)
    except Exception as e:
        print(f"Error while sending media: {e}")
    
    # Fallback to copy_message in case of any exceptions
    return await app.copy_message(target_chat_id, msg.chat.id, msg.id, reply_to_message_id=topic_id)
    

def format_caption(original_caption, sender, custom_caption):
    delete_words = load_delete_words(sender)
    replacements = load_replacement_words(sender)

    # Remove and replace words in the caption
    for word in delete_words:
        original_caption = original_caption.replace(word, '  ')
    for word, replace_word in replacements.items():
        original_caption = original_caption.replace(word, replace_word)

    # Append custom caption if available
    return f"{original_caption}\n\n__**{custom_caption}**__" if custom_caption else original_caption

    
# ------------------------ Button Mode Editz FOR SETTINGS ----------------------------

# Define a dictionary to store user chat IDs
user_chat_ids = {}

def load_user_data(user_id, key, default_value=None):
    try:
        user_data = collection.find_one({"_id": user_id})
        return user_data.get(key, default_value) if user_data else default_value
    except Exception as e:
        print(f"Error loading {key}: {e}")
        return default_value

def load_saved_channel_ids():
    saved_channel_ids = set()
    try:
        # Retrieve channel IDs from MongoDB collection
        for channel_doc in collection.find({"channel_id": {"$exists": True}}):
            saved_channel_ids.add(channel_doc["channel_id"])
    except Exception as e:
        print(f"Error loading saved channel IDs: {e}")
    return saved_channel_ids

def save_user_data(user_id, key, value):
    try:
        collection.update_one(
            {"_id": user_id},
            {"$set": {key: value}},
            upsert=True
        )
    except Exception as e:
        print(f"Error saving {key}: {e}")


# Delete and replacement word functions
load_delete_words = lambda user_id: set(load_user_data(user_id, "delete_words", []))
save_delete_words = lambda user_id, words: save_user_data(user_id, "delete_words", list(words))

load_replacement_words = lambda user_id: load_user_data(user_id, "replacement_words", {})
save_replacement_words = lambda user_id, replacements: save_user_data(user_id, "replacement_words", replacements)

# User session functions
def load_user_session(user_id):
    return load_user_data(user_id, "session")

# Upload preference functions
set_dupload = lambda user_id, value: save_user_data(user_id, "dupload", value)
get_dupload = lambda user_id: load_user_data(user_id, "dupload", False)

# User preferences storage
user_rename_preferences = {}
user_caption_preferences = {}

# Rename and caption preference functions
async def set_rename_command(user_id, custom_rename_tag):
    user_rename_preferences[str(user_id)] = custom_rename_tag

get_user_rename_preference = lambda user_id: user_rename_preferences.get(str(user_id), 'Team A')

async def set_caption_command(user_id, custom_caption):
    user_caption_preferences[str(user_id)] = custom_caption

get_user_caption_preference = lambda user_id: user_caption_preferences.get(str(user_id), '')

# Initialize the dictionary to store user sessions

sessions = {}
m = None
SET_PIC = "settings.jpg"
MESS = "Customize by your end and Configure your settings ..."

@gf.on(events.NewMessage(incoming=True, pattern='/settings'))
async def settings_command(event):
    user_id = event.sender_id
    await send_settings_message(event.chat_id, user_id)

async def send_settings_message(chat_id, user_id):
    
    # Define the rest of the buttons
    buttons = [
        [Button.inline("Set Chat ID", b'setchat'), Button.inline("Set Rename Tag", b'setrename')],
        [Button.inline("Caption", b'setcaption'), Button.inline("Replace Words", b'setreplacement')],
        [Button.inline("Remove Words", b'delete'), Button.inline("Reset", b'reset')],
        [Button.inline("Session Login", b'addsession'), Button.inline("Logout", b'logout')],
        [Button.inline("Set Thumbnail", b'setthumb'), Button.inline("Remove Thumbnail", b'remthumb')],
        [Button.inline("PDF Wtmrk", b'pdfwt'), Button.inline("Video Wtmrk", b'watermark')],
        [Button.inline("Upload Method", b'uploadmethod')],  # Include the dynamic Fast DL button
        [Button.url("Report Errors", CONTACT)]
    ]

    await gf.send_file(
        chat_id,
        file=SET_PIC,
        caption=MESS,
        buttons=buttons
    )


pending_photos = {}

@gf.on(events.CallbackQuery)
async def callback_query_handler(event):
    user_id = event.sender_id
    
    if event.data == b'setchat':
        await event.respond("Send me the ID of that chat:")
        sessions[user_id] = 'setchat'

    elif event.data == b'setrename':
        await event.respond("Send me the rename tag:")
        sessions[user_id] = 'setrename'
    
    elif event.data == b'setcaption':
        await event.respond("Send me the caption:")
        sessions[user_id] = 'setcaption'

    elif event.data == b'setreplacement':
        await event.respond("Send me the replacement words in the format: 'WORD(s)' 'REPLACEWORD'")
        sessions[user_id] = 'setreplacement'

    elif event.data == b'addsession':
        await event.respond("Send Pyrogram V2 session")
        sessions[user_id] = 'addsession' # (If you want to enable session based login just uncomment this and modify response message accordingly)

    elif event.data == b'delete':
        await event.respond("Send words seperated by space to delete them from caption/filename ...")
        sessions[user_id] = 'deleteword'
        
    elif event.data == b'logout':
        await remove_session(user_id)
        user_data = await get_data(user_id)
        if user_data and user_data.get("session") is None:
            await event.respond("Logged out and deleted session successfully.")
        else:
            await event.respond("You are not logged in.")
        
    elif event.data == b'setthumb':
        pending_photos[user_id] = True
        await event.respond('Please send the photo you want to set as the thumbnail.')
    
    elif event.data == b'pdfwt':
        await event.respond("Watermark is Pro+ Plan.. contact @Contact_xbot")
        return

    elif event.data == b'uploadmethod':
        # Retrieve the user's current upload method (default to Pyrogram)
        user_data = collection.find_one({'user_id': user_id})
        current_method = user_data.get('upload_method', 'Pyrogram') if user_data else 'Pyrogram'
        pyrogram_check = " ✅" if current_method == "Pyrogram" else ""
        telethon_check = " ✅" if current_method == "Telethon" else ""

        # Display the buttons for selecting the upload method
        buttons = [
            [Button.inline(f"Pyrogram v2{pyrogram_check}", b'pyrogram')],
            [Button.inline(f"Fast Download ⚡{telethon_check}", b'telethon')]
        ]
        await event.edit("Choose your preferred upload method:\n\n__**Note:** **Fast Download ⚡**, built on Telethon(base), by Admin still in beta.__", buttons=buttons)

    elif event.data == b'pyrogram':
        save_user_upload_method(user_id, "Pyrogram")
        await event.edit("Upload method set to **Pyrogram** ✅")

    elif event.data == b'telethon':
        save_user_upload_method(user_id, "Telethon")
        await event.edit("Upload method set to **Fast Download ⚡\n\nThanks for choosing this library as it will help me to analyze the error raise issues on github.** ✅")        
        
    elif event.data == b'reset':
        try:
            user_id_str = str(user_id)
            
            collection.update_one(
                {"_id": user_id},
                {"$unset": {
                    "delete_words": "",
                    "replacement_words": "",
                    "watermark_text": "",
                    "duration_limit": ""
                }}
            )
            
            collection.update_one(
                {"user_id": user_id},
                {"$unset": {
                    "delete_words": "",
                    "replacement_words": "",
                    "watermark_text": "",
                    "duration_limit": ""
                }}
            )            
            user_chat_ids.pop(user_id, None)
            user_rename_preferences.pop(user_id_str, None)
            user_caption_preferences.pop(user_id_str, None)
            thumbnail_path = f"{user_id}.jpg"
            if os.path.exists(thumbnail_path):
                os.remove(thumbnail_path)
            await event.respond("✅ Reset successfully, to logout click /logout")
        except Exception as e:
            await event.respond(f"Error clearing delete list: {e}")
    
    elif event.data == b'remthumb':
        try:
            os.remove(f'{user_id}.jpg')
            await event.respond('Thumbnail removed successfully!')
        except FileNotFoundError:
            await event.respond("No thumbnail found to remove.")
    

@gf.on(events.NewMessage(func=lambda e: e.sender_id in pending_photos))
async def save_thumbnail(event):
    user_id = event.sender_id  # Use event.sender_id as user_id

    if event.photo:
        temp_path = await event.download_media()
        if os.path.exists(f'{user_id}.jpg'):
            os.remove(f'{user_id}.jpg')
        os.rename(temp_path, f'./{user_id}.jpg')
        await event.respond('Thumbnail saved successfully!')

    else:
        await event.respond('Please send a photo... Retry')

    # Remove user from pending photos dictionary in both cases
    pending_photos.pop(user_id, None)

def save_user_upload_method(user_id, method):
    # Save or update the user's preferred upload method
    collection.update_one(
        {'user_id': user_id},  # Query
        {'$set': {'upload_method': method}},  # Update
        upsert=True  # Create a new document if one doesn't exist
    )

@gf.on(events.NewMessage)
async def handle_user_input(event):
    user_id = event.sender_id
    if user_id in sessions:
        session_type = sessions[user_id]

        if session_type == 'setchat':
            try:
                chat_id = event.text
                user_chat_ids[user_id] = chat_id
                await event.respond("Chat ID set successfully!")
            except ValueError:
                await event.respond("Invalid chat ID!")
                
        elif session_type == 'setrename':
            custom_rename_tag = event.text
            await set_rename_command(user_id, custom_rename_tag)
            await event.respond(f"Custom rename tag set to: {custom_rename_tag}")
        
        elif session_type == 'setcaption':
            custom_caption = event.text
            await set_caption_command(user_id, custom_caption)
            await event.respond(f"Custom caption set to: {custom_caption}")

        elif session_type == 'setreplacement':
            match = re.match(r"'(.+)' '(.+)'", event.text)
            if not match:
                await event.respond("Usage: 'WORD(s)' 'REPLACEWORD'")
            else:
                word, replace_word = match.groups()
                delete_words = load_delete_words(user_id)
                if word in delete_words:
                    await event.respond(f"The word '{word}' is in the delete set and cannot be replaced.")
                else:
                    replacements = load_replacement_words(user_id)
                    replacements[word] = replace_word
                    save_replacement_words(user_id, replacements)
                    await event.respond(f"Replacement saved: '{word}' will be replaced with '{replace_word}'")

        elif session_type == 'addsession':
            session_string = event.text
            await set_session(user_id, session_string)
            await event.respond("✅ Session string added successfully!")
                
        elif session_type == 'deleteword':
            words_to_delete = event.message.text.split()
            delete_words = load_delete_words(user_id)
            delete_words.update(words_to_delete)
            save_delete_words(user_id, delete_words)
            await event.respond(f"Words added to delete list: {', '.join(words_to_delete)}")
               
            
        del sessions[user_id]
    
# Command to store channel IDs
@gf.on(events.NewMessage(incoming=True, pattern='/lock'))
async def lock_command_handler(event):
    if event.sender_id not in OWNER_ID:
        return await event.respond("You are not authorized to use this command.")
    
    # Extract the channel ID from the command
    try:
        channel_id = int(event.text.split(' ')[1])
    except (ValueError, IndexError):
        return await event.respond("Invalid /lock command. Use /lock CHANNEL_ID.")
    
    # Save the channel ID to the MongoDB database
    try:
        # Insert the channel ID into the collection
        collection.insert_one({"channel_id": channel_id})
        await event.respond(f"Channel ID {channel_id} locked successfully.")
    except Exception as e:
        await event.respond(f"Error occurred while locking channel ID: {str(e)}")


async def handle_large_file(file, sender, edit, caption):
    if pro is None:
        await edit.edit('**__ ❌ 4GB trigger not found__**')
        os.remove(file)
        gc.collect()
        return
    
    dm = None
    
    print("4GB connector found.")
    await edit.edit('**__ ✅ 4GB trigger connected...__**\n\n')
    
    target_chat_id = user_chat_ids.get(sender, sender)
    file_extension = str(file).split('.')[-1].lower()
    metadata = video_metadata(file)
    duration = metadata['duration']
    width = metadata['width']
    height = metadata['height']
    
    thumb_path = await screenshot(file, duration, sender)
    try:
        if file_extension in VIDEO_EXTENSIONS:
            dm = await pro.send_video(
                LOG_GROUP,
                video=file,
                caption=caption,
                thumb=thumb_path,
                height=height,
                width=width,
                duration=duration,
                progress=progress_bar,
                progress_args=(
                    "╭─────────────────────╮\n│       **__4GB Uploader__ ⚡**\n├─────────────────────",
                    edit,
                    time.time()
                )
            )
        else:
            # Send as document
            dm = await pro.send_document(
                LOG_GROUP,
                document=file,
                caption=caption,
                thumb=thumb_path,
                progress=progress_bar,
                progress_args=(
                    "╭─────────────────────╮\n│      **__4GB Uploader ⚡__**\n├─────────────────────",
                    edit,
                    time.time()
                )
            )

        from_chat = dm.chat.id
        msg_id = dm.id
        freecheck = 0
        if freecheck == 1:
            reply_markup = InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("💎 Get Premium to Forward", url=CONTACT)]
                ]
            )
            await app.copy_message(
                target_chat_id,
                from_chat,
                msg_id,
                protect_content=True,
                reply_markup=reply_markup
            )
        else:
            # Simple copy without protect_content or reply_markup
            await app.copy_message(
                target_chat_id,
                from_chat,
                msg_id
            )
            
    except Exception as e:
        print(f"Error while sending file: {e}")

    finally:
        await edit.delete()
        os.remove(file)
        gc.collect()
        return

async def rename_file(file, sender):
    delete_words = load_delete_words(sender)
    custom_rename_tag = get_user_rename_preference(sender)
    replacements = load_replacement_words(sender)
    
    last_dot_index = str(file).rfind('.')
    
    if last_dot_index != -1 and last_dot_index != 0:
        ggn_ext = str(file)[last_dot_index + 1:]
        
        if ggn_ext.isalpha() and len(ggn_ext) <= 9:
            if ggn_ext.lower() in VIDEO_EXTENSIONS:
                original_file_name = str(file)[:last_dot_index]
                file_extension = 'mp4'
            else:
                original_file_name = str(file)[:last_dot_index]
                file_extension = ggn_ext
        else:
            original_file_name = str(file)[:last_dot_index]
            file_extension = 'mp4'
    else:
        original_file_name = str(file)
        file_extension = 'mp4'
        
    for word in delete_words:
        original_file_name = original_file_name.replace(word, "")

    for word, replace_word in replacements.items():
        original_file_name = original_file_name.replace(word, replace_word)

    new_file_name = f"{original_file_name} {custom_rename_tag}.{file_extension}"
    await asyncio.to_thread(os.rename, file, new_file_name)
    return new_file_name


async def sanitize(file_name: str) -> str:
    sanitized_name = re.sub(r'[\\/:"*?<>|]', '_', file_name)
    # Strip leading/trailing whitespaces
    return sanitized_name.strip()
    
async def is_file_size_exceeding(file_path, size_limit):
    try:
        return os.path.getsize(file_path) > size_limit
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        return False
    except Exception as e:
        print(f"Error while checking file size: {e}")
        return False


user_progress = {}

def progress_callback(done, total, user_id):
    """
    Calculates and formats the progress bar for file uploads, including speed and ETA.
    """
    # Initialize user's progress tracking if not already present
    if user_id not in user_progress:
        user_progress[user_id] = {
            'previous_done': 0,
            'previous_time': time.time()
        }
    
    user_data = user_progress[user_id]
    
    # Calculate percentages and format the progress bar
    percent = (done / total) * 100
    completed_blocks = int(percent // 10)
    remaining_blocks = 10 - completed_blocks
    progress_bar = "♦" * completed_blocks + "◇" * remaining_blocks
    
    # Convert sizes to MB for display
    done_mb = done / (1024 * 1024)
    total_mb = total / (1024 * 1024)
    
    # Calculate speed in MB/s and Mbps
    current_time = time.time()
    time_delta = current_time - user_data['previous_time']
    
    # Prevent division by zero and handle initial state
    if time_delta > 0 and user_data['previous_done'] is not None:
        bytes_transferred_since_last_update = done - user_data['previous_done']
        speed_bps = bytes_transferred_since_last_update / time_delta  # Bytes per second
    else:
        speed_bps = 0
    
    speed_mbs = speed_bps / (1024 * 1024)  # MB/s
    speed_mbps = (speed_bps * 8) / (1024 * 1024) # Mbps (Mega**bits** per second)

    # Calculate Estimated Time of Arrival (ETA)
    remaining_bytes = total - done
    if speed_bps > 0:
        remaining_time_sec = remaining_bytes / speed_bps
        remaining_time_min = remaining_time_sec / 60
    else:
        remaining_time_min = float('inf') # Infinite if no progress

    # Format the final output string
    final = (
        f"╭──────────────────╮\n"
        f"│      **__Vkk ⚡ Uploader__** \n"
        f"├──────────\n"
        f"│ {progress_bar}\n\n"
        f"│ **__Progress:__** {percent:.2f}%\n"
        f"│ **__Done:__** {done_mb:.2f} MB / {total_mb:.2f} MB\n"
        f"│ **__Speed:__** {speed_mbs:.2f} MB/s ({speed_mbps:.2f} Mbps)\n"
        f"│ **__ETA:__** {remaining_time_min:.2f} min\n"
        f"╰──────────────────╯\n\n"
        f"**__Please wait__**"
    )
    
    # Update tracking variables for the next call
    user_data['previous_done'] = done
    user_data['previous_time'] = current_time
    
    return final

def dl_progress_callback(done, total, user_id):
    if user_id not in user_progress:
        user_progress[user_id] = {'previous_done': 0, 'previous_time': time.time()}
    
    user_data = user_progress[user_id]
    percent = (done / total) * 100
    progress_bar = "♦" * int(percent // 10) + "◇" * (10 - int(percent // 10))
    
    done_mb = done / (1024 * 1024)  # MB
    total_mb = total / (1024 * 1024)
    
    # Calculate speed in MB/s (not Mbps)
    speed_bytes = done - user_data['previous_done']
    elapsed_time = max(0.1, time.time() - user_data['previous_time'])  # Prevent division by zero
    
    speed_mb_per_sec = (speed_bytes / elapsed_time) / (1024 * 1024)  # MB/s
    speed_mbps = speed_mb_per_sec * 8  # Convert to Mbps if needed
    
    # ETA calculation
    remaining_bytes = total - done
    eta_seconds = remaining_bytes / (speed_bytes / elapsed_time) if speed_bytes > 0 else 0
    eta_minutes = eta_seconds / 60
    
    # Update tracking
    user_data['previous_done'] = done
    user_data['previous_time'] = time.time()
    
    return (
        f"╭──────────────────╮\n"
        f"│     **__Vkk ⚡ Downloader__**       \n"
        f"├──────────\n"
        f"│ {progress_bar}\n\n"
        f"│ **__Progress:__** {percent:.2f}%\n"
        f"│ **__Done:__** {done_mb:.2f} MB / {total_mb:.2f} MB\n"
        f"│ **__Speed:__** {speed_mb_per_sec:.2f} MB/s ({speed_mbps:.2f} Mbps)\n"  # Show BOTH
        f"│ **__ETA:__** {eta_minutes:.2f} min\n"
        f"╰──────────────────╯\n\n"
        f"**__Please wait__**"
    )
