# ---------------------------------------------------
# File Name: get_func.py
# Description: A Pyrogram bot for downloading files from Telegram channels or groups
#              and uploading them back to Telegram.
# Author: Gagan
# GitHub: https://github.com/devgaganin/
# Telegram: https://t.me/team_spy_pro
# YouTube: https://youtube.com/@dev_gagan
# Created: 2025-01-11
# Last Modified: 2025-05-12 # Updated last modified date
# Version: 2.0.5
# License: MIT License
# Improved logic handles
# ---------------------------------------------------

import asyncio
import time
import gc
import os
import re
from typing import Callable
from devgagan import app
from devgagan import sex as gf
from telethon.tl.types import DocumentAttributeVideo, Message
from telethon.sessions import StringSession
import pymongo
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import ChannelBanned, ChannelInvalid, ChannelPrivate, ChatIdInvalid, ChatInvalid
from pyrogram.enums import MessageMediaType, ParseMode
from devgagan.core.func import *
from pyrogram.errors import RPCError
from pyrogram.types import Message
from config import MONGO_DB as MONGODB_CONNECTION_STRING, LOG_GROUP, OWNER_ID, STRING, API_ID, API_HASH
from devgagan.core.mongo.db import set_session, remove_session, get_data
from telethon import TelegramClient, events, Button
from devgagantools import fast_upload

def thumbnail(sender):
    return f'{sender}.jpg' if os.path.exists(f'{sender}.jpg') else None

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
    print("App imported from package.")
else:
    pro = None
    print("STRING is not available. 'app' is set to None.")

async def fetch_upload_method(user_id):
    """Fetch the user's preferred upload method."""
    print(f"DEBUG: Fetching upload method for user {user_id}") # Debug log
    user_data = collection.find_one({"user_id": user_id})
    method = user_data.get("upload_method", "Pyrogram") if user_data else "Pyrogram"
    print(f"DEBUG: Upload method for user {user_id} is {method}") # Debug log
    return method

async def format_caption_to_html(caption: str) -> str:
    # ... (your existing format_caption_to_html function) ...
    caption = re.sub(r"^> (.*)", r"<blockquote>\1</blockquote>", caption, flags=re.MULTILINE)
    caption = re.sub(r"```(.*?)```", r"<pre>\1</pre>", caption, flags=re.DOTALL)
    caption = re.sub(r"`(.*?)`", r"<code>\1</code>", caption)
    caption = re.sub(r"\*\*(.*?)\*\*", r"<b>\1</b>", caption)
    caption = re.sub(r"\*(.*?)\*", r"<b>\1</b>", caption)
    caption = re.sub(r"__(.*?)__", r"<i>\1</i>", caption)
    caption = re.sub(r"_(.*?)_", r"<i>\1</i>", caption)
    caption = re.sub(r"~~(.*?)~~", r"<s>\1</s>", caption)
    caption = re.sub(r"\|\|(.*?)\|\|", r"<details>\1</details>") # Corrected regex for spoiler
    caption = re.sub(r"\[(.*?)\]\((.*?)\)", r'<a href="\2">\1</a>', caption)
    return caption.strip() if caption else None


async def upload_media(sender, target_chat_id, file, caption, edit, topic_id):
    print(f"DEBUG: Starting upload_media for file: {file}") # Debug log
    try:
        upload_method = await fetch_upload_method(sender)  # Fetch the upload method (Pyrogram or Telethon)
        print(f"DEBUG: Upload method selected: {upload_method}") # Debug log
        metadata = video_metadata(file)
        width, height, duration = metadata['width'], metadata['height'], metadata['duration']
        thumb_path = await screenshot(file, duration, sender)
        print(f"DEBUG: Thumbnail path: {thumb_path}") # Debug log

        video_formats = {'mp4', 'mkv', 'avi', 'mov'}
        document_formats = {'pdf', 'docx', 'txt', 'epub'} # Defined but not used in upload logic below
        image_formats = {'jpg', 'png', 'jpeg'}

        # Pyrogram upload
        if upload_method == "Pyrogram":
            print("DEBUG: Using Pyrogram upload method.") # Debug log
            if file.split('.')[-1].lower() in video_formats:
                print(f"DEBUG: Sending video with Pyrogram to {target_chat_id}") # Debug log
                dm = await app.send_video(
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
                    progress_args=("╭─────────────────────╮\n│       **__Pyro Uploader__**\n├─────────────────────", edit, time.time())
                )
                print(f"DEBUG: Video sent. Copying to LOG_GROUP: {LOG_GROUP}") # Debug log
                await dm.copy(LOG_GROUP)
                print("DEBUG: Video copied to LOG_GROUP.") # Debug log

            elif file.split('.')[-1].lower() in image_formats:
                print(f"DEBUG: Sending photo with Pyrogram to {target_chat_id}") # Debug log
                dm = await app.send_photo(
                    chat_id=target_chat_id,
                    photo=file,
                    caption=caption,
                    parse_mode=ParseMode.MARKDOWN,
                    progress=progress_bar,
                    reply_to_message_id=topic_id,
                    progress_args=("╭─────────────────────╮\n│       **__Pyro Uploader__**\n├─────────────────────", edit, time.time())
                )
                print(f"DEBUG: Photo sent. Copying to LOG_GROUP: {LOG_GROUP}") # Debug log
                await dm.copy(LOG_GROUP)
                print("DEBUG: Photo copied to LOG_GROUP.") # Debug log
            else: # Handles documents and other file types not explicitly listed
                print(f"DEBUG: Sending document with Pyrogram to {target_chat_id}") # Debug log
                dm = await app.send_document(
                    chat_id=target_chat_id,
                    document=file,
                    caption=caption,
                    thumb=thumb_path,
                    reply_to_message_id=topic_id,
                    progress=progress_bar,
                    parse_mode=ParseMode.MARKDOWN,
                    progress_args=("╭─────────────────────╮\n│       **__Pyro Uploader__**\n├─────────────────────", edit, time.time())
                )
                print("DEBUG: Document sent. Waiting 2 seconds before copying...") # Debug log
                await asyncio.sleep(2) # Added a small delay as seen in your original code
                print(f"DEBUG: Copying document to LOG_GROUP: {LOG_GROUP}") # Debug log
                await dm.copy(LOG_GROUP)
                print("DEBUG: Document copied to LOG_GROUP.") # Debug log

        # Telethon upload
        elif upload_method == "Telethon":
            print("DEBUG: Using Telethon upload method.") # Debug log
            await edit.delete()
            progress_message = await gf.send_message(sender, "**__Uploading...__**")
            caption = await format_caption_to_html(caption) # Assuming this is for Telethon
            print("DEBUG: Starting fast_upload with Telethon.") # Debug log
            uploaded = await fast_upload(
                gf, file,
                reply=progress_message,
                name=None,
                progress_bar_function=lambda done, total: progress_callback(done, total, sender)
            )
            print(f"DEBUG: fast_upload complete. Uploaded file object: {uploaded}") # Debug log
            await progress_message.delete()

            attributes = [
                DocumentAttributeVideo(
                    duration=duration,
                    w=width,
                    h=height,
                    supports_streaming=True
                )
            ] if file.split('.')[-1].lower() in video_formats else []
            print(f"DEBUG: Sending file with Telethon to {target_chat_id}") # Debug log
            await gf.send_file(
                target_chat_id,
                uploaded,
                caption=caption,
                attributes=attributes,
                reply_to=topic_id,
                thumb=thumb_path
            )
            print(f"DEBUG: File sent with Telethon. Sending copy to LOG_GROUP: {LOG_GROUP}") # Debug log
            await gf.send_file(
                LOG_GROUP,
                uploaded,
                caption=caption,
                attributes=attributes,
                thumb=thumb_path
            )
            print("DEBUG: File copied to LOG_GROUP with Telethon.") # Debug log

    except Exception as e:
        print(f"DEBUG: Exception caught in upload_media: {e}") # Debug log
        # Sending error message to LOG_GROUP here might help if the error happens *before* the copy/send_file
        try:
            await app.send_message(LOG_GROUP, f"**Upload Failed:** {str(e)}\nFile: `{file}`")
            print("DEBUG: Error message sent to LOG_GROUP.") # Debug log
        except Exception as log_e:
            print(f"DEBUG: Failed to send error message to LOG_GROUP: {log_e}") # Debug log
        print(f"Error during media upload: {e}")

    finally:
        print(f"DEBUG: upload_media finally block for file: {file}") # Debug log
        if thumb_path and os.path.exists(thumb_path):
            print(f"DEBUG: Removing thumbnail: {thumb_path}") # Debug log
            os.remove(thumb_path)
        # File removal is handled in get_msg, but adding a check here too might help
        # if file and os.path.exists(file):
        #     print(f"DEBUG: Removing file: {file}") # Debug log
        #     os.remove(file)
        gc.collect()
        print("DEBUG: upload_media finally block finished.") # Debug log


async def get_msg(userbot, sender, edit_id, msg_link, i, message):
    print(f"DEBUG: Starting get_msg for link: {msg_link}") # Debug log
    try:
        # Sanitize the message link
        msg_link = msg_link.split("?single")[0]
        chat, msg_id = None, None
        saved_channel_ids = load_saved_channel_ids()
        size_limit = 2 * 1024 * 1024 * 1024  # 1.99 GB size limit
        file = ''
        edit = ''
        print(f"DEBUG: Processed link: {msg_link}") # Debug log

        # Extract chat and message ID for valid Telegram links
        if 't.me/c/' in msg_link or 't.me/b/' in msg_link:
            print("DEBUG: Detected t.me/c/ or t.me/b/ link.") # Debug log
            parts = msg_link.split("/")
            if 't.me/b/' in msg_link:
                chat = parts[-2]
                msg_id = int(parts[-1]) + i # fixed bot problem
            else:
                chat = int('-100' + parts[parts.index('c') + 1])
                msg_id = int(parts[-1]) + i

            print(f"DEBUG: Extracted chat: {chat}, msg_id: {msg_id}") # Debug log

            if chat in saved_channel_ids:
                print(f"DEBUG: Chat {chat} is in saved_channel_ids. Aborting.") # Debug log
                await app.edit_message_text(
                    message.chat.id, edit_id,
                    "Sorry! This channel is protected by **Admin**."
                )
                return

        elif '/s/' in msg_link: # fixed story typo
            print("DEBUG: Detected story link.") # Debug log
            edit = await app.edit_message_text(sender, edit_id, "Story Link Detected...")
            if userbot is None:
                print("DEBUG: userbot is None for story link. Aborting.") # Debug log
                await edit.edit("Login in bot save stories...")
                return
            parts = msg_link.split("/")
            chat = parts[3]

            if chat.isdigit():  # this is for channel stories
                chat = f"-100{chat}"

            msg_id = int(parts[-1])
            print(f"DEBUG: Extracted story chat: {chat}, msg_id: {msg_id}") # Debug log
            await download_user_stories(userbot, chat, msg_id, edit, sender)
            await edit.delete(2)
            print("DEBUG: Story processing finished.") # Debug log
            return

        else:
            print("DEBUG: Detected public link.") # Debug log
            edit = await app.edit_message_text(sender, edit_id, "Public link detected...")
            chat = msg_link.split("t.me/")[1].split("/")[0]
            msg_id = int(msg_link.split("/")[-1])
            print(f"DEBUG: Extracted public chat: {chat}, msg_id: {msg_id}") # Debug log
            await copy_message_with_chat_id(app, userbot, sender, chat, msg_id, edit)
            await edit.delete(2)
            print("DEBUG: Public link processing finished.") # Debug log
            return # Exit after handling public link

        # Fetch the target message (only for t.me/c/ or t.me/b/ links now)
        print(f"DEBUG: Fetching message {msg_id} from chat {chat} using userbot.") # Debug log
        msg = await userbot.get_messages(chat, msg_id)
        if not msg or msg.service or msg.empty:
            print("DEBUG: Message not found, is service message, or is empty. Aborting.") # Debug log
            # Consider editing the message to inform the user
            await app.edit_message_text(sender, edit_id, "Could not fetch the message.")
            return

        target_chat_id = user_chat_ids.get(message.chat.id, message.chat.id)
        topic_id = None
        if '/' in str(target_chat_id):
            target_chat_id, topic_id = map(int, str(target_chat_id).split('/', 1)) # Ensure target_chat_id is string
        print(f"DEBUG: Target chat ID: {target_chat_id}, Topic ID: {topic_id}") # Debug log


        # Handle different message types
        if msg.media == MessageMediaType.WEB_PAGE_PREVIEW:
            print("DEBUG: Detected WEB_PAGE_PREVIEW.") # Debug log
            await clone_message(app, msg, target_chat_id, topic_id, edit_id, LOG_GROUP)
            print("DEBUG: WEB_PAGE_PREVIEW processing finished.") # Debug log
            return

        if msg.text:
            print("DEBUG: Detected text message.") # Debug log
            await clone_text_message(app, msg, target_chat_id, topic_id, edit_id, LOG_GROUP)
            print("DEBUG: Text message processing finished.") # Debug log
            return

        if msg.sticker:
            print("DEBUG: Detected sticker.") # Debug log
            await handle_sticker(app, msg, target_chat_id, topic_id, edit_id, LOG_GROUP)
            print("DEBUG: Sticker processing finished.") # Debug log
            return


        # Handle file media (photo, document, video, audio, voice)
        print("DEBUG: Detected file media (photo, document, video, audio, voice).") # Debug log
        file_size = get_message_file_size(msg)
        print(f"DEBUG: File size: {file_size}") # Debug log

        size_limit_pro = 4 * 1024 * 1024 * 1024 # Assuming 4GB limit for pro
        if file_size and file_size > size_limit_pro and pro is None:
             print(f"DEBUG: File size {file_size} exceeds {size_limit_pro} and pro is None. Aborting.") # Debug log
             await app.edit_message_text(sender, edit_id, "**❌ 4GB Uploader not found**")
             return

        file_name = await get_media_filename(msg)
        edit = await app.edit_message_text(sender, edit_id, "**Downloading...**")
        print(f"DEBUG: Starting download for file: {file_name}") # Debug log

        # Download media
        file = await userbot.download_media(
            msg,
            file_name=file_name,
            progress=progress_bar,
            progress_args=("╭─────────────────────╮\n│       **__Downloading__...**\n├─────────────────────", edit, time.time())
        )
        print(f"DEBUG: Download complete. File saved to: {file}") # Debug log

        caption = await get_final_caption(msg, sender)
        print(f"DEBUG: Final caption: {caption}") # Debug log

        # Rename file
        file = await rename_file(file, sender)
        print(f"DEBUG: File renamed to: {file}") # Debug log

        # Handle specific media types after download
        if msg.audio:
            print(f"DEBUG: Sending audio to {target_chat_id}") # Debug log
            result = await app.send_audio(target_chat_id, file, caption=caption, reply_to_message_id=topic_id)
            print("DEBUG: Audio sent. Copying to LOG_GROUP.") # Debug log
            await result.copy(LOG_GROUP)
            print("DEBUG: Audio copied to LOG_GROUP.") # Debug log
            await edit.delete(2)
            print("DEBUG: Audio processing finished.") # Debug log
            return

        if msg.voice:
            print(f"DEBUG: Sending voice to {target_chat_id}") # Debug log
            result = await app.send_voice(target_chat_id, file, reply_to_message_id=topic_id)
            print("DEBUG: Voice sent. Copying to LOG_GROUP.") # Debug log
            await result.copy(LOG_GROUP)
            print("DEBUG: Voice copied to LOG_GROUP.") # Debug log
            await edit.delete(2)
            print("DEBUG: Voice processing finished.") # Debug log
            return

        if msg.photo:
            print(f"DEBUG: Sending photo to {target_chat_id}") # Debug log
            result = await app.send_photo(target_chat_id, file, caption=caption, reply_to_message_id=topic_id)
            print("DEBUG: Photo sent. Copying to LOG_GROUP.") # Debug log
            await result.copy(LOG_GROUP)
            print("DEBUG: Photo copied to LOG_GROUP.") # Debug log
            await edit.delete(2)
            print("DEBUG: Photo processing finished.") # Debug log
            return

        # Handle video and document uploads (potentially large files)
        print("DEBUG: Handling video or document upload.") # Debug log
        if file_size > size_limit: # Using the 1.99 GB limit defined earlier
             print(f"DEBUG: File size {file_size} exceeds {size_limit}. Handling as large file.") # Debug log
             await handle_large_file(file, sender, edit, caption)
             print("DEBUG: handle_large_file finished.") # Debug log
        else:
             print(f"DEBUG: File size {file_size} is within limit. Calling upload_media.") # Debug log
             await upload_media(sender, target_chat_id, file, caption, edit, topic_id)
             print("DEBUG: upload_media finished.") # Debug log


    except (ChannelBanned, ChannelInvalid, ChannelPrivate, ChatIdInvalid, ChatInvalid) as e:
        print(f"DEBUG: Caught Telegram Channel/Chat exception: {e}") # Debug log
        await app.edit_message_text(sender, edit_id, "Have you joined the channel?")
    except Exception as e:
        print(f"DEBUG: Caught general exception in get_msg: {e}") # Debug log
        # await app.edit_message_text(sender, edit_id, f"Failed to save: `{msg_link}`\n\nError: {str(e)}")
        print(f"Error: {e}")
        # Consider sending a more specific error message to the user
        # await app.send_message(sender, f"An error occurred while processing your link: {e}")
    finally:
        print(f"DEBUG: get_msg finally block for link: {msg_link}") # Debug log
        # Clean up the downloaded file
        if file and os.path.exists(file):
            print(f"DEBUG: Removing downloaded file: {file}") # Debug log
            os.remove(file)
        if edit:
            # Ensure edit message is deleted even if there was an error
            try:
                await edit.delete(2)
                print("DEBUG: Edit message deleted in finally block.") # Debug log
            except Exception as delete_e:
                print(f"DEBUG: Failed to delete edit message in finally block: {delete_e}") # Debug log
        gc.collect()
        print("DEBUG: get_msg finally block finished.") # Debug log


async def clone_message(app, msg, target_chat_id, topic_id, edit_id, log_group):
    print("DEBUG: Cloning message (web page preview).") # Debug log
    edit = await app.edit_message_text(target_chat_id, edit_id, "Cloning...")
    devgaganin = await app.send_message(target_chat_id, msg.text.markdown, reply_to_message_id=topic_id)
    print(f"DEBUG: Cloned message sent. Copying to log group: {log_group}") # Debug log
    await devgaganin.copy(log_group)
    print("DEBUG: Cloned message copied to log group.") # Debug log
    await edit.delete()
    print("DEBUG: clone_message finished.") # Debug log

async def clone_text_message(app, msg, target_chat_id, topic_id, edit_id, log_group):
    print("DEBUG: Cloning text message.") # Debug log
    edit = await app.edit_message_text(target_chat_id, edit_id, "Cloning text message...")
    devgaganin = await app.send_message(target_chat_id, msg.text.markdown, reply_to_message_id=topic_id)
    print(f"DEBUG: Cloned text message sent. Copying to log group: {log_group}") # Debug log
    await devgaganin.copy(log_group)
    print("DEBUG: Cloned text message copied to log group.") # Debug log
    await edit.delete()
    print("DEBUG: clone_text_message finished.") # Debug log


async def handle_sticker(app, msg, target_chat_id, topic_id, edit_id, log_group):
    print("DEBUG: Handling sticker.") # Debug log
    edit = await app.edit_message_text(target_chat_id, edit_id, "Handling sticker...")
    result = await app.send_sticker(target_chat_id, msg.sticker.file_id, reply_to_message_id=topic_id)
    print(f"DEBUG: Sticker sent. Copying to log group: {log_group}") # Debug log
    await result.copy(log_group)
    print("DEBUG: Sticker copied to log group.") # Debug log
    await edit.delete()
    print("DEBUG: handle_sticker finished.") # Debug log


async def get_media_filename(msg):
    # ... (your existing get_media_filename function) ...
    if msg.document:
        return msg.document.file_name
    if msg.video:
        return msg.video.file_name if msg.video.file_name else "temp.mp4"
    if msg.photo:
        return "temp.jpg"
    return "unknown_file"

def get_message_file_size(msg):
    # ... (your existing get_message_file_size function) ...
    if msg.document:
        return msg.document.file_size
    if msg.photo:
        return msg.photo.file_size
    if msg.video:
        return msg.video.file_size
    return 1 # Return a small size if no media with size is found

async def get_final_caption(msg, sender):
    # ... (your existing get_final_caption function) ...
    # Handle caption based on the upload method
    if msg.caption:
        original_caption = msg.caption.markdown
    else:
        original_caption = ""

    custom_caption = get_user_caption_preference(sender)
    final_caption = f"{original_caption}\n\n{custom_caption}" if custom_caption else original_caption
    replacements = load_replacement_words(sender)
    for word, replace_word in replacements.items():
        final_caption = final_caption.replace(word, replace_word)

    return final_caption if final_caption else None


async def download_user_stories(userbot, chat_id, msg_id, edit, sender):
    # ... (your existing download_user_stories function) ...
    try:
        print(f"DEBUG: Downloading story for chat {chat_id}, msg_id {msg_id}") # Debug log
        # Fetch the story using the provided chat ID and message ID
        story = await userbot.get_stories(chat_id, msg_id)
        if not story:
            print("DEBUG: No story available.") # Debug log
            await edit.edit("No story available for this user.")
            return
        if not story.media:
            print("DEBUG: Story has no media.") # Debug log
            await edit.edit("The story doesn't contain any media.")
            return
        await edit.edit("Downloading Story...")
        file_path = await userbot.download_media(story)
        print(f"DEBUG: Story downloaded: {file_path}") # Debug log
        # Send the downloaded story based on its type
        if story.media:
            await edit.edit("Uploading Story...")
            if story.media == MessageMediaType.VIDEO:
                print(f"DEBUG: Sending story video to {sender}") # Debug log
                await app.send_video(sender, file_path)
            elif story.media == MessageMediaType.DOCUMENT:
                 print(f"DEBUG: Sending story document to {sender}") # Debug log
                 await app.send_document(sender, file_path)
            elif story.media == MessageMediaType.PHOTO:
                 print(f"DEBUG: Sending story photo to {sender}") # Debug log
                 await app.send_photo(sender, file_path)
        if file_path and os.path.exists(file_path):
            print(f"DEBUG: Removing story file: {file_path}") # Debug log
            os.remove(file_path)
        await edit.edit("Story processed successfully.")
        print("DEBUG: download_user_stories finished.") # Debug log
    except RPCError as e:
        print(f"DEBUG: RPCError in download_user_stories: {e}") # Debug log
        print(f"Failed to fetch story: {e}")
        await edit.edit(f"Error: {e}")
    except Exception as e:
        print(f"DEBUG: General exception in download_user_stories: {e}") # Debug log
        print(f"Error: {e}")
        await edit.edit(f"An unexpected error occurred: {e}")


async def copy_message_with_chat_id(app, userbot, sender, chat_id, message_id, edit):
    print(f"DEBUG: Starting copy_message_with_chat_id for chat {chat_id}, msg_id {message_id}") # Debug log
    target_chat_id = user_chat_ids.get(sender, sender)
    file = None
    result = None
    size_limit = 2 * 1024 * 1024 * 1024  # 2 GB size limit
    print(f"DEBUG: Target chat ID for copy: {target_chat_id}") # Debug log

    try:
        print(f"DEBUG: Attempting app.get_messages for chat {chat_id}, msg_id {message_id}") # Debug log
        msg = await app.get_messages(chat_id, message_id)
        print(f"DEBUG: Message fetched: {msg}") # Debug log

        custom_caption = get_user_caption_preference(sender)
        final_caption = format_caption(msg.caption or '', sender, custom_caption)
        print(f"DEBUG: Final caption for copy: {final_caption}") # Debug log

        # Parse target_chat_id and topic_id
        topic_id = None
        if '/' in str(target_chat_id):
            target_chat_id, topic_id = map(int, str(target_chat_id).split('/', 1))
        print(f"DEBUG: Target chat ID (parsed): {target_chat_id}, Topic ID (parsed): {topic_id}") # Debug log

        # Handle different media types
        if msg.media:
            print("DEBUG: Detected media in copy_message_with_chat_id. Calling send_media_message.") # Debug log
            result = await send_media_message(app, target_chat_id, msg, final_caption, topic_id)
            print("DEBUG: send_media_message finished. Returning.") # Debug log
            return # Exit after handling media

        elif msg.text:
            print("DEBUG: Detected text message in copy_message_with_chat_id. Calling app.copy_message.") # Debug log
            result = await app.copy_message(target_chat_id, chat_id, message_id, reply_to_message_id=topic_id)
            print("DEBUG: app.copy_message finished. Returning.") # Debug log
            return # Exit after handling text

        # Fallback if result is None (This block might be for handling public group usernames that Pyrogram couldn't resolve directly)
        if result is None:
             print("DEBUG: Result is None after initial copy/send attempts. Trying Telethon fallback.") # Debug log
             await edit.edit("Trying if it is a group...")
             # This line assumes chat_id is a username string if result is None
             print(f"DEBUG: Attempting userbot.get_chat for username: @{chat_id}") # Debug log
             chat_obj = await userbot.get_chat(f"@{chat_id}")
             chat_id_int = chat_obj.id # Get integer ID from username
             print(f"DEBUG: Resolved username @{chat_id} to integer ID: {chat_id_int}") # Debug log

             print(f"DEBUG: Attempting userbot.get_messages for chat {chat_id_int}, msg_id {message_id}") # Debug log
             msg = await userbot.get_messages(chat_id_int, message_id)
             print(f"DEBUG: Message fetched via Telethon: {msg}") # Debug log

             if not msg or msg.service or msg.empty:
                 print("DEBUG: Message not found, is service message, or is empty via Telethon. Aborting fallback.") # Debug log
                 return

             final_caption = format_caption(msg.caption.markdown if msg.caption else "", sender, custom_caption)
             print(f"DEBUG: Final caption for Telethon fallback: {final_caption}") # Debug log

             edit = await app.edit_message_text(sender, edit.id, "**Downloading (Telethon Fallback)...**") # Update edit message
             print(f"DEBUG: Starting download via Telethon fallback for file: {await get_media_filename(msg)}") # Debug log
             file = await userbot.download_media(
                 msg,
                 progress=progress_bar,
                 progress_args=("╭─────────────────────╮\n│       **__Downloading__...**\n├─────────────────────", edit, time.time())
             )
             print(f"DEBUG: Download complete via Telethon fallback. File: {file}") # Debug log
             file = await rename_file(file, sender)
             print(f"DEBUG: File renamed via Telethon fallback: {file}") # Debug log


             if msg.photo:
                 print(f"DEBUG: Sending photo via Pyrogram after Telethon download to {target_chat_id}") # Debug log
                 result = await app.send_photo(target_chat_id, file, caption=final_caption, reply_to_message_id=topic_id)
                 print("DEBUG: Photo sent via Pyrogram.") # Debug log
             elif msg.video or msg.document:
                 print(f"DEBUG: Handling video/document via Telethon fallback.") # Debug log
                 if await is_file_size_exceeding(file, size_limit):
                     print(f"DEBUG: File size {os.path.getsize(file)} exceeds {size_limit}. Handling as large file via fallback.") # Debug log
                     await handle_large_file(file, sender, edit, final_caption)
                     print("DEBUG: handle_large_file finished via fallback.") # Debug log
                     return # Exit after handling large file
                 print(f"DEBUG: File size {os.path.getsize(file)} is within limit. Calling upload_media via fallback.") # Debug log
                 await upload_media(sender, target_chat_id, file, final_caption, edit, topic_id)
                 print("DEBUG: upload_media finished via fallback.") # Debug log
                 return # Exit after upload_media
             elif msg.audio:
                 print(f"DEBUG: Sending audio via Pyrogram after Telethon download to {target_chat_id}") # Debug log
                 result = await app.send_audio(target_chat_id, file, caption=final_caption, reply_to_message_id=topic_id)
                 print("DEBUG: Audio sent via Pyrogram.") # Debug log
             elif msg.voice:
                 print(f"DEBUG: Sending voice via Pyrogram after Telethon download to {target_chat_id}") # Debug log
                 result = await app.send_voice(target_chat_id, file, reply_to_message_id=topic_id)
                 print("DEBUG: Voice sent via Pyrogram.") # Debug log
             elif msg.sticker:
                 print(f"DEBUG: Sending sticker via Pyrogram after Telethon download to {target_chat_id}") # Debug log
                 result = await app.send_sticker(target_chat_id, msg.sticker.file_id, reply_to_message_id=topic_id)
                 print("DEBUG: Sticker sent via Pyrogram.") # Debug log
             else:
                 print("DEBUG: Unsupported media type via Telethon fallback.") # Debug log
                 await edit.edit("Unsupported media type.")

             # After successful send/upload in fallback, copy to LOG_GROUP
             if result: # Check if a result was obtained from sending
                 print(f"DEBUG: Copying result from fallback send to LOG_GROUP: {LOG_GROUP}") # Debug log
                 await result.copy(LOG_GROUP)
                 print("DEBUG: Result copied to LOG_GROUP.") # Debug log


    except Exception as e:
        print(f"DEBUG: Caught exception in copy_message_with_chat_id: {e}") # Debug log
        print(f"Error : {e}")
        # error_message = f"Error occurred while processing message: {str(e)}"
        # await app.send_message(sender, error_message)
        # await app.send_message(sender, f"Make Bot admin in your Channel - {target_chat_id} and restart the process after /cancel")
        # Consider sending a more specific error message to the user and logging to LOG_GROUP here as well
        try:
            await app.send_message(LOG_GROUP, f"**Processing Failed:** {str(e)}\nLink: `{msg_link}`")
            print("DEBUG: Error message sent to LOG_GROUP from copy_message_with_chat_id exception.") # Debug log
        except Exception as log_e:
            print(f"DEBUG: Failed to send error message to LOG_GROUP from copy_message_with_chat_id exception: {log_e}") # Debug log


    finally:
        print(f"DEBUG: copy_message_with_chat_id finally block for link: {msg_link}") # Debug log
        # Clean up
        if file and os.path.exists(file):
            print(f"DEBUG: Removing downloaded file in copy_message_with_chat_id finally: {file}") # Debug log
            os.remove(file)
        if edit:
             # Ensure edit message is deleted
             try:
                 await edit.delete(2)
                 print("DEBUG: Edit message deleted in copy_message_with_chat_id finally.") # Debug log
             except Exception as delete_e:
                 print(f"DEBUG: Failed to delete edit message in copy_message_with_chat_id finally: {delete_e}") # Debug log
        gc.collect()
        print("DEBUG: copy_message_with_chat_id finally block finished.") # Debug log


async def send_media_message(app, target_chat_id, msg, caption, topic_id):
    print(f"DEBUG: Starting send_media_message to {target_chat_id}") # Debug log
    try:
        if msg.video:
            print("DEBUG: Sending video by file_id.") # Debug log
            return await app.send_video(target_chat_id, msg.video.file_id, caption=caption, reply_to_message_id=topic_id)
        if msg.document:
            print("DEBUG: Sending document by file_id.") # Debug log
            return await app.send_document(target_chat_id, msg.document.file_id, caption=caption, reply_to_message_id=topic_id)
        if msg.photo:
            print("DEBUG: Sending photo by file_id.") # Debug log
            return await app.send_photo(target_chat_id, msg.photo.file_id, caption=caption, reply_to_message_id=topic_id)
    except Exception as e:
        print(f"DEBUG: Error while sending media by file_id in send_media_message: {e}") # Debug log
        print(f"Error while sending media: {e}")

    # Fallback to copy_message in case of any exceptions
    print(f"DEBUG: Falling back to copy_message to {target_chat_id}") # Debug log
    return await app.copy_message(target_chat_id, msg.chat.id, msg.id, reply_to_message_id=topic_id)


def format_caption(original_caption, sender, custom_caption):
    # ... (your existing format_caption function) ...
    delete_words = load_delete_words(sender)
    replacements = load_replacement_words(sender)

    # Remove and replace words in the caption
    for word in delete_words:
        original_caption = original_caption.replace(word, '  ') # Use two spaces for clarity
    for word, replace_word in replacements.items():
        original_caption = original_caption.replace(word, replace_word)

    # Append custom caption if available
    return f"{original_caption}\n\n__**{custom_caption}**__" if custom_caption else original_caption


# ------------------------ Button Mode Editz FOR SETTINGS ----------------------------

# Define a dictionary to store user chat IDs
user_chat_ids = {}

def load_user_data(user_id, key, default_value=None):
    # ... (your existing load_user_data function) ...
    try:
        user_data = collection.find_one({"_id": user_id})
        return user_data.get(key, default_value) if user_data else default_value
    except Exception as e:
        print(f"Error loading {key}: {e}")
        return default_value

def load_saved_channel_ids():
    # ... (your existing load_saved_channel_ids function) ...
    saved_channel_ids = set()
    try:
        # Retrieve channel IDs from MongoDB collection
        for channel_doc in collection.find({"channel_id": {"$exists": True}}):
            saved_channel_ids.add(channel_doc["channel_id"])
    except Exception as e:
        print(f"Error loading saved channel IDs: {e}")
    return saved_channel_ids

def save_user_data(user_id, key, value):
    # ... (your existing save_user_data function) ...
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
    # ... (your existing send_settings_message function) ...
    # Define the rest of the buttons
    buttons = [
        [Button.inline("Set Chat ID", b'setchat'), Button.inline("Set Rename Tag", b'setrename')],
        [Button.inline("Caption", b'setcaption'), Button.inline("Replace Words", b'setreplacement')],
        [Button.inline("Remove Words", b'delete'), Button.inline("Reset", b'reset')],
        [Button.inline("Session Login", b'addsession'), Button.inline("Logout", b'logout')],
        [Button.inline("Set Thumbnail", b'setthumb'), Button.inline("Remove Thumbnail", b'remthumb')],
        [Button.inline("PDF Wtmrk", b'pdfwt'), Button.inline("Video Wtmrk", b'watermark')],
        [Button.inline("Upload Method", b'uploadmethod')],  # Include the dynamic Fast DL button
        [Button.url("Report Errors", "https://t.me/Contact_xbot")]
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
    # ... (your existing callback_query_handler function) ...
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
            [Button.inline(f"SpyLib v1 ⚡{telethon_check}", b'telethon')]
        ]
        await event.edit("Choose your preferred upload method:\n\n__**Note:** **SpyLib ⚡**, built on Telethon(base), by Admin still in beta.__", buttons=buttons)

    elif event.data == b'pyrogram':
        save_user_upload_method(user_id, "Pyrogram")
        await event.edit("Upload method set to **Pyrogram** ✅")

    elif event.data == b'telethon':
        save_user_upload_method(user_id, "Telethon")
        await event.edit("Upload method set to **SpyLib ⚡\n\nThanks for choosing this library as it will help me to analyze the error raise issues on github.** ✅")

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
    # ... (your existing save_thumbnail function) ...
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
    # ... (your existing save_user_upload_method function) ...
    # Save or update the user's preferred upload method
    collection.update_one(
        {'user_id': user_id},  # Query
        {'$set': {'upload_method': method}},  # Update
        upsert=True  # Create a new document if one doesn't exist
    )

@gf.on(events.NewMessage)
async def handle_user_input(event):
    # ... (your existing handle_user_input function) ...
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
    # ... (your existing lock_command_handler function) ...
    if event.sender_id not in OWNER_ID:
        return await event.respond("You are not authorized to use this command.")

    # Extract the channel ID
