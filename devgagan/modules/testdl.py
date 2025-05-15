
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
import random
import requests
import string
import aiohttp
from devgagan import app
from devgagan.core.func import *
from datetime import datetime, timedelta
from motor.motor_asyncio import AsyncIOMotorClient
from config import MONGO_DB, WEBSITE_URL, AD_API, LOG_GROUP  



import time
import math
import asyncio
import os
from telethon import TelegramClient, events, types
from telethon.errors import FloodWaitError, TakeoutInitError, RPCError
from datetime import datetime, timedelta

# --- Configuration ---
# The chat ID for a private channel link t.me/c/CHANNEL_ID/MESSAGE_ID is -100 * CHANNEL_ID
TARGET_CHANNEL_ID_RAW = 2587931495
TARGET_CHAT_ID = -100 * TARGET_CHANNEL_ID_RAW
TARGET_MESSAGE_ID = 2402 # The message ID of the specific file you want to download

DOWNLOAD_DIR = 'downloads' # Directory to save downloaded files
EDIT_INTERVAL = 5 # Seconds between message edits to show progress (to avoid FloodWaits)

# Dictionary to store download progress data per progress message ID
# Stores: {'progress_msg_id': {'start_time': float, 'last_edited_time': float, 'last_bytes_sent': int, 'total_size': int}}
DOWNLOAD_PROGRESS_DATA = {}

# --- Helper function to format bytes into human-readable string ---
def humanbytes(size):
    if size is None: # Handle None or unknown size
        return "N/A"
    size = float(size)
    power = 1024
    n = 0
    iec_p = {0: '', 1: 'K', 2: 'M', 3: 'G', 4: 'T', 5: 'P', 6: 'E', 7: 'Z', 8: 'Y'}
    while size >= power:
        size /= power
        n += 1
    return f"{size:.2f} {iec_p[n]}B"

# --- Progress Callback Function ---
async def progress_callback(current, total, client, progress_message_id, chat_id):
    # Retrieve progress data for this download
    progress_data = DOWNLOAD_PROGRESS_DATA.get(progress_message_id)
    if not progress_data:
        # This can happen if the download finished very quickly before the first edit
        return

    now = time.time()
    start_time = progress_data['start_time']
    last_edited_time = progress_data['last_edited_time']
    last_bytes_sent = progress_data['last_bytes_sent']
    total_size = progress_data['total_size'] # Use stored total size

    # Calculate speed (bytes per second) based on bytes transferred since the last edit
    time_since_last_edit = now - last_edited_time
    bytes_since_last_edit = current - last_bytes_sent

    # Calculate instantaneous speed only if a meaningful time has passed
    if time_since_last_edit > 0:
         speed_bytes_per_second = bytes_since_last_edit / time_since_last_edit
    else:
         speed_bytes_per_second = 0 # Avoid division by zero


    # Calculate ETA (estimated time of arrival) using the calculated speed
    bytes_remaining = total_size - current
    if speed_bytes_per_second > 0:
        eta_seconds = bytes_remaining / speed_bytes_per_second
        # Format ETA nicely as HH:MM:SS or MM:SS
        eta_formatted = str(timedelta(seconds=int(eta_seconds)))
    else:
        eta_formatted = "Calculating..."

    # Avoid division by zero for percentage if total is 0
    percentage = (current / total_size) * 100 if total_size > 0 else 0

    # Prepare the progress text
    progress_text = (
        f"📥 Downloading: {percentage:.2f}%\n"
        f"📦 Size: {humanbytes(current)} / {humanbytes(total_size)}\n"
        f"⚡ Speed: {humanbytes(speed_bytes_per_second)}/s\n"
        f"⏳ ETA: {eta_formatted}"
    )

    # Update the message in Telegram only if a certain interval has passed or at start/end
    if now - last_edited_time > EDIT_INTERVAL or percentage == 0 or percentage >= 99.5:
        try:
            await client.edit_message(chat_id, progress_message_id, progress_text)
            # Update progress data after successful edit
            DOWNLOAD_PROGRESS_DATA[progress_message_id]['last_edited_time'] = now
            DOWNLOAD_PROGRESS_DATA[progress_message_id]['last_bytes_sent'] = current

        except FloodWaitError as e:
            print(f"FloodWait during edit of message {progress_message_id} in chat {chat_id}: Need to wait {e.seconds}s")
            # Wait and try editing again. Update time if successful.
            await asyncio.sleep(e.seconds)
            try:
                 await client.edit_message(chat_id, progress_message_id, progress_text)
                 DOWNLOAD_PROGRESS_DATA[progress_message_id]['last_edited_time'] = time.time()
                 DOWNLOAD_PROGRESS_DATA[progress_message_id]['last_bytes_sent'] = current
            except Exception as edit_error:
                 print(f"Error editing message {progress_message_id} after FloodWait: {edit_error}")

        except Exception as edit_error:
            # Catch other potential errors during message edit (e.g., message deleted)
            print(f"Error editing message {progress_message_id} in chat {chat_id}: {edit_error}")


# --- Telethon Command Handler for /testdonl ---

@app.on_message(filters.command("testdonl"))
async def test_download_handler(event):
    user_chat_id = event.chat_id

    # Send an initial message to indicate download started
    # This message will be edited to show progress
    status_message = await event.reply("🔍 Fetching file details...")
    progress_message_id = status_message.id

    # Initialize progress data for this download in the global dictionary
    # We'll update start_time and total_size once the message is fetched
    DOWNLOAD_PROGRESS_DATA[progress_message_id] = {
        'start_time': time.time(), # Initial timestamp
        'last_edited_time': time.time(),
        'last_bytes_sent': 0,
        'total_size': 0 # Placeholder
    }

    fetch_start_time = time.time() # Time before fetching the message

    try:
        # Get the message containing the media from the target chat
        # Use get_messages with chat ID and a list of message IDs
        messages = await client.get_messages(TARGET_CHAT_ID, ids=[TARGET_MESSAGE_ID])

        if not messages or not messages[0]:
            await client.edit_message(user_chat_id, progress_message_id, "❌ Could not find the specified message.")
            return

        target_message = messages[0]

        if not target_message.media:
             await client.edit_message(user_chat_id, progress_message_id, "❌ The specified message does not contain downloadable media.")
             return

        # --- Determine File Size ---
        file_size = 0
        # Check different media types for the size attribute
        if hasattr(target_message.media, 'document') and hasattr(target_message.media.document, 'size'):
            file_size = target_message.media.document.size
        elif hasattr(target_message.media, 'photo') and isinstance(target_message.media.photo, types.Photo):
             # Photos might have multiple sizes, use the largest one's size for total
             largest_photo_size = max(target_message.media.photo.sizes, key=lambda s: getattr(s, 'size', 0) or 0)
             file_size = getattr(largest_photo_size, 'size', 0) or 0
        elif hasattr(target_message.media, 'video') and hasattr(target_message.media.video, 'size'):
            file_size = target_message.media.video.size
        elif hasattr(target_message.media, 'audio') and hasattr(target_message.media.audio, 'size'):
            file_size = target_message.media.audio.size
        elif hasattr(target_message.media, 'voice') and hasattr(target_message.media.voice, 'size'):
            file_size = target_message.media.voice.size
        elif hasattr(target_message.media, 'video_note') and hasattr(target_message.media.video_note, 'size'):
             file_size = target_message.media.video_note.size

        # Store the determined total size in the progress data
        DOWNLOAD_PROGRESS_DATA[progress_message_id]['total_size'] = file_size

        # --- Determine File Name and Save Path ---
        # Try to use the original filename from DocumentAttributeFilename
        # Fallback to a generic name based on message ID and media type
        file_name_part = f"media_{target_message.id}" # Default base name
        if hasattr(target_message.media, 'document') and target_message.media.document.attributes:
             for attr in target_message.media.document.attributes:
                 if isinstance(attr, types.DocumentAttributeFilename):
                     file_name_part = attr.file_name # Use original filename if available
                     break
        elif hasattr(target_message.media, 'photo'):
             file_name_part = f"photo_{target_message.id}.jpg" # Common extension for photos
        elif hasattr(target_message.media, 'video'):
             file_name_part = f"video_{target_message.id}.mp4" # Common extension for videos
        elif hasattr(target_message.media, 'audio'):
             file_name_part = f"audio_{target_message.id}.mp3" # Common extension for audio
        elif hasattr(target_message.media, 'voice'):
             file_name_part = f"voice_{target_message.id}.ogg" # Common extension for voice notes
        elif hasattr(target_message.media, 'video_note'):
             file_name_part = f"video_note_{target_message.id}.mp4" # Common extension for video notes

        # Ensure the download directory exists
        if not os.path.exists(DOWNLOAD_DIR):
            os.makedirs(DOWNLOAD_DIR)

        # Construct the full save path
        save_path = os.path.join(DOWNLOAD_DIR, file_name_part)

        # Add logic to avoid overwriting existing files (e.g., add a counter: file.ext, file_1.ext, file_2.ext...)
        base, ext = os.path.splitext(save_path)
        count = 1
        while os.path.exists(save_path):
             save_path = f"{base}_{count}{ext}"
             count += 1


        # --- Perform the download with the progress callback ---
        # Update the start time right before the download begins
        start_time = time.time()
        DOWNLOAD_PROGRESS_DATA[progress_message_id]['start_time'] = start_time
        DOWNLOAD_PROGRESS_DATA[progress_message_id]['last_edited_time'] = start_time # Reset last edit time

        downloaded_path = await client.download_media(
            target_message, # Pass the message object to download media from
            file=save_path, # Specify the path to save the file
            progress_callback=progress_callback, # Set the progress callback function
            # Pass extra args to the callback: client, progress_message_id, chat_id
            progress_args=(client, progress_message_id, user_chat_id)
        )

        end_time = time.time()
        total_elapsed_time = end_time - start_time

        if downloaded_path:
            final_message = (
                "✅ Download completed successfully!\n"
                f"💾 Saved to: `{downloaded_path}`\n" # Use markdown code block for path
                f"⏱️ Total time: {total_elapsed_time:.2f} seconds"
            )
        else:
            final_message = "❌ Download failed or was cancelled." # download_media returns None on failure/cancel

        # Ensure the final message is sent even if the download was cancelled after some progress
        await client.edit_message(user_chat_id, progress_message_id, final_message, parse_mode='md') # Use markdown

    except FloodWaitError as e:
        error_msg = f"⏳ FloodWait Error: Please try again in {e.seconds} seconds."
        await client.edit_message(user_chat_id, progress_message_id, error_msg)
        print(f"FloodWait caught: {e}")
    except TakeoutInitError:
         # This error can occur if trying to access private chat history without takeout initialized
         error_msg = "🔒 Access Error: Cannot access this chat history. Ensure the account is a member or configured correctly."
         await client.edit_message(user_chat_id, progress_message_id, error_msg)
         print(f"TakeoutInitError caught.")
    except RPCError as e:
        # Catch other Telegram API errors
        error_msg = f"❌ Telegram API Error: {e.code} - {e.text}"
        await client.edit_message(user_chat_id, progress_message_id, error_msg)
        print(f"RPCError caught: {e}")
    except Exception as e:
        # Catch any other unexpected errors
        error_msg = f"⚠️ An unexpected error occurred: {type(e).__name__} - {e}"
        print(f"Exception details: {type(e).__name__}: {e}") # Log the full exception details
        await client.edit_message(user_chat_id, progress_message_id, error_msg)

    finally:
        # Clean up the progress data for this download regardless of success or failure
        if progress_message_id in DOWNLOAD_PROGRESS_DATA:
             del DOWNLOAD_PROGRESS_DATA[progress_message_id]

# --- How to integrate this into your bot ---
# You need to add this event handler to your Telethon client instance.
# If you have a main file where you define and start your client,
# add this handler using client.add_event_handler or the decorator @client.on(...)
#
# Example assuming your client is named 'client':
# client.add_event_handler(test_download_handler, events.NewMessage(pattern='/testdonl'))
#
# If using the decorator, ensure 'client' is defined before the handler function:
# from telethon import TelegramClient, events
#
# api_id = ...
# api_hash = ...
# client = TelegramClient('my_session', api_id, api_hash)
#
# @client.on(events.NewMessage(pattern='/testdonl'))
# async def test_download_handler(event):
#    # ... (paste the handler code here) ...
#
# # Then run your client
# # client.start()
# # client.run_until_disconnected()
