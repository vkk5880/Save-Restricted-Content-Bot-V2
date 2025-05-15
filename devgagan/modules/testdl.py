# In your devgagan/modules/testdl.py file

from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message # Import Message type for type hints
import random
import requests
import string
import aiohttp
# Import the Pyrogram client instance 'app' from devgagan.__init__
from devgagan import app
# Import the Telethon client instance 'telethon_user_client' from devgagan.__init__
from devgagan import telethon_user_client
from devgagan.core.func import *
from datetime import datetime, timedelta
from motor.motor_asyncio import AsyncIOMotorClient
from config import MONGO_DB, WEBSITE_URL, AD_API, LOG_GROUP


import time
import math
import asyncio
import os

# --- CORRECTED TELETHON IMPORTS ---
from telethon import TelegramClient, events, types # Import Telethon client, events, types
from telethon.errors import FloodWaitError, RPCError # Removed TakeoutInitError, correct errors
# Note: The datetime import is already above, no need to repeat unless specifically for Telethon types

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
# This callback receives the Pyrogram client to edit the message it sent.
async def progress_callback(current, total, pyro_client: Client, progress_message_id: int, chat_id: int):
    # Retrieve progress data for this download
    progress_data = DOWNLOAD_PROGRESS_DATA.get(progress_message_id)
    if not progress_data:
        # This can happen if the download finished very quickly before the first edit, or cleanup happened
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
    speed_bytes_per_second = 0
    if time_since_last_edit > 0.5: # Use a small threshold to avoid division by zero or tiny intervals
         speed_bytes_per_second = bytes_since_last_edit / time_since_last_edit
    elif current > 0 and (now - start_time) > 0: # Fallback to average speed if needed
         elapsed_total = now - start_time
         speed_bytes_per_second = current / elapsed_total


    # Calculate ETA (estimated time of arrival) using the calculated speed
    bytes_remaining = total_size - current
    eta_formatted = "Calculating..."
    if speed_bytes_per_second > 1: # Only calculate ETA if speed is meaningful (>1 byte/s)
        eta_seconds = bytes_remaining / speed_bytes_per_second
        # Format ETA nicely as HH:MM:SS or MM:SS
        eta_formatted = str(timedelta(seconds=int(eta_seconds)))


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
            # Use the Pyrogram client passed as 'pyro_client' to edit the message
            await pyro_client.edit_message_text(chat_id, progress_message_id, progress_text) # Use edit_message_text
            # Update progress data after successful edit
            DOWNLOAD_PROGRESS_DATA[progress_message_id]['last_edited_time'] = now
            DOWNLOAD_PROGRESS_DATA[progress_message_id]['last_bytes_sent'] = current

        except FloodWaitError as e: # This FloodWait is from Telegram API response during edit
            print(f"FloodWait during edit of message {progress_message_id} in chat {chat_id}: Need to wait {e.seconds}s")
            await asyncio.sleep(e.seconds)
            # Try editing again after wait
            try:
                 await pyro_client.edit_message_text(chat_id, progress_message_id, progress_text) # Use edit_message_text
                 DOWNLOAD_PROGRESS_DATA[progress_message_id]['last_edited_time'] = time.time()
                 DOWNLOAD_PROGRESS_DATA[progress_message_id]['last_bytes_sent'] = current
            except Exception as edit_error:
                 print(f"Error editing message {progress_message_id} after FloodWait: {edit_error}")

        except Exception as edit_error:
            # Catch other potential errors during message edit (e.g., message deleted)
            print(f"Error editing message {progress_message_id} in chat {chat_id}: {edit_error}")


# --- Pyrogram Command Handler for /testdonl ---

@app.on_message(filters.command("testdonl")) # This decorator uses your Pyrogram client 'app'
async def test_download_handler(client: Client, message: Message): # 'client' here is your Pyrogram client (which is 'app')
    # client: Pyrogram client instance (app)
    # message: Pyrogram Message object (the command message)

    user_chat_id = message.chat.id

    # Ensure telethon_user_client is initialized and started
    global telethon_user_client
    if telethon_user_client is None or not telethon_user_client.is_connected():
         # Handle the case where telethon_user_client wasn't initialized or connected
         await client.send_message(user_chat_id, "⚠️ Telethon client is not running or connected. Cannot perform this download.")
         print("Error: telethon_user_client is not running/connected in test_download_handler.")
         return


    # Send an initial message using the Pyrogram client ('client' in this handler's scope)
    # Using send_message is often more reliable for standalone status messages than message.reply
    status_message = await client.send_message(user_chat_id, "🔍 Fetching file details...")

    progress_message_id = status_message.id

    # Initialize progress data for this download in the global dictionary
    DOWNLOAD_PROGRESS_DATA[progress_message_id] = {
        'start_time': time.time(), # Initial timestamp
        'last_edited_time': time.time(),
        'last_bytes_sent': 0,
        'total_size': 0 # Placeholder, will be updated once message is fetched
    }

    # fetch_start_time = time.time() # Not strictly needed if using download_start_time later

    try:
        # --- IMPORTANT: Use the TELETHON client instance ('telethon_user_client') to get the message ---
        # Telethon's get_messages returns Telethon Message objects
        messages = await telethon_user_client.get_messages(TARGET_CHAT_ID, ids=[TARGET_MESSAGE_ID])

        if not messages or not messages[0]:
            # Use the Pyrogram client ('client') to edit the message
            await client.edit_message_text(user_chat_id, progress_message_id, "❌ Could not find the specified message.") # Use edit_message_text
            return

        target_message = messages[0] # This is a Telethon Message object


        if not target_message.media:
             await client.edit_message_text(user_chat_id, progress_message_id, "❌ The specified message does not contain downloadable media.") # Use edit_message_text
             return

        # --- Determine File Size ---
        # This logic uses Telethon media object attributes (target_message.media) and Telethon types
        file_size = 0
        if hasattr(target_message.media, 'document') and hasattr(target_message.media.document, 'size'):
            file_size = target_message.media.document.size
        elif hasattr(target_message.media, 'photo') and isinstance(target_message.media.photo, types.Photo):
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
        # This logic also relies on Telethon media object attributes and types
        file_name_part = f"media_{target_message.id}" # Default base name
        if hasattr(target_message.media, 'document') and target_message.media.document.attributes:
             for attr in target_message.media.document.attributes:
                 if isinstance(attr, types.DocumentAttributeFilename): # Using telethon.types.DocumentAttributeFilename
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

        if not os.path.exists(DOWNLOAD_DIR):
            os.makedirs(DOWNLOAD_DIR)

        save_path = os.path.join(DOWNLOAD_DIR, file_name_part)

        base, ext = os.path.splitext(save_path)
        count = 1
        original_save_path = save_path
        while os.path.exists(save_path):
             save_path = f"{base}_{count}{ext}"
             count += 1
        if save_path != original_save_path:
             print(f"File already exists, saving as: {save_path}")


        # --- Perform the download with the progress callback ---
        # Use the TELETHON client instance ('telethon_user_client') to call download_media
        download_start_time = time.time() # Record time before starting download
        DOWNLOAD_PROGRESS_DATA[progress_message_id]['start_time'] = download_start_time
        DOWNLOAD_PROGRESS_DATA[progress_message_id]['last_edited_time'] = download_start_time # Reset last edit time

        downloaded_path = await telethon_user_client.download_media( # <-- CORRECT (uses Telethon client)
            target_message, # Pass the Telethon message object returned by telethon_client.get_messages
            file=save_path, # Specify the path to save the file
            progress_callback=progress_callback, # Set the progress callback function
            # Pass the Pyrogram client instance ('client') to the callback
            # The callback needs the Pyrogram client to edit the message sent by the Pyrogram client.
            progress_args=(client, progress_message_id, user_chat_id) # Pass Pyrogram client ('client') from handler scope
        )

        end_time = time.time()
        total_elapsed_time = end_time - download_start_time # Calculate time based on download start

        if downloaded_path:
            final_message = (
                "✅ Download completed successfully!\n"
                f"💾 Saved to: `{downloaded_path}`\n" # Use markdown code block for path
                f"⏱️ Total time: {total_elapsed_time:.2f} seconds"
            )
        else:
            final_message = "❌ Download failed or was cancelled."

        # Use the Pyrogram client ('client') to edit the final message
        await client.edit_message_text(user_chat_id, progress_message_id, final_message, parse_mode='md') # Use edit_message_text


    except FloodWaitError as e: # These are Telethon errors, expected from telethon_client calls
        error_msg = f"⏳ FloodWait Error: Please try again in {e.seconds} seconds."
        await client.edit_message_text(user_chat_id, progress_message_id, error_msg) # Use edit_message_text
        print(f"FloodWait caught: {e}")
    except RPCError as e: # This is a Telethon error, expected from telethon_client calls
        error_msg = f"❌ Telegram API Error: {e.code} - {e.text}"
        await client.edit_message_text(user_chat_id, progress_message_id, error_msg) # Use edit_message_text
        print(f"RPCError caught: {e}")
    except Exception as e:
        # Catch any other unexpected errors
        error_msg = f"⚠️ An unexpected error occurred: {type(e).__name__} - {e}"
        print(f"Exception details: {type(e).__name__}: {e}")
        await client.edit_message_text(user_chat_id, progress_message_id, error_msg) # Use edit_message_text

    finally:
        # Clean up the progress data for this download regardless of success or failure
        if progress_message_id in DOWNLOAD_PROGRESS_DATA:
             del DOWNLOAD_PROGRESS_DATA[progress_message_id]
