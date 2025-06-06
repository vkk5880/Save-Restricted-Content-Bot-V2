# ---------------------------------------------------
# File Name: main.py
# Description: A Pyrogram bot for downloading files from Telegram channels or groups 
#              and uploading them back to Telegram.
# Author: Gagan
# GitHub: https://github.com/devgaganin/
# Telegram: https://t.me/team_spy_pro
# YouTube: https://youtube.com/@dev_gagan
# Created: 2025-01-11
# Last Modified: 2025-01-11
# Version: 2.0.5
# License: MIT License
# More readable 
# ---------------------------------------------------

import time
import random
import string
import asyncio
import pymongo
from pyrogram import filters, Client
from devgagan import app
from config import API_ID, API_HASH, FREEMIUM_LIMIT, PREMIUM_LIMIT, OWNER_ID
from devgagan.core.get_func import get_msg, get_msg_direct
from devgagan.core.get_func import get_msg_telethon
from devgagan.core.func import *
from devgagan.core.mongo import db
from pyrogram.errors import FloodWait
from datetime import datetime, timedelta
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from devgagan.core.mongo.db import user_sessions_real
import subprocess
from telethon.sync import TelegramClient
from session_converter import SessionManager
from pyrogram.handlers import MessageHandler, CallbackQueryHandler
from telethon.errors import FloodWaitError
from devgagan.modules.shrink import is_user_verified, create_bot_client_pyro, create_bot_client_telethon
from config import MONGO_DB as MONGODB_CONNECTION_STRING, LOG_GROUP, OWNER_ID, STRING, API_ID, CONTACT, API_HASH, CHANNEL_LINK
import logging
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import (
    SessionPasswordNeededError,
    AuthKeyError,
    AccessTokenExpiredError,
    AuthKeyDuplicatedError
)


'''
from devgagan.modules.connect_user import (
    connect_user, 
    disconnect_user, 
    owner_message_handler, 
    user_reply_handler, 
    send_message_callback, 
    cancel_message_callback,
    active_connections
)
'''
#import devgagan.modules.connectUser  # Correct import path
#from devgagan.modules.connectUser import register_handlers  # Import register function
from devgagan.modules.shrink import is_user_verified
async def generate_random_name(length=8):
    return ''.join(random.choices(string.ascii_lowercase, k=length))

users_loop = {}
interval_set = {}
batch_mode = {}
#register_handlers(app)
'''
# Create a separate instance for connectUser.py handlers
connect_app = Client("connect_user_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# Register handlers with the new instance
register_handlers(connect_app)

# Start the new instance separately
connect_app.run()

'''
# MongoDB database name and collection name
DB_NAME = "smart_users"
COLLECTION_NAME = "super_user"

mongo_app = pymongo.MongoClient(MONGODB_CONNECTION_STRING)
mongo_db = mongo_app[DB_NAME]
collection = mongo_db[COLLECTION_NAME]


bot_client_pyro = None
bot_client_tele = None

async def fetch_upload_method(message, user_id):
    """Fetch the user's preferred upload method."""
    freecheck = await chk_user(message, user_id)
    if freecheck == 1 and user_id not in OWNER_ID and not await is_user_verified(user_id):
        print("Always Pyrogram for non-pro ...")
        return "Pyrogram" # Always Pyrogram for non-pro

    user_data = collection.find_one({"user_id": user_id})
    #print(f"fetch_upload_method ... {user_data.get('upload_method', 'Pyrogram')}")
    return user_data.get("upload_method", "Pyrogram") if user_data else "Pyrogram"

async def process_and_upload_direct(userbot, user_id, msg_id, link, retry_count, message):
    print("process_and_upload_direct method.")
    try:
        await get_msg_direct(userbot, user_id, msg_id, link, retry_count, message)
        await asyncio.sleep(4)
    finally:
        pass


async def process_and_upload_link(userbot, user_id, msg_id, link, retry_count, message):
    print("process_and_upload_link method.")
    try:
        await get_msg(userbot, user_id, msg_id, link, retry_count, message)
        await asyncio.sleep(4)
    finally:
        pass


async def process_and_upload_link_telethon(telethon_userbot, user_id, msg_id, link, retry_count, message):
    print("process_and_upload_link method_telethon.")
    try:
        await get_msg_telethon(telethon_userbot, user_id, msg_id, link, retry_count, message)
        await asyncio.sleep(15)
    finally:
        pass




# Function to check if the user can proceed
async def check_interval(user_id, freecheck):
    if freecheck != 1 or await is_user_verified(user_id):  # Premium or owner users can always proceed
        return True, None

    now = datetime.now()

    # Check if the user is on cooldown
    if user_id in interval_set:
        cooldown_end = interval_set[user_id]
        if now < cooldown_end:
            remaining_time = (cooldown_end - now).seconds
            return False, f"Please wait {remaining_time} seconds(s) before sending another link. Alternatively, purchase premium for instant access.\n\n>"
        else:
            del interval_set[user_id]  # Cooldown expired, remove user from interval set

    return True, None

async def set_interval(user_id, interval_minutes=45):
    now = datetime.now()
    # Set the cooldown interval for the user
    interval_set[user_id] = now + timedelta(seconds=interval_minutes)
    

@app.on_message(
    filters.regex(r'https?://(?:www\.)?t\.me/[^\s]+|tg://openmessage\?user_id=\w+&message_id=\d+')
    & filters.private
)
async def single_link(_, message):
    user_id = message.chat.id

    # Check subscription and batch mode
    if await subscribe(_, message) == 1 or user_id in batch_mode:
        return

    # Check if user is already in a loop
    if users_loop.get(user_id, False):
        await message.reply(
            "You already have an ongoing process. Please wait for it to finish or cancel it with /cancel."
        )
        return

    # Check freemium limits
    if await chk_user(message, user_id) == 1 and FREEMIUM_LIMIT == 0 and user_id not in OWNER_ID and not await is_user_verified(user_id):
        await message.reply("Freemium service is currently not available. Upgrade to premium for access.")
        return

    # Check cooldown
    can_proceed, response_message = await check_interval(user_id, await chk_user(message, user_id))
    if not can_proceed:
        await message.reply(response_message)
        return



    sessions = await db.get_sessions(user_id)
    if not sessions or not sessions.get("userbot_token"):
        if user_id not in OWNER_ID:
            logger.warning(f"No userbot_token found for user {user_id}")
            msg = await message.reply(
                "⚠️ You need to set up your bot first. Please use /setbot.\n\n"
                "💡 Tip: Set preferred file formats in /settings for automatic conversion."
            )
            return None

    # Add user to the loop
    users_loop[user_id] = True

    link = message.text if "tg://openmessage" in message.text else get_link(message.text)
    msg = await message.reply("Processing...")

    upload_methods = await fetch_upload_method(message, user_id)  # Fetch the upload method (Pyrogram or Telethon)
    print(f"upload_method ... {upload_methods}")
    telethon_userbot = None
    userbot = None
    bot_client_pyro = await create_bot_client_pyro(user_id)
    bot_client_tele = await create_bot_client_telethon(user_id)
    if upload_methods == "Pyrogram":
        userbot = await initialize_userbot(user_id)
    elif upload_methods == "Telethon":
        telethon_userbot  = await initialize_telethon_userbot(user_id)
    try:
        if await is_normal_tg_link(link):
            # Pass userbot if available; handle normal Telegram links
            print("process_and_upload_link.")
            if upload_methods == "Pyrogram":
                await process_and_upload_link(userbot, user_id, msg.id, link, 0, message)
            elif upload_methods == "Telethon":
                if telethon_userbot is None:
                    print("telethon_userbot is Non.")
                    await message.reply("telethon_userbot is Non.")
                    return
                await process_and_upload_link_telethon(telethon_userbot, user_id, msg.id, link, 0, message)
            await set_interval(user_id, interval_minutes=45)
            print("process_and_upload_link was completed.")
        else:
            # Handle special Telegram links
            print("process_and_upload_special_link.")
            if upload_methods == "Pyrogram":
                await process_special_links(userbot, user_id, msg, link)
            elif upload_methods == "Telethon":
                if telethon_userbot is None:
                    print("telethon_userbot is Non.")
                    await message.reply("telethon_userbot is Non.")
                    return
                await process_special_links_telethon(telethon_userbot, user_id, msg, link)
            
    except (FloodWaitError, FloodWait) as fw:
        seconds = fw.seconds if hasattr(fw, 'seconds') else fw.value
        await msg.edit_text(f'Try again in {seconds} seconds due to floodwait from Telegram.')
    except Exception as e:
        await msg.edit_text(f"Link: `{link}`\n\n**Error:** {str(e)}")
    finally:
        users_loop[user_id] = False
        if userbot:
            await userbot.stop()
        if telethon_userbot:
            await telethon_userbot.disconnect()
        try:
            await msg.delete()
        except Exception:
            pass






# Initialize logger at module level
logger = logging.getLogger(__name__)




async def initialize_telethon_userbot(user_id):
    """
    Initialize and verify Telethon userbot with complete status checking
    Returns: TelegramClient instance or None if initialization fails
    """
    try:
        # 1. Get session from DB
        sessions = await db.get_sessions(user_id)
        if not sessions or not sessions.get("telethon_session"):
            logger.warning(f"No Telethon session found for user {user_id}")
            return None

        # 2. Create client instance
        telethon_userbot = TelegramClient(
            session=StringSession(sessions["telethon_session"]),
            api_id=API_ID,
            api_hash=API_HASH,
            device_model="iPhone 16 Pro",
            system_version="13.3.1",
        )

        # 3. Start connection with verification
        try:
            await telethon_userbot.start()
            print(f"Original DC: {telethon_userbot.session.dc_id}")
            #await telethon_userbot.disconnect()
            #await telethon_userbot._switch_dc(4)  # Europe
            #print(f"New DC: {telethon_userbot.session.dc_id}")
            #await telethon_userbot.start()
            #telethon_userbot.session = StringSession(sessions["telethon_session"])
            #await telethon_userbot.connect()  # Reconnect with the session
            #await telethon_userbot.get_me()  # Test API call
            # 4. Verify active connection
            if not telethon_userbot.is_connected():
                logger.error("Start completed but not actually connected")
                await telethon_userbot.disconnect()
                return None

            # 5. Verify authorization
            if not await telethon_userbot.is_user_authorized():
                logger.error("Session invalid - not authorized")
                await telethon_userbot.disconnect()
                await db.remove_telethon_session(user_id)
                return None

            # 6. Test API call
            try:
                me = await telethon_userbot.get_me()
                logger.info(f"Successfully started as @{me.username}")
                return telethon_userbot
            except Exception as api_error:
                logger.error(f"API test failed: {str(api_error)}")
                await telethon_userbot.disconnect()
                return None

        except (ConnectionError, asyncio.TimeoutError) as e:
            logger.error(f"Connection failed: {str(e)}")
            return None
        except AuthKeyError as e:
            logger.error(f"Invalid auth key: {str(e)}")
            await db.remove_telethon_session(user_id)
            return None

    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        return None


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


async def convert_user_string(pyrogram_string: str):
    # Convert to Telethon session

    if not pyrogram_string:
        print("Error: Pyrogram session string provided to conversion is empty.")
        return None


    try:
        # Convert to Telethon session
        # from_pyrogram_string_session requires the API_ID
        session_manager = SessionManager.from_pyrogram_string_session(pyrogram_string)

        # Export the session as a Telethon string
        telethon_session_string = session_manager.telethon_string_session()
        print(telethon_session_string)
        return telethon_session_string

    except Exception as e:
        print(f"Error during session conversion: {e}")
        # You might want more specific error handling here
        return None



async def initialize_telethon_userbotsss(user_id): # this ensure the single startup .. even if logged in or not
    """Initialize the userbot session for the given user."""
    sessions = await db.get_sessions(user_id)
    if sessions:
        telethon_string = sessions["telethon_session"]
        try:
            device = 'iPhone 16 Pro' # added gareebi text
            telethon_userbot = TelegramClient(
                "telethon_userbot",
                api_id=API_ID,
                api_hash=API_HASH,
                device_model=device,
                session_string=telethon_string
            )
            await telethon_userbot.start()
            print("telethon_userbot success")
            return telethon_userbot
        except Exception:
            print("telethon_userbot  error")
            return None

    print("telethon_session_string or data none")
    return None



async def is_normal_tg_link(link: str) -> bool:
    """Check if the link is a standard Telegram link."""
    special_identifiers = ['t.me/+', 't.me/c/', 't.me/b/', 'tg://openmessage']
    return 't.me/' in link and not any(x in link for x in special_identifiers)
    
async def process_special_links(userbot, user_id, msg, link):
    """Handle special Telegram links."""
    if 't.me/+' in link:
        result = await userbot_join(userbot, link)
        await msg.edit_text(result)
    elif any(sub in link for sub in ['t.me/c/', 't.me/b/', '/s/', 'tg://openmessage']):
        await process_and_upload_link(userbot, user_id, msg.id, link, 0, msg)
        await set_interval(user_id, interval_minutes=45)
    else:
        await msg.edit_text("Invalid link format.")

async def process_special_links_telethon(telethon_userbot, user_id, msg, link):
    """Handle special Telegram links using Telethon."""
    if 't.me/+' in link:
        result = await telethon_userbot_join(telethon_userbot, link)
        await msg.edit_text(result)
    elif any(sub in link for sub in ['t.me/c/', 't.me/b/', '/s/', 'tg://openmessage']):
        await process_and_upload_link_telethon(telethon_userbot, user_id, msg.id, link, 0, msg)
        await set_interval(user_id, interval_minutes=45)
    else:
        await msg.edit_text("Invalid link format.")



@app.on_message(filters.command("batch") & filters.private)
async def batch_link(_, message):
    join = await subscribe(_, message)
    if join == 1:
        return
    user_id = message.chat.id
    
    # Check if a batch process is already running
    if users_loop.get(user_id, False):
        await app.send_message(
            message.chat.id,
            "You already have a batch process running. Please wait for it to complete."
        )
        return

    freecheck = await chk_user(message, user_id)
    if freecheck == 1 and FREEMIUM_LIMIT == 0 and user_id not in OWNER_ID and not await is_user_verified(user_id):
        await message.reply("Freemium service is currently not available. Upgrade to premium for access.")
        return


    sessions = await db.get_sessions(user_id)
    if not sessions or not sessions.get("userbot_token"):
        if user_id not in OWNER_ID:
            logger.warning(f"No userbot_token found for user {user_id}")
            msg = await message.reply(
                "⚠️ You need to set up your bot first. Please use /setbot.\n\n"
                "💡 Tip: Set preferred file formats in /settings for automatic conversion."
            )
            return None
    
    max_batch_size = FREEMIUM_LIMIT if freecheck == 1 else PREMIUM_LIMIT

    # Start link input
    for attempt in range(3):
        start = await app.ask(message.chat.id, "Please send the start link.\n\n> Maximum tries 3")
        start_id = start.text.strip()
        s = start_id.split("/")[-1]
        if s.isdigit():
            cs = int(s)
            break
        await app.send_message(message.chat.id, "Invalid link. Please send again ...")
    else:
        await app.send_message(message.chat.id, "Maximum attempts exceeded. Try later.")
        return

    # Number of messages input
    for attempt in range(3):
        num_messages = await app.ask(message.chat.id, f"How many messages do you want to process?\n> Max limit {max_batch_size}")
        try:
            cl = int(num_messages.text.strip())
            if 1 <= cl <= max_batch_size:
                break
            raise ValueError()
        except ValueError:
            await app.send_message(
                message.chat.id, 
                f"Invalid number. Please enter a number between 1 and {max_batch_size}."
            )
    else:
        await app.send_message(message.chat.id, "Maximum attempts exceeded. Try later.")
        return

    # Validate and interval check
    can_proceed, response_message = await check_interval(user_id, freecheck)
    if not can_proceed:
        await message.reply(response_message)
        return
        
    join_button = InlineKeyboardButton("Join Channel", url=CHANNEL_LINK)
    keyboard = InlineKeyboardMarkup([[join_button]])
    pin_msg = await app.send_message(
        user_id,
        f"Batch process started ⚡\nProcessing: 0/{cl}\n\n****",
        reply_markup=keyboard
    )
    await pin_msg.pin(both_sides=True)

    users_loop[user_id] = True
    telethon_userbot = None
    userbot = None
    
    try:
        upload_methods = await fetch_upload_method(message, user_id)
        print(f"upload_method ... {upload_methods}")
        
        # Initialize the appropriate client
        if upload_methods == "Pyrogram":
            userbot = await initialize_userbot(user_id)
        elif upload_methods == "Telethon":
            telethon_userbot = await initialize_telethon_userbot(user_id)
        
        normal_links_handled = False
        
        # Process all links
        for i in range(cs, cs + cl):
            if not users_loop.get(user_id, False):
                break
                
            url = f"{'/'.join(start_id.split('/')[:-1])}/{i}"
            link = get_link(url)
            msg = await app.send_message(message.chat.id, f"Processing...")
            
            try:
                # Handle normal public links
                if 't.me/' in link and not any(x in link for x in ['t.me/b/', 't.me/c/', 'tg://openmessage']):
                    if upload_methods == "Pyrogram":
                        await process_and_upload_link(userbot, user_id, msg.id, link, i-cs, message)
                    elif upload_methods == "Telethon":
                        await process_and_upload_link_telethon(telethon_userbot, user_id, msg.id, link, i-cs, message)
                    normal_links_handled = True
                
                # Handle special links
                elif any(x in link for x in ['t.me/b/', 't.me/c/', 'tg://openmessage']):
                    if upload_methods == "Pyrogram" and userbot:
                        await process_special_links(userbot, user_id, msg, link)
                    elif upload_methods == "Telethon" and telethon_userbot:
                        await process_special_links_telethon(telethon_userbot, user_id, msg, link)
                    else:
                        await app.send_message(message.chat.id, "Login in bot first ...")
                        break
                
                await pin_msg.edit_text(
                    f"Batch process started ⚡\nProcessing: {i - cs + 1}/{cl}\n\n****",
                    reply_markup=keyboard
                )
                
            except (FloodWaitError, FloodWait) as fw:
                seconds = fw.seconds if hasattr(fw, 'seconds') else fw.value
                await msg.edit_text(f'FloodWait: Try again in {seconds} seconds')
                await asyncio.sleep(seconds)
                continue
            except Exception as e:
                await msg.edit_text(f"Error processing {link}: {str(e)}")
                continue
            finally:
                try:
                    await msg.delete()
                except:
                    pass

        if normal_links_handled:
            await set_interval(user_id, interval_minutes=300)
        
        await pin_msg.edit_text(
            f"Batch completed successfully for {cl} messages 🎉\n\n****",
            reply_markup=keyboard
        )
        await app.send_message(message.chat.id, "Batch completed successfully! 🎉")

    except Exception as e:
        await app.send_message(message.chat.id, f"Batch processing failed: {e}")
    finally:
        users_loop.pop(user_id, None)
        if userbot:
            await userbot.stop()
        if telethon_userbot:
            await telethon_userbot.disconnect()
        




@app.on_message(filters.command("batchdr") & filters.private)
async def batchdr_link(_, message):
    join = await subscribe(_, message)
    if join == 1:
        return
    user_id = message.chat.id
    
    # Check if a batch process is already running
    if users_loop.get(user_id, False):
        await app.send_message(
            message.chat.id,
            "You already have a batch process running. Please wait for it to complete."
        )
        return

    freecheck = await chk_user(message, user_id)
    if freecheck == 1 and FREEMIUM_LIMIT == 0 and user_id not in OWNER_ID and not await is_user_verified(user_id):
        await message.reply("Freemium service is currently not available. Upgrade to premium for access.")
        return


    sessions = await db.get_sessions(user_id)
    if not sessions or not sessions.get("userbot_token"):
        if user_id not in OWNER_ID:
            logger.warning(f"No userbot_token found for user {user_id}")
            msg = await message.reply(
                "⚠️ You need to set up your bot first. Please use /setbot.\n\n"
                "💡 Tip: Set preferred file formats in /settings for automatic conversion."
            )
            return None
    
    max_batch_size = FREEMIUM_LIMIT if freecheck == 1 else PREMIUM_LIMIT

    # Start link input
    for attempt in range(3):
        start = await app.ask(message.chat.id, "Please send the start link.\n\n> Maximum tries 3")
        start_id = start.text.strip()
        s = start_id.split("/")[-1]
        if s.isdigit():
            cs = int(s)
            break
        await app.send_message(message.chat.id, "Invalid link. Please send again ...")
    else:
        await app.send_message(message.chat.id, "Maximum attempts exceeded. Try later.")
        return

    # Number of messages input
    for attempt in range(3):
        num_messages = await app.ask(message.chat.id, f"How many messages do you want to process?\n> Max limit {max_batch_size}")
        try:
            cl = int(num_messages.text.strip())
            if 1 <= cl <= max_batch_size:
                break
            raise ValueError()
        except ValueError:
            await app.send_message(
                message.chat.id, 
                f"Invalid number. Please enter a number between 1 and {max_batch_size}."
            )
    else:
        await app.send_message(message.chat.id, "Maximum attempts exceeded. Try later.")
        return

    # Validate and interval check
    can_proceed, response_message = await check_interval(user_id, freecheck)
    if not can_proceed:
        await message.reply(response_message)
        return
        
    join_button = InlineKeyboardButton("Join Channel", url=CHANNEL_LINK)
    keyboard = InlineKeyboardMarkup([[join_button]])
    pin_msg = await app.send_message(
        user_id,
        f"Batch process started ⚡\nProcessing: 0/{cl}\n\n****",
        reply_markup=keyboard
    )
    await pin_msg.pin(both_sides=True)

    users_loop[user_id] = True
    telethon_userbot = None
    userbot = None
    
    try:
        upload_methods = await fetch_upload_method(message, user_id)
        print(f"upload_method ... {upload_methods}")
        
        # Initialize the appropriate client
        userbot = await initialize_userbot(user_id)
        
        normal_links_handled = False
        
        # Process all links
        for i in range(cs, cs + cl):
            if not users_loop.get(user_id, False):
                break
                
            url = f"{'/'.join(start_id.split('/')[:-1])}/{i}"
            link = get_link(url)
            msg = await app.send_message(message.chat.id, f"Processing...")
            
            try:
                # Handle normal public links
                if 't.me/' in link and not any(x in link for x in ['t.me/b/', 't.me/c/', 'tg://openmessage']):
                    await process_and_upload_direct(userbot, user_id, msg.id, link, i-cs, message)
                    #normal_links_handled = True
                
                # Handle special links
                elif any(x in link for x in ['t.me/b/', 't.me/c/', 'tg://openmessage']):
                    if userbot:
                        await process_and_upload_direct(userbot, user_id, msg.id, link, i-cs, message)
                    else:
                        await app.send_message(message.chat.id, "Login in bot first ...")
                        break
                
                await pin_msg.edit_text(
                    f"Batch process started ⚡\nProcessing: {i - cs + 1}/{cl}\n\n****",
                    reply_markup=keyboard
                )
                
            except (FloodWaitError, FloodWait) as fw:
                seconds = fw.seconds if hasattr(fw, 'seconds') else fw.value
                await msg.edit_text(f'FloodWait: Try again in {seconds} seconds')
                await asyncio.sleep(seconds)
                continue
            except Exception as e:
                await msg.edit_text(f"Error processing {link}: {str(e)}")
                continue
            finally:
                try:
                    await msg.delete()
                except:
                    pass

        if normal_links_handled:
            await set_interval(user_id, interval_minutes=300)
        
        await pin_msg.edit_text(
            f"Batch completed successfully for {cl} messages 🎉\n\n****",
            reply_markup=keyboard
        )
        await app.send_message(message.chat.id, "Batch completed successfully! 🎉")

    except Exception as e:
        await app.send_message(message.chat.id, f"Batch processing failed: {e}")
    finally:
        users_loop.pop(user_id, None)
        if userbot:
            await userbot.stop()
        

@app.on_message(filters.command("cancel"))
async def stop_batch(_, message):
    user_id = message.chat.id

    # Check if there is an active batch process for the user
    if user_id in users_loop and users_loop[user_id]:
        users_loop[user_id] = False  # Set the loop status to False
        await app.send_message(
            message.chat.id, 
            "Batch processing has been stopped successfully. You can start a new batch now if you want."
        )
    elif user_id in users_loop and not users_loop[user_id]:
        await app.send_message(
            message.chat.id, 
            "The batch process was already stopped. No active batch to cancel."
        )
    else:
        await app.send_message(
            message.chat.id, 
            "No active batch processing is running to cancel."
        )












