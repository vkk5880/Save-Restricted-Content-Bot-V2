 
# ---------------------------------------------------
# File Name: shrink.py
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
# ---------------------------------------------------

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
#import devgagan.modules.connectUser  # Correct import path
#from devgagan.modules.connectUser import register_handlers
 
from pyrogram.types import Message # Import Message type
from config import LOG_GROUP # Make sure LOG_GROUP is imported from your config


import logging
import sys # Import sys for standard output
 
tclient = AsyncIOMotorClient(MONGO_DB)
tdb = tclient["telegram_bot"]
token = tdb["tokens"]
 
 
async def create_ttl_index():
    await token.create_index("expires_at", expireAfterSeconds=0)
 
 
 
Param = {}
 
 
async def generate_random_param(length=8):
    """Generate a random parameter."""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))
 
 
async def get_shortened_url(deep_link):
    api_url = f"https://{WEBSITE_URL}/api?api={AD_API}&url={deep_link}"
 
     
    async with aiohttp.ClientSession() as session:
        async with session.get(api_url) as response:
            if response.status == 200:
                data = await response.json()   
                if data.get("status") == "success":
                    return data.get("shortenedUrl")
    return None
 
 
async def is_user_verified(user_id):
    """Check if a user has an active session."""
    session = await token.find_one({"user_id": user_id})
    return session is not None




 

# --- Simple Logging Setup ---
# This sets up a basic logger that prints to standard output.
# If you have a more comprehensive logging setup elsewhere, you might remove this block.

# Get a logger instance (using __name__ is common)
logger = logging.getLogger(__name__)

# Set the minimum logging level to capture
# logging.DEBUG is useful for development, logging.INFO or higher for production
logger.setLevel(logging.DEBUG)

# Create a handler to send log messages to standard output
handler = logging.StreamHandler(sys.stdout)

# Define the format for log messages
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Add the formatter to the handler
handler.setFormatter(formatter)

# Add the handler to the logger
# The check 'if not logger.handlers:' prevents adding the handler multiple times
# if this code block were somehow executed more than once.
if not logger.handlers:
    logger.addHandler(handler)

# --- End Simple Logging Setup ---


# This function should be placed in one of the files inside your
# devgagan/modules directory that is imported by __main__.py

@app.on_message(filters.command("testlogcopy"))
async def test_log_copy_command(client: Client, message: Message):
    """
    Handles the /testlogcopy command to send a test message to the LOG_GROUP
    by copying a temporary message. The temporary message is NOT deleted.
    Uses LOG_GROUP directly in the copy method and uses the simple logging setup.
    Includes LOG_GROUP value in the reply message.
    """
    # LOG_GROUP is used directly from config

    temp_message = None # Initialize temp_message to None
    logger.info(f"Received /testlogcopy command from user {message.from_user.id}") # Log command reception

    try:
        # 1. Send a temporary message to get a Message object
        # We send it to the user who sent the command
        temp_message = await message.reply("Creating a message to copy...")
        logger.debug(f"Sent temporary message (ID: {temp_message.id}) to user {message.chat.id}") # Log temporary message sent

        # 2. Copy the temporary message to the LOG_GROUP
        # Using LOG_GROUP directly
        logger.debug(f"Attempting to copy temporary message (ID: {temp_message.id}) to LOG_GROUP {LOG_GROUP}") # Log copy attempt
        copied_message = await temp_message.copy(chat_id=LOG_GROUP)
        logger.info(f"Temporary message successfully copied to LOG_GROUP {LOG_GROUP}. Copied message ID: {copied_message.id}") # Log successful copy

        # 3. Reply to the user indicating success, including LOG_GROUP value
        await message.reply(f"Test message successfully copied to the LOG_GROUP (`{LOG_GROUP}`) using copy(). Temporary message was not deleted.")
        logger.info(f"Replied to user indicating successful copy to LOG_GROUP {LOG_GROUP}.") # Log user notification

    except Exception as e:
        # Reply to the user indicating failure and the error, including LOG_GROUP value
        await message.reply(f"Failed to copy test message to the LOG_GROUP (`{LOG_GROUP}`): {e}")
        logger.error(f"Failed to copy temporary message to LOG_GROUP {LOG_GROUP} via /testlogcopy. Error: {e}", exc_info=True) # Log failure with traceback

    # The finally block for deleting the temporary message has been removed.


 























 
# This is the function previously duplicated, now renamed to test_msg_command
# and intended to send a message to a private group.
@app.on_message(filters.command("testmsg")) # Using the command name "testmsg"
async def test_msg_command(client, message):
    """
    Handles the /testmsg command to send a message to a private group.
    """
    # Replace with your target private group's chat ID (integer format)
    # Make sure the bot account is a member of this group
    private_group_id = -1002633547185 # Replace with your group ID

    try:
        # Use the 'client' instance passed to the handler
        await app.send_message(LOG_GROUP, text="This is a test message from the bot!")
        # Use the correct variable name in the print statement
        print("Message successfully sent to group {LOG_GROUP}")
    except Exception as e:
        print("Error sending message: {e}")

    # Optional: Reply to the user who sent the command for confirmation
    await message.reply("Test message successfully attempted to the LOG_GROUP (`{LOG_GROUP}`) using copy().")






@app.on_message(filters.command("start"))
async def token_handler(client, message):
    """Handle the /token command."""
    join = await subscribe(client, message)
    if join == 1:
        return
    chat_id = "still_waiting_for_uh"
    msg = await app.get_messages(chat_id,5)
    user_id = message.chat.id
    if len(message.command) <= 1:
        image_url = "https://tecolotito.elsiglocoahuila.mx/i/2023/12/2131463.jpeg"
        join_button = InlineKeyboardButton("Join Channel", url="https://t.me/+9FZJh0WMZnE4YWRk")
        premium = InlineKeyboardButton("Get Premium", url="https://t.me/contact_xbot")   
        keyboard = InlineKeyboardMarkup([
            [join_button],   
            [premium]    
        ])
         
        await message.reply_photo(
            msg.photo.file_id,
            caption=(
                "Hi 👋 Welcome, Wanna intro...?\n\n"
                "✳️ I can save posts from channels or groups where forwarding is off. I can download videos/audio from YT, INSTA, ... social platforms\n"
                "✳️ Simply send the post link of a public channel. For private channels, do /login. Send /help to know more."
            ),
            reply_markup=keyboard
        )
        return  
 
    param = message.command[1] if len(message.command) > 1 else None
    freecheck = await chk_user(message, user_id)
    if freecheck != 1:
        await message.reply("You are a premium user no need of token 😉")
        return
 
     
    if param:
        if user_id in Param and Param[user_id] == param:
             
            await token.insert_one({
                "user_id": user_id,
                "param": param,
                "created_at": datetime.utcnow(),
                "expires_at": datetime.utcnow() + timedelta(hours=3),
            })
            del Param[user_id]   
            await message.reply("✅ You have been verified successfully! Enjoy your session for next 3 hours.")
            return
        else:
            await message.reply("❌ Invalid or expired verification link. Please generate a new token.")
            return
 
@app.on_message(filters.command("token"))
async def smart_handler(client, message):
    user_id = message.chat.id
     
    freecheck = await chk_user(message, user_id)
    if freecheck != 1:
        await message.reply("You are a premium user no need of token 😉")
        return
    if await is_user_verified(user_id):
        await message.reply("✅ Your free session is already active enjoy!")
    else:
         
        param = await generate_random_param()
        Param[user_id] = param   
 
         
        deep_link = f"https://t.me/{client.me.username}?start={param}"
 
         
        shortened_url = await get_shortened_url(deep_link)
        if not shortened_url:
            await message.reply("❌ Failed to generate the token link. Please try again.")
            return
 
         
        button = InlineKeyboardMarkup(
            [[InlineKeyboardButton("Verify the token now...", url=shortened_url)]]
        )
        await message.reply("Click the button below to verify your free access token: \n\n> What will you get ? \n1. No time bound upto 3 hours \n2. Batch command limit will be FreeLimit + 20 \n3. All functions unlocked", reply_markup=button)

# ✅ Function to show Admin Commands List
@app.on_message(filters.command("admin_commands_list"))
async def show_admin_commands(client, message):
    """Displays the list of available admin commands (Owner only)."""
    owner_id=1970647198
    if message.from_user.id != owner_id:
        await message.reply("🚫 You are not the owner and cannot access this command!")
        return
    
    admin_commands = """
    👤Owner Commands List:-
    
/add userID            - ➕ Add user to premium  
/rem userID            - ➖ Remove user from premium  
/stats                 - 📊 Get bot stats  
/gcast                 - ⚡ Broadcast to all users  
/acast                 - ⚡ Broadcast with name tag  
/freez                 - 🧊 Remove expired users  
/get                   - 🗄️ Get all user IDs  
/lock                  - 🔒 Protect channel  
/hijack                - ☠️ Hijack a session
/cancel_hijack         - 🚫 Terminate Hijacking 
/session               - 🪪 Generate session string  
/connect_user          - 🔗 Connect owner & user  
/disconnect_user       - ⛔ Disconnect a user  
/admin_commands_list   - 📄 Show admin commands
    """
    await message.reply(admin_commands)

#onwer bot command list till here
#register_handlers(app)
