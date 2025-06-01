 
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
from config import MONGO_DB, WEBSITE_URL, AD_API, CONTACT, LOG_GROUP  
#import devgagan.modules.connectUser  # Correct import path
#from devgagan.modules.connectUser import register_handlers
from config import MONGO_DB as MONGODB_CONNECTION_STRING, LOG_GROUP, OWNER_ID, STRING, API_ID, CONTACT, API_HASH, CHANNEL_LINK
from pyrogram.types import Message 
from config import LOG_GROUP
import re
from devgagan.core.mongo import db
import logging
from pyrogram.enums import ParseMode
import sys # Import sys for standard output
from pyrogram import filters, Client
from telethon import TelegramClient
import pymongo
from devgagan.core.mongo import db
from devgagan.core.mongo.db import user_sessions_real
 
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
logger = logging.getLogger(__name__)

# logging.DEBUG is useful for development, logging.INFO or higher for production
logger.setLevel(logging.DEBUG)

handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
if not logger.handlers:
    logger.addHandler(handler)

# --- End Simple Logging Setup ---

@app.on_message(filters.command("testlogcopy"))
async def test_log_copy_command(client: Client, message: Message):
    temp_message = None # Initialize temp_message to None
    logger.info(f"Received /testlogcopy command from user {message.from_user.id}") # Log command reception

    try:
        temp_message = await message.reply("Creating a message to copy...")
        logger.debug(f"Sent temporary message (ID: {temp_message.id}) to user {message.chat.id}") # Log temporary message sent

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


@app.on_message(filters.command("testmsg")) # Using the command name "testmsg"
async def test_msg_command(client, message):
    private_group_id = -1002633547185 # Replace with your group ID

    try:
        # Use the 'client' instance passed to the handler
        await app.send_message(LOG_GROUP, text="This is a test message from the bot!")
        # Use the correct variable name in the print statement
        print("Message successfully sent to group {LOG_GROUP}")
    except Exception as e:
        print("Error sending message: {e}")

    # Optional: Reply to the user who sent the command for confirmation
    await message.reply(f"Test message successfully attempted to the LOG_GROUP (`{LOG_GROUP}`) .")




# MongoDB database name and collection name
DB_NAME = "smart_users"
COLLECTION_NAME = "super_user"

mongo_app = pymongo.MongoClient(MONGODB_CONNECTION_STRING)
mongo_db = mongo_app[DB_NAME]
collection = mongo_db[COLLECTION_NAME]


bot_client_pyro = None
bot_client_tele = None

async def create_bot_client_pyro(user_id):
    """Safely create and start a bot client with proper error handling"""

    sessions = await db.get_sessions(user_id)
    if not sessions or not sessions.get("userbot_token"):
        logger.warning(f"No userbot_token found for user {user_id}")
        return None
    bot_tokens = sessions.get("userbot_token")
    bot_client = Client(
        name="f:User_RestrictBot_{user_id}:",  # Session name
        api_id=API_ID,         # Your API ID from my.telegram.org
        api_hash=API_HASH,     # Your API Hash
        bot_token=bot_tokens,   # The bot token from user
        parse_mode=ParseMode.MARKDOWN
    )
    
    try:
        await bot_client.start()
        logger.info(f"Bot client started successfully for token: {bot_tokens[:10]}...")
        bot_client_pyro = bot_client
        return bot_client
    except Exception as e:
        logger.error(f"Failed to start bot client: {e}")
        await bot_client.stop()
        raise RuntimeError(f"Could not start bot client: {str(e)}")



async def create_bot_client_telethon(user_id):
    """Safely create and start a bot client with proper error handling"""

    sessions = await db.get_sessions(user_id)
    if not sessions or not sessions.get("userbot_token"):
        logger.warning(f"No userbot_token found for user {user_id}")
        return None
    bot_tokens = sessions.get("userbot_token")
    telethon_user_client_bot = TelegramClient(f"user_bot_restricted_tele_{user_id}",api_id=API_ID,api_hash=API_HASH)
    
    try:
        await telethon_user_client_bot.start(bot_token=bot_tokens)
        logger.info(f"Bot client telethon_user_client_bot started successfully for token: {bot_tokens[:10]}...")
        bot_client_tele = telethon_user_client_bot
        return telethon_user_client_bot
    except Exception as e:
        logger.error(f"Failed to start bot client telethon_user_client_bot: {e}")
        await telethon_user_client_bot.stop()
        raise RuntimeError(f"Could not start bot client telethon_user_client_bot: {str(e)}")




async def save_userbot_token(user_id, token_string):
    """Save user bot token to database"""
    ist = pytz.timezone('Asia/Kolkata')
    current_time = datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S")
    
    update_data = {
        "userbot_token": token_string
    }
    
    await db.user_sessions_real.update_one(
        {"user_id": user_id},
        {"$set": update_data},
        upsert=True
    )



@app.on_message(filters.command("setbot"))
async def setbot_handler(client: Client, message: Message):
    """Handle bot setup process"""
    user_id = message.chat.id
    
    # Send instructions for creating bot via BotFather
    instructions = """
🤖 *How to create a bot and get its token:*

1. Search for @BotFather in Telegram
2. Send `/newbot` to BotFather
3. Choose a name for your bot (must end with 'bot', e.g., 'my_test_bot')
4. Choose a username for your bot (must be unique and end with 'bot')
5. After creation, BotFather will give you a *token* (like `123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11`)

📌 *Please send me your bot token now (or forward the message from BotFather containing the token):*
    """
    
    await message.reply(instructions, parse_mode=ParseMode.MARKDOWN)
    
    # Wait for user to send token
    try:
        token_msg = await client.ask(
            user_id,
            "⌛ Waiting for your bot token...\n"
            "You can send just the token or forward BotFather's message.",
            filters=filters.text,
            timeout=300  # 5 minutes timeout
        )
    except TimeoutError:
        await message.reply("⌛ Timeout reached. Please use /setbot to try again.")
        return
    
    # Extract token from message
    token = extract_token_from_message(token_msg.text)
    
    if not token:
        await message.reply(
            "❌ Invalid token format. Please send only the token or forward BotFather's message.\n"
            "Example token: `123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11`",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    # Save to database
    await db.save_userbot_token(user_id, token)
    #save_userbot_token(user_id, token)
    await message.reply("✅ Bot token saved successfully! Your bot is now connected.")

def extract_token_from_message(text: str) -> str:
    """Extract bot token from message text"""
    # Direct token format (numbers:letters-and-numbers)
    if re.match(r'^\d+:[A-Za-z0-9_-]+$', text.strip()):
        return text.strip()
    
    # Token in BotFather message format
    token_match = re.search(r'(\d+:[A-Za-z0-9_-]+)', text)
    if token_match:
        return token_match.group(1)
    
    return None


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
        premium = InlineKeyboardButton("Get Premium", url=CONTACT)   
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
    owner_id=5914434064
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
