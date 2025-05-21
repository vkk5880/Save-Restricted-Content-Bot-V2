# ---------------------------------------------------
# File Name: __init__.py
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

import asyncio
import logging
from pyrogram import Client
from pyrogram.enums import ParseMode 
from config import API_ID, API_HASH, BOT_TOKEN, STRING, MONGO_DB
from telethon.sync import TelegramClient
from motor.motor_asyncio import AsyncIOMotorClient
import time
import sys
import os

SESSION_DIR = "data/sessions"
os.makedirs(SESSION_DIR, exist_ok=True) # Create the directory if it doesn't exist

pyrogram_session_path = os.path.join(SESSION_DIR, "my_pyrogram_bot")
telethon_session_file_path = os.path.join(SESSION_DIR, "my_telethon_user")

loop = asyncio.get_event_loop()

logging.basicConfig(
    format='[%(levelname)s/%(asctime)s] %(name)s: %(message)s',  
    level=logging.INFO, # Set the minimum logging level to capture (e.g., logging.INFO, logging.DEBUG)
    stream=sys.stdout # Direct log output to standard output
)

botStartTime = time.time()
#name=pyrogram_session_path,
app = Client(
    ":RestrictBot:",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=50,
    parse_mode=ParseMode.MARKDOWN
)

pro = Client("ggbot", api_id=API_ID, api_hash=API_HASH, session_string=STRING,  workers=80, max_concurrent_transmissions=80)

#telethon_user_client = TelegramClient(session=telethon_session_file_path, API_ID, API_HASH).start(bot_token=BOT_TOKEN)


telethon_user_client = TelegramClient(
    'sexrepo',
    api_id=API_ID,
    api_hash=API_HASH,
)


#current_dc = telethon_user_client.session.dc_id
#if current_dc != 4:
 #print(f"Original DC: {current_dc}")
 #await telethon_user_client.disconnect()
 #await telethon_user_client._switch_dc(4)  # Switch to DC4
 #await telethon_user_client.connect()
 #logger.info(f"New DC: {telethon_user_client.session.dc_id}")

# MongoDB setup
tclient = AsyncIOMotorClient(MONGO_DB)
tdb = tclient["telegram_bot"]  # Your database
token = tdb["tokens"]  # Your tokens collection

async def create_ttl_index():
    """Ensure the TTL index exists for the `tokens` collection."""
    await token.create_index("expires_at", expireAfterSeconds=0)

# Run the TTL index creation when the bot starts
async def setup_database():
 await create_ttl_index()
 print("MongoDB TTL index created.")
 
 



# You can call this in your main bot file before starting the bot

async def restrict_bot():
    global BOT_ID, BOT_NAME, BOT_USERNAME
    await setup_database()
    await app.start()
    await telethon_user_client.start(bot_token=BOT_TOKEN)
    print(f"Original DC: {telethon_user_client.session.dc_id}")
    #await telethon_user_client._switch_dc(4)  # Switch to DC4
    #print(f"New DC: {telethon_user_client.session.dc_id}")
    getme = await app.get_me()
    BOT_ID = getme.id
    BOT_USERNAME = getme.username
    if getme.last_name:
        BOT_NAME = getme.first_name + " " + getme.last_name
    else:
        BOT_NAME = getme.first_name
    if STRING:
        await pro.start()

loop.run_until_complete(restrict_bot())
