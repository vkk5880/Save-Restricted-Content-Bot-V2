# In your devgagan/__init__.py file

# ---------------------------------------------------
# File Name: __init__.py
# Description: Client initialization for the bot
# Author: Gagan
# GitHub: https://github.com/devgaganin/
# Telegram: https://t.me/team_spy_pro
# YouTube: https://youtube.com/@dev_gagan
# Created: 2025-01-11
# Last Modified: 2025-05-15 # Updated last modified date
# Version: 2.0.6 # Incremented version
# License: MIT License
# ---------------------------------------------------

import asyncio # Keep if needed elsewhere, though not strictly for client init
import logging
# Import Pyrogram Client
from pyrogram import Client
from pyrogram.enums import ParseMode
# Import async Telethon Client (remove .sync)
from telethon import TelegramClient
# Import MongoDB client
from motor.motor_asyncio import AsyncIOMotorClient
# Import time if needed (e.g., for logging/timing init, not for sleep here)
import time
# Import config variables
from config import API_ID, API_HASH, BOT_TOKEN, STRING, MONGO_DB


# Configure logging (can be done here or in __main__.py, good to have it early)
logging.basicConfig(
    format="[%(levelname) 5s/%(asctime)s] %(name)s: %(message)s",
    level=logging.INFO,
)

# --- Initialize Clients (DO NOT START THEM HERE - ASYNC START IN __main__.py) ---

# Pyrogram Bot Client
print("Initializing Pyrogram bot client...")
app = Client(
    ":RestrictBot:", # Session name for the bot
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=50, # Adjust workers based on load
    parse_mode=ParseMode.MARKDOWN
)
print("Pyrogram bot client initialized.")

# Pyrogram User Client (if needed via session string)
pro = None # Initialize as None
if STRING:
    print("Initializing Pyrogram user client...")
    try:
        # Ensure STRING is a valid Pyrogram session string
        pro = Client("ggbot", api_id=API_ID, api_hash=API_HASH, session_string=STRING)
        print("Pyrogram user client initialized.")
    except Exception as e:
        print(f"Error initializing Pyrogram user client from STRING: {e}")
        # Handle error - maybe log and proceed without user client


# --- Telethon User/Bot Client for specific tasks (like restricted downloads) ---
# Use the async version of TelegramClient
# For private channel access/download, a user client using API_ID/API_HASH and session file is usually needed
# if the bot isn't guaranteed to be a member of the specific channel.
# Authentication will be required the first time this session connects/starts.
telethon_user_client = None # Initialize as None
print("Initializing Telethon user client...")
try:
    # 'telethon_user_session' is the name of the session file it will create/use
    # This client will handle its own authentication flow on the first await .start() or .connect()
    telethon_user_client = TelegramClient('telethon_user_session', API_ID, API_HASH)
    print("Telethon user client initialized.")
except Exception as e:
     print(f"Error initializing Telethon user client: {e}")
     telethon_user_client = None # Ensure it's None if initialization fails


# --- MongoDB setup ---
print("Initializing MongoDB client...")
tclient = AsyncIOMotorClient(MONGO_DB)
tdb = tclient["telegram_bot"]  # Your database
token = tdb["tokens"]  # Your tokens collection
print("MongoDB client initialized.")

# Define async function for TTL index creation
async def create_ttl_index():
    """Ensure the TTL index exists for the `tokens` collection."""
    if MONGO_DB and tclient: # Only try if MONGO_DB is configured and client is initialized
        try:
            # Drop existing index if it exists to avoid errors on re-creation or definition changes
            # Index name is usually collectionfield_direction, but better to list and check
            existing_indexes = await token.index_information()
            if 'expires_at_1' in existing_indexes: # Check for default index name
                 print("Dropping existing TTL index...")
                 await token.drop_index("expires_at_1")
                 print("Existing TTL index dropped.")
            # You might need more sophisticated checks if your index has a custom name

            print("Creating new TTL index...")
            await token.create_index("expires_at", expireAfterSeconds=0)
            print("MongoDB TTL index created successfully.")
        except Exception as e:
             print(f"Error creating MongoDB TTL index: {e}")
    else:
        print("MongoDB_DB not configured or client not initialized. Skipping TTL index creation.")


# Define async function to start all clients
async def initialize_clients():
    """Async function to start Pyrogram and Telethon clients."""
    print("Starting clients asynchronously...")

    # Start Pyrogram bot client
    await app.start()
    print("Pyrogram bot client started.")

    # Start Pyrogram user client
    if pro:
        await pro.start()
        print("Pyrogram user client started.")

    # Start Telethon user client
    if telethon_user_client:
        # Telethon's start() handles connection and authentication flow (will prompt if needed on first run)
        print("Starting Telethon user client...")
        try:
            # If using a session file, just start. If first run, it might wait for auth input.
            # Make sure your environment supports user input during Docker startup if needed.
            # Or authenticate the Telethon session file separately the first time.
            await telethon_user_client.start()
            print("Telethon user client started.")
        except Exception as e:
            print(f"Error starting Telethon user client: {e}")
            # Handle Telethon auth errors or connection issues here
            telethon_user_client = None # Set to None if start fails


    print("Clients initialization complete.")

# Define async function to stop all clients
async def shutdown_clients():
    """Async function to stop Pyrogram and Telethon clients gracefully."""
    print("Stopping clients...")
    await app.stop()
    print("Pyrogram bot client stopped.")
    if pro:
        await pro.stop()
        print("Pyrogram user client stopped.")
    if telethon_user_client:
        await telethon_user_client.disconnect() # Telethon uses disconnect
        print("Telethon user client stopped.")
    print("Clients shutdown complete.")


# --- Export clients and functions to be used in __main__.py and modules ---
__all__ = [
    'app', # Pyrogram bot client
    'pro', # Pyrogram user client
    'telethon_user_client', # Telethon user client
    'tdb', # MongoDB database
    'token', # MongoDB tokens collection
    'initialize_clients', # Async function to start clients
    'shutdown_clients', # Async function to stop clients
    'create_ttl_index', # Async function to create TTL index
    # You might want to add other common utilities or objects here
]
