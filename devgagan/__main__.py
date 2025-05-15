# In your __main__.py file

# ---------------------------------------------------
# File Name: __main__.py
# Description: Bot startup and main execution logic
# Author: Gagan
# GitHub: https://github.com/devgaganin/
# Telegram: https://t.me/team_spy_pro
# YouTube: https://youtube.com/@dev_gagan
# Created: 2025-01-11
# Last Modified: 2025-05-15 # Updated last modified date
# Version: 2.0.6 # Incremented version
# License: MIT License
# ---------------------------------------------------

import asyncio
import importlib
import gc
# Import uvloop
import uvloop
# Import Pyrogram idle function
from pyrogram import idle

# Import clients, DB objects, and startup/shutdown functions from devgagan.__init__.py
from devgagan import (
    app,                # Pyrogram bot client
    pro,                # Pyrogram user client
    telethon_user_client, # Telethon user client
    tdb,                # MongoDB database
    token,              # MongoDB tokens collection
    initialize_clients, # Async function to start all clients
    shutdown_clients,   # Async function to stop all clients
    create_ttl_index    # Async function to create TTL index
)
from devgagan.modules import ALL_MODULES # Assuming ALL_MODULES is a list of module names
from devgagan.core.mongo.plans_db import check_and_remove_expired_users
# Import create_scheduler from aiojobs if you use it here
from aiojobs import create_scheduler


# ----------------------------Bot-Start---------------------------- #

# --- Add uvloop installation here, as early as possible ---
uvloop.install()
print("uvloop installed.")
# -----------------------------------------------------------

# Use asyncio.run() as the main entry point, it manages the loop
# loop = asyncio.get_event_loop() # REMOVE or comment out this line

# Function to schedule expiry checks (runs as a background task)
async def schedule_expiry_check():
    # Create scheduler instance within the async function
    scheduler = await create_scheduler()
    print("Scheduler created.")
    while True:
        # Note: The comment in your code says "Check every hour", but sleep is 60s.
        # Adjust asyncio.sleep duration if needed (e.g., 3600 for 1 hour).
        print("Spawning expiry check job...")
        # The job runs concurrently, schedule_expiry_check task just waits 60s then spawns the next
        await scheduler.spawn(check_and_remove_expired_users(tdb, token)) # Pass db/collection if needed by check_and_remove_expired_users
        print("Expiry check job spawned.")
        await asyncio.sleep(60)  # Sleep for 1 minute between spawning jobs
        # gc.collect() # Usually not needed explicitly

async def devggn_boot():
    print("Starting bot initialization (devggn_boot)...")

    # --- Database Setup ---
    # Run MongoDB TTL index creation
    await create_ttl_index()
    print("Database setup complete.")


    # --- Initialize and Start Clients (Pyrogram, Telethon) ---
    # This function is now in __init__.py and handles starting all clients
    await initialize_clients()
    print("Clients started.")

    # --- Import Modules ---
    # Import modules *after* clients are started so handlers can access clients (app, pro, telethon_user_client)
    print("Importing modules...")
    for all_module in ALL_MODULES:
        try:
            importlib.import_module("devgagan.modules." + all_module)
            print(f"Imported module: devgagan.modules.{all_module}")
        except Exception as e:
             print(f"Error importing module devgagan.modules.{all_module}: {e}")
             # Decide how to handle import errors - maybe log and continue or exit


    print("""
---------------------------------------------------
📂 Bot Deployed successfully ...
📝 Description: A Pyrogram bot for downloading files from Telegram channels or groups
                and uploading them back to Telegram.
👨‍💻 Author: Adarsh # Note: Header says Gagan, print says Adarsh - potential inconsistency
📬 Telegram: https://t.me/Contact_xbot # Note: Header says team_spy_pro, print says Contact_xbot - inconsistency
🗓️ Created: 2025-01-11
🔄 Last Modified: 2025-05-15 # Updated last modified date
🛠️ Version: 2.0.6 # Incremented version
📜 License: MIT License
---------------------------------------------------
""")

    # --- Schedule background tasks ---
    print("Scheduling background tasks...")
    # Create the background task
    background_task = asyncio.create_task(schedule_expiry_check())
    print("Auto removal scheduler task created.")

    print("Bot is now running and idle...")
    # --- Keep the bot running and responsive ---
    # Use Pyrogram's idle() to wait for termination signals.
    # This keeps the main Pyrogram client (app) alive.
    # Background tasks and other clients need to manage their own loops/lifecycle or be awaited here if they are the main task.
    # idle() is designed for Pyrogram.
    await idle()

    print("Idle stopped. Starting shutdown...")
    # --- Shutdown clients gracefully ---
    # This function is now in __init__.py and handles stopping all clients
    await shutdown_clients()

    # Cancel the background task if it's still running
    if background_task and not background_task.done():
         print("Cancelling background scheduler task...")
         background_task.cancel()
         try:
             await background_task # Wait for cancellation
         except asyncio.CancelledError:
             print("Background scheduler task cancelled.")
         except Exception as e:
             print(f"Error during background task cancellation: {e}")


    print("Bot execution finished.")


if __name__ == "__main__":
    # --- Main entry point ---
    # Use asyncio.run() to execute the main async function and manage the event loop lifecycle.
    # uvloop is already installed at the top.
    print("Running bot with asyncio.run()...")
    try:
        asyncio.run(devggn_boot())
    except KeyboardInterrupt:
        print("Bot execution interrupted by user.")
    except Exception as e:
        print(f"An unexpected error occurred during bot execution: {e}")
        # You might want to log this exception before exiting

# ------------------------------------------------------------------ #
