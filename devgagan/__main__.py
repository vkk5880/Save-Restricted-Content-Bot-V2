# ---------------------------------------------------
# File Name: __main__.py
# Description: A Pyrogram bot for downloading files from Telegram channels or groups
#              and uploading them back to Telegram.
# Author: Gagan (Original Author Note: Adarsh in banner)
# GitHub: https://github.com/devgaganin/
# Telegram: https://t.me/team_spy_pro (Original Note: Contact_xbot in banner)
# YouTube: https://youtube.com/@dev_gagan
# Created: 2025-01-11
# Last Modified: 2025-05-11 (Updated for modern asyncio and clarity)
# Version: 2.0.6
# License: MIT License
# ---------------------------------------------------

import asyncio
import importlib
import gc
from pyrogram import idle
from devgagan.modules import ALL_MODULES # Assuming this path is correct for your project structure
from devgagan.core.mongo.plans_db import check_and_remove_expired_users # Assuming path is correct
from aiojobs import create_scheduler

# ----------------------------Bot-Start---------------------------- #

# Function to schedule expiry checks
async def schedule_expiry_check():
    """Periodically checks for and removes expired users."""
    # Using await create_scheduler() as per aiojobs documentation for an async context
    scheduler = await create_scheduler()
    print("User expiry check scheduler started.")
    while True:
        try:
            print("Running scheduled job: check_and_remove_expired_users")
            await scheduler.spawn(check_and_remove_expired_users())
            await asyncio.sleep(3600)  # Check every hour (3600 seconds)
            gc.collect() # Optional: explicit garbage collection
        except asyncio.CancelledError:
            print("Expiry check scheduler cancelled.")
            await scheduler.close() # Gracefully close the scheduler
            raise
        except Exception as e:
            print(f"Error in schedule_expiry_check: {e}")
            # Decide on retry logic or how to handle persistent errors
            await asyncio.sleep(60) # Wait a minute before retrying on error


async def devggn_boot():
    """Initializes and starts the bot."""
    print("Importing modules...")
    for all_module in ALL_MODULES:
        try:
            importlib.import_module("devgagan.modules." + all_module)
            print(f"Successfully imported module: {all_module}")
        except ImportError as e:
            print(f"Failed to import module {all_module}: {e}")
            # Consider if a failed module import should halt startup

    # Banner printing - ensure author/contact details are what you intend
    print("""
---------------------------------------------------
📂 Bot Deployed successfully ...
📝 Description: A Pyrogram bot for downloading files from Telegram channels or groups
                 and uploading them back to Telegram.
👨‍💻 Author: Adarsh (as per banner, original file note Gagan)
📬 Telegram: https://t.me/Contact_xbot (as per banner, original file note team_spy_pro)
🗓️ Created: 2025-01-11
🔄 Last Modified: 2025-05-11
🛠️ Version: 2.0.6
📜 License: MIT License
---------------------------------------------------
""")

    # Create and start the background task for checking user expiry
    # This task will run concurrently with the bot's main operations.
    expiry_check_task = asyncio.create_task(schedule_expiry_check())
    print("User expiry removal task scheduled...")

    print("Bot is now idle and listening for updates...")
    await idle()  # Keep the Pyrogram client running

    print("Bot received stop signal. Shutting down...")
    # Cancel background tasks gracefully
    expiry_check_task.cancel()
    try:
        await expiry_check_task # Wait for the task to actually cancel
    except asyncio.CancelledError:
        print("Expiry check task was cancelled successfully.")

    print("Bot stopped.")


if __name__ == "__main__":
    try:
        # asyncio.run() is the modern way to run an asyncio program (Python 3.7+)
        # It handles loop creation, running the future, and closing the loop.
        asyncio.run(devggn_boot())
    except KeyboardInterrupt:
        print("Shutdown requested by user (KeyboardInterrupt).")
    except Exception as main_e:
        print(f"An unexpected error occurred in __main__: {main_e}")

# ------------------------------------------------------------------ #
