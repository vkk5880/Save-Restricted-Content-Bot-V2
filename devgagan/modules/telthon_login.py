# ---------------------------------------------------
# File Name: combined_login.py
# Description: Combined Pyrogram + Telethon login system
# Author: Gagan
# GitHub: https://github.com/devgaganin/
# Telegram: https://t.me/team_spy_pro
# YouTube: https://youtube.com/@dev_gagan
# Created: 2025-01-11
# Last Modified: 2025-05-07
# Version: 2.0.7
# License: MIT License
# ---------------------------------------------------
from devgagan import app
from pyrogram import Client, filters
from telethon import TelegramClient
from telethon.sessions import StringSession
import os
import pytz
from datetime import datetime
from config import API_ID, API_HASH, OWNER_ID
from devgagan.core.mongo import db
from devgagan.core.func import subscribe
import logging 

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def check_pyrogram_login(user_id):
    """Check if user has Pyrogram login"""
    user_data = await db.user_sessions_real.find_one({"user_id": user_id})
    return bool(user_data and user_data.get("session_string"))

async def save_telethon_session(user_id, session_string, phone_number):
    """Save Telethon session to database"""
    ist = pytz.timezone('Asia/Kolkata')
    current_time = datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S")
    
    update_data = {
        "telethon_session_string": session_string,
        "telethon_phone": phone_number,
        "last_telethon_login": current_time
    }
    
    await db.user_sessions_real.update_one(
        {"user_id": user_id},
        {"$set": update_data},
        upsert=True
    )

@app.on_message(filters.command("telethon_login"))
async def telethon_login_handler(client, message):
    """Handle Telethon login only after Pyrogram login"""
    user_id = message.chat.id
    
    # Check Pyrogram login first
    if not await check_pyrogram_login(user_id):
        await message.reply("🔒 You must login with Pyrogram first using /login")
        return
    
    # Check subscription
    joined = await subscribe(client, message)
    if joined == 1:
        return
    
    # Start Telethon login process
    phone_msg = await client.ask(
        user_id,
        '📱 Enter your phone number for Telethon login (with country code):\nExample: +919876543210',
        filters=filters.text
    )
    phone_number = phone_msg.text.strip()

    try:
        await message.reply("📲 Sending OTP via Telethon...")
        telethon_client = TelegramClient(
            f"telethon_session_{user_id}", 
            API_ID, 
            API_HASH
        )
        await telethon_client.connect()
        
        sent_code = await telethon_client.send_code_request(phone_number)
        
        otp_msg = await client.ask(
            user_id,
            "🔢 Enter the Telethon OTP you received (format: 1 2 3 4 5)",
            filters=filters.text,
            timeout=300
        )
        phone_code = otp_msg.text.replace(" ", "")
        
        try:
            await telethon_client.sign_in(
                phone_number,
                code=phone_code,
                phone_code_hash=sent_code.phone_code_hash
            )
        except SessionPasswordNeeded:
            password_msg = await client.ask(
                user_id,
                "🔐 Your account has 2FA. Enter your password:",
                filters=filters.text,
                timeout=300
            )
            await telethon_client.sign_in(password=password_msg.text)
        
        # Get session string and save
        session_string = StringSession.save(telethon_client.session)
        await db.set_telethon_session(user_id, session_string)
        await save_telethon_session(user_id, session_string, phone_number)
        
        await message.reply(
            "✅ Telethon login successful!\n"
            "🚀 You can now use both Pyrogram and Telethon features."
        )
        
    except Exception as e:
        logger.error(f"Telethon login failed for {user_id}: {e}")
        await message.reply(f"❌ Telethon login failed: {str(e)}")
    finally:
        try:
            await telethon_client.disconnect()
        except:
            pass

@app.on_message(filters.command("logout_all"))
async def logout_all_handler(client, message):
    """Logout from both Pyrogram and Telethon"""
    user_id = message.chat.id
    
    # Delete Pyrogram session
    pyro_file = f"session_{user_id}.session"
    if os.path.exists(pyro_file):
        os.remove(pyro_file)
    
    # Delete Telethon session
    telethon_file = f"telethon_session_{user_id}.session"
    if os.path.exists(telethon_file):
        os.remove(telethon_file)
    
    # Clear database entries
    await db.user_sessions_real.update_one(
        {"user_id": user_id},
        {"$set": {
            "session_string": None,
            "telethon_session_string": None
        }}
    )
    
    await message.reply("✅ Successfully logged out from both")
