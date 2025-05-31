# ---------------------------------------------------
# File Name: db.py tel_db = db.users_data_tel_db  # Setting the database
# Description: MongoDB operations for Pyrogram bot
# Author: Gagan
# GitHub: https://github.com/devgaganin/
# Telegram: https://t.me/team_spy_pro
# YouTube: https://youtube.com/@dev_gagan
# Created: 2025-01-11
# Last Modified: 2025-01-11
# Version: 2.0.5
# License: MIT License
# ---------------------------------------------------

from config import MONGO_DB
from motor.motor_asyncio import AsyncIOMotorClient as MongoCli
import json # Import json for printing in get_sessions
import os # Import os for cleanup in upload_media if it's in this file (though unlikely)
import gc
# Initialize MongoDB Client
mongo = MongoCli(MONGO_DB)
database = mongo.user_data
db = database.users_data_db  # Setting the database

# ✅ Corrected way to get collection reference
user_sessions_real = db["user_sessions_real"]  # ✅ Corrected syntax

# Function to get user data
async def get_data(user_id):
    return await db.find_one({"_id": user_id})

# Function to set thumbnail for user
async def set_thumbnail(user_id, thumb):
    data = await get_data(user_id)
    if data:
        await db.update_one({"_id": user_id}, {"$set": {"thumb": thumb}})
    else:
        await db.insert_one({"_id": user_id, "thumb": thumb})

# Function to set caption for user
async def set_caption(user_id, caption):
    data = await get_data(user_id)
    if data:
        await db.update_one({"_id": user_id}, {"$set": {"caption": caption}})
    else:
        await db.insert_one({"_id": user_id, "caption": caption})

# Function to replace caption text
async def replace_caption(user_id, replace_txt, to_replace):
    data = await get_data(user_id)
    if data:
        await db.update_one({"_id": user_id}, {"$set": {"replace_txt": replace_txt, "to_replace": to_replace}})
    else:
        await db.insert_one({"_id": user_id, "replace_txt": replace_txt, "to_replace": to_replace})

# Function to set user session
# Function to set Pyrogram session
async def set_session(user_id, session):
    """Set Pyrogram session string in database"""
    await db.update_one(
        {"_id": user_id},
        {"$set": {"session": session}},
        upsert=True
    )


async def save_userbot_token(user_id, token_string):
    """Set save_userbot_token string in database"""
    await db.update_one(
        {"_id": user_id},
        {"$set": {"userbot_token": token_string}},
        upsert=True
    )



# Function to set Telethon session
async def set_telethon_session(user_id, telethon_session_string):
    """Set Telethon session string in database"""
    await db.update_one(
        {"_id": user_id},
        {"$set": {"telethon_session_string": telethon_session_string}},
        upsert=True
    )

# Function to get both sessions
async def get_sessionsss(user_id):
    """Get both Pyrogram and Telethon sessions"""
    data = await db.find_one({"_id": user_id})
    if data:
        return {
            "pyro_session": data.get("session"),
            "telethon_session": data.get("telethon_session_string")
        }
    return None



async def get_sessions(user_id):
    """
    Retrieve and print both Pyrogram and Telethon sessions
    Returns dict with sessions or None if not found
    """
    try:
        print(f"🔍 Fetching sessions for user {user_id}...")
        data = await db.find_one({"_id": user_id})
        
        if not data:
            print("❌ No session data found in database")
            return None

        # Print complete document for debugging
        print("📄 Full database document:")
        print(json.dumps(data, indent=2, default=str))

        # Extract sessions
        sessions = {
            "userbot_token": data.get("userbot_token"),
            "pyro_session": data.get("session"),
            "telethon_session": data.get("telethon_session_string"),
            "has_pyro": bool(data.get("session")),
            "has_telethon": bool(data.get("telethon_session_string"))
        }

        print("\n🔑 Extracted sessions:")
        print(f"Pyrogram: {'✅' if sessions['has_pyro'] else '❌'}")
        print(f"Telethon: {'✅' if sessions['has_telethon'] else '❌'}")

        if sessions['pyro_session']:
            print(f"\nPyrogram session (first 10 chars): {sessions['pyro_session'][:10]}...")
        if sessions['telethon_session']:
            print(f"Telethon session (first 10 chars): {sessions['telethon_session'][:10]}...")

        return sessions

    except Exception as e:
        print(f"⚠️ Error fetching sessions: {e}")
        return None

# Function to check if Pyrogram session exists
async def has_pyro_session(user_id):
    """Check if user has Pyrogram session"""
    data = await db.find_one({"_id": user_id})
    return bool(data and data.get("session"))

# Function to check if Telethon session exists
async def has_telethon_session(user_id):
    """Check if user has Telethon session"""
    data = await db.find_one({"_id": user_id})
    return bool(data and data.get("telethon_session_string"))

# Function to remove Pyrogram session
async def remove_pyro_session(user_id):
    """Remove Pyrogram session"""
    await db.update_one(
        {"_id": user_id},
        {"$unset": {"session": ""}}
    )

# Function to remove Telethon session
async def remove_telethon_session(user_id):
    """Remove Telethon session"""
    await db.update_one(
        {"_id": user_id},
        {"$unset": {"telethon_session_string": ""}}
    )

# Function to remove both sessions
async def remove_all_sessions(user_id):
    """Remove both Pyrogram and Telethon sessions"""
    await db.update_one(
        {"_id": user_id},
        {"$unset": {
            "session": "",
            "telethon_session_string": ""
        }}
    )


# Function to add new clean words to user data
async def clean_words(user_id, new_clean_words):
    data = await get_data(user_id)
    if data:
        existing_words = data.get("clean_words", []) or []
        updated_words = list(set(existing_words + new_clean_words))
        await db.update_one({"_id": user_id}, {"$set": {"clean_words": updated_words}})
    else:
        await db.insert_one({"_id": user_id, "clean_words": new_clean_words})

# Function to remove specific clean words
async def remove_clean_words(user_id, words_to_remove):
    data = await get_data(user_id)
    if data:
        existing_words = data.get("clean_words", []) or []
        updated_words = [word for word in existing_words if word not in words_to_remove]
        await db.update_one({"_id": user_id}, {"$set": {"clean_words": updated_words}})
    else:
        await db.insert_one({"_id": user_id, "clean_words": []})

# Function to set user channel
async def set_channel(user_id, chat_id):
    data = await get_data(user_id)
    if data:
        await db.update_one({"_id": user_id}, {"$set": {"chat_id": chat_id}})
    else:
        await db.insert_one({"_id": user_id, "chat_id": chat_id})

# Function to remove all words
async def all_words_remove(user_id):
    await db.update_one({"_id": user_id}, {"$set": {"clean_words": None}})

# Function to remove user thumbnail
async def remove_thumbnail(user_id):
    await db.update_one({"_id": user_id}, {"$set": {"thumb": None}})

# Function to remove user caption
async def remove_caption(user_id):
    await db.update_one({"_id": user_id}, {"$set": {"caption": None}})

# Function to remove replace text fields
async def remove_replace(user_id):
    await db.update_one({"_id": user_id}, {"$set": {"replace_txt": None, "to_replace": None}})

# Function to remove user session
async def remove_session(user_id):
    await db.update_one({"_id": user_id}, {"$set": {"session": None}})

# Function to remove user channel
async def remove_channel(user_id):
    await db.update_one({"_id": user_id}, {"$set": {"chat_id": None}})

# Function to delete session from database
async def delete_session(user_id):
    """Delete the session associated with the given user_id from the database."""
    await db.update_one({"_id": user_id}, {"$unset": {"session": ""}})

# ✅ Functions for user_sessions_real

# Function to save user session in `user_sessions_real`
async def save_user_session(user_id, session_string):
    """Save user session in the new user_sessions_real collection."""
    await user_sessions_real.insert_one({"user_id": user_id, "session_string": session_string})

# Function to get user session from `user_sessions_real`
async def get_user_session(user_id):
    """Retrieve user session from the user_sessions_real collection."""
    return await user_sessions_real.find_one({"user_id": user_id})

# Function to remove user session from `user_sessions_real`
async def remove_user_session(user_id):
    """Remove user session from the new user_sessions_real collection."""
    await user_sessions_real.delete_one({"user_id": user_id})
