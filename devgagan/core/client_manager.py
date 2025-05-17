import asyncio
from pyrogram import Client
from telethon import TelegramClient
from config import API_ID, API_HASH, STRING, MONGO_DB
import pymongo

mongo_client = pymongo.MongoClient(MONGO_DB)
db = mongo_client["smart_users"]
collection = db["super_user"]

class ClientManager:
    def __init__(self):
        self.pyro_client = None
        self.telethon_client = None
        self.active_client = None
        
    async def initialize(self, user_id):
        """Initialize the appropriate client based on user preference"""
        user_data = collection.find_one({"user_id": user_id})
        upload_method = user_data.get("upload_method", "Pyrogram") if user_data else "Pyrogram"
        
        if upload_method == "Pyrogram":
            if not self.pyro_client:
                self.pyro_client = Client(
                    "pyro_session",
                    api_id=API_ID,
                    api_hash=API_HASH,
                    in_memory=True
                )
                await self.pyro_client.start()
            self.active_client = self.pyro_client
        else:
            if not self.telethon_client:
                self.telethon_client = TelegramClient(
                    StringSession(STRING),
                    API_ID,
                    API_HASH
                )
                await self.telethon_client.start()
            self.active_client = self.telethon_client
            
        return upload_method, self.active_client

    async def shutdown(self):
        """Properly shutdown all clients"""
        if self.pyro_client:
            await self.pyro_client.stop()
        if self.telethon_client:
            await self.telethon_client.disconnect()

client_manager = ClientManager()
