from client_manager import client_manager
from upload_methods import pyrogram_download, telethon_download, pyrogram_upload, telethon_upload
from devgagan.core.func import *

async def handle_command(user_id, message):
    try:
        # Initialize the correct client
        upload_method, client = await client_manager.initialize(user_id)
        
        if upload_method == "Pyrogram":
            # Pyrogram handling
            msg = await pyrogram_download(
                client,
                message,
                progress_callback=progress_bar,
                progress_args=("Downloading with Pyrogram...", None, time.time())
            )
            
            # Process the file
            file = await rename_file(msg.download_media(), user_id)
            caption = await get_final_caption(message, user_id)
            
            # Upload with Pyrogram
            await pyrogram_upload(
                client,
                user_id,
                message.chat.id,
                file,
                caption=caption
            )
            
        else:
            # Telethon handling
            msg = await telethon_download(
                client,
                message,
                user_id,
                progress_callback=dl_progress_callback
            )
            
            # Process the file
            file = await rename_file(msg, user_id)
            caption = await get_final_caption(message, user_id)
            
            # Upload with Telethon
            await telethon_upload(
                client,
                user_id,
                message.chat.id,
                file,
                caption=caption
            )
            
    except Exception as e:
        print(f"Error handling command: {e}")
    finally:
        if 'file' in locals() and os.path.exists(file):
            os.remove(file)
