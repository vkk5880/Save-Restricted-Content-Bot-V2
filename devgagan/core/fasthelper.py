import sys
import os
import pathlib
import time
import datetime as dt
import inspect
from telethon.tl.types import InputDocumentFileLocation

sys.path.insert(0, f"{pathlib.Path(__file__).parent.resolve()}")

from devgagan.core.parallel_transfer import download_file, ParallelTransferrer
from devgagan.core.parallel_transfer import upload_file

class Timer:
    def __init__(self, time_between=5):
        self.start_time = time.time()
        self.time_between = time_between

    def can_send(self):
        if time.time() > (self.start_time + self.time_between):
            self.start_time = time.time()
            return True
        return False

def progress_bar_str(done, total):
    percent = round(done/total*100, 2)
    strin = "░░░░░░░░░░"
    strin = list(strin)
    for i in range(round(percent)//10):
        strin[i] = "█"
    strin = "".join(strin)
    final = f"Percent: {percent}%\n{human_readable_size(done)}/{human_readable_size(total)}\n{strin}"
    return final 

def human_readable_size(size, decimal_places=2):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB', 'PB']:
        if size < 1024.0 or unit == 'PB':
            break
        size /= 1024.0
    return f"{size:.{decimal_places}f} {unit}"


async def safe_turbo_download(client, msg, reply=None, download_folder=None, progress_bar_function=progress_bar_str):
    """
    Safe parallel file download with progress updates
    
    Args:
        client: TelegramClient instance
        msg: Message object containing the document
        reply: Message object to edit with progress (optional)
        download_folder: Custom download directory (optional)
        progress_bar_function: Function to format progress bar
    """
    timer = Timer()
    file = msg.document
    filename = msg.file.name or "file_" + str(int(time.time()))
    
    # Set download directory
    dir = "downloads/"
    try:
        os.makedirs(dir, exist_ok=True)
    except Exception as e:
        raise Exception(f"Failed to create download directory: {e}")

    download_location = os.path.join(download_folder or dir, filename)

    # Get direct file location
    file_loc = InputDocumentFileLocation(
        id=file.id,
        access_hash=file.access_hash,
        file_reference=file.file_reference,
        thumb_size=''
    )

    async def progress_bar(downloaded_bytes, total_bytes):
        if timer.can_send() and reply:
            data = progress_bar_function(downloaded_bytes, total_bytes)
            try:
                await reply.edit(f"Downloading...\n{data}")
            except:
                pass  # Silently handle edit errors

    # Initialize parallel transfer - will use defaults from parallel_transfer.py
    transfer = ParallelTransferrer(client)
    downloaded_bytes = 0
    
    try:
        with open(download_location, 'wb') as f:
            async for chunk in transfer.download(
                file_loc,
                file_size=file.size
                # Uses connection_count and part_size_kb from parallel_transfer.py
            ):
                f.write(chunk)
                downloaded_bytes += len(chunk)
                
                # Progress reporting
                if reply:
                    if inspect.iscoroutinefunction(progress_bar):
                        await progress_bar(downloaded_bytes, file.size)
                    else:
                        progress_bar(downloaded_bytes, file.size)
    
    except Exception as e:
        # Cleanup failed download
        if os.path.exists(download_location):
            os.remove(download_location)
        raise Exception(f"Download failed: {e}")
    
    return download_location



async def fast_download(client, msg, reply = None, download_folder = None, progress_bar_function = progress_bar_str):
    timer = Timer()

    async def progress_bar(downloaded_bytes, total_bytes):
        if timer.can_send():
            data = progress_bar_function(downloaded_bytes, total_bytes)
            await reply.edit(f"Downloading...\n{data}")

    file = msg.document
    filename = msg.file.name
    dir = "downloads/"

    try:
        os.mkdir("downloads/")
    except:
        pass

    if not filename:
        filename = "video.mp4"
                    
    if download_folder == None:
        download_location = dir + filename
    else:
        download_location = download_folder + filename 

    with open(download_location, "wb") as f:
        if reply != None:
            await download_file(
                client=client, 
                location=file, 
                out=f,
                progress_callback=progress_bar
            )
        else:
            await download_file(
                client=client, 
                location=file, 
                out=f,
            )
    return download_location

async def fast_upload(client, file_location, reply=None, name=None, progress_bar_function = progress_bar_str):
    timer = Timer()
    if name == None:
        name = file_location.split("/")[-1]
    async def progress_bar(downloaded_bytes, total_bytes):
        if timer.can_send():
            data = progress_bar_function(downloaded_bytes, total_bytes)
            await reply.edit(f"Uploading...\n{data}")
    if reply != None:
        with open(file_location, "rb") as f:
            the_file = await upload_file(
                client=client,
                file=f,
                name=name,
                progress_callback=progress_bar
            )
    else:
        with open(file_location, "rb") as f:
            the_file = await upload_file(
                client=client,
                file=f,
                name=name,
            )
        
    return the_file
