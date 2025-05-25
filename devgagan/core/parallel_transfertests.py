# stable_parallel_downloader.py
import asyncio
import os
import time
import hashlib
import logging
from typing import BinaryIO, Optional, Callable, Union

from pyrogram import Client
from pyrogram.types import Message
from pyrogram.errors import FloodWait, BadRequest
from pyrogram.raw import functions, types
from pyrogram.file_id import FileId

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("StablePyrogramDownloader")

class StableDownloader:
    def __init__(
        self,
        client: Client,
        message: Message,
        speed_limit: Optional[int] = None,
        max_connections: int = 3,
        chunk_size: int = 256 * 1024  # Start with 256KB chunks
    ):
        self.client = client
        self.message = message
        self.speed_limit = speed_limit
        self.max_connections = max_connections
        self.base_chunk_size = chunk_size
        self.file_size = 0
        self.downloaded = 0
        self.start_time = time.time()
        self.last_update = self.start_time
        self.file_location = None
        self._hash = hashlib.md5()
        self._active_tasks = set()
        self._lock = asyncio.Lock()
        self._connection_maintained = False

    async def _maintain_connection(self):
        """Ensure the connection stays active during download"""
        while self._connection_maintained:
            try:
                await self.client.send(functions.Ping(ping_id=12345))
                await asyncio.sleep(10)  # Ping every 10 seconds
            except Exception as e:
                logger.warning(f"Ping failed: {e}")
                await asyncio.sleep(1)

    async def _get_file_location(self):
        """Get file location with connection handling"""
        try:
            if self.message.document:
                file_id = FileId.decode(self.message.document.file_id)
                self.file_location = types.InputDocumentFileLocation(
                    id=file_id.media_id,
                    access_hash=file_id.access_hash,
                    file_reference=file_id.file_reference,
                    thumb_size=""
                )
                self.file_size = self.message.document.file_size
            elif self.message.photo:
                file_id = FileId.decode(self.message.photo.file_id)
                self.file_location = types.InputPhotoFileLocation(
                    id=file_id.media_id,
                    access_hash=file_id.access_hash,
                    file_reference=file_id.file_reference,
                    thumb_size=""
                )
                self.file_size = self.message.photo.file_size
        except Exception as e:
            logger.error(f"Failed to get file location: {e}")
            raise

    async def _download_chunk(self, offset: int, limit: int, file_handle: BinaryIO):
        """Download a single chunk with robust error handling"""
        retries = 0
        max_retries = 5
        chunk_size = limit
        
        while retries < max_retries:
            try:
                # Get actual bytes remaining
                actual_limit = min(chunk_size, self.file_size - offset)
                if actual_limit <= 0:
                    return

                result = await self.client.invoke(
                    functions.upload.GetFile(
                        location=self.file_location,
                        offset=offset,
                        limit=actual_limit
                    ),
                    sleep_threshold=0
                )

                async with self._lock:
                    file_handle.seek(offset)
                    file_handle.write(result.bytes)
                    self.downloaded += len(result.bytes)
                    self._hash.update(result.bytes)
                
                return
                
            except FloodWait as e:
                logger.warning(f"Flood wait {e.value}s, retrying...")
                await asyncio.sleep(e.value)
            except BadRequest as e:
                if "LIMIT_INVALID" in str(e):
                    new_size = max(128 * 1024, chunk_size // 2)
                    logger.info(f"Reducing chunk size from {chunk_size} to {new_size}")
                    chunk_size = new_size
                    await asyncio.sleep(1)
                else:
                    logger.error(f"Bad request: {e}")
                    await asyncio.sleep(2 ** retries)
            except Exception as e:
                logger.error(f"Chunk download error: {e}")
                await asyncio.sleep(2 ** retries)
            
            retries += 1
        
        raise Exception(f"Failed after {max_retries} retries")

    async def download(self, file_path: str, progress_callback: Optional[Callable] = None):
        """Main download method with connection maintenance"""
        await self._get_file_location()
        
        # Start connection maintenance
        self._connection_maintained = True
        maintenance_task = asyncio.create_task(self._maintain_connection())

        try:
            with open(file_path, "wb") as f:
                # Initialize file with zeros
                f.truncate(self.file_size)
                
                # Calculate chunks with adaptive sizing
                chunk_size = min(self.base_chunk_size, self.file_size)
                chunks = (self.file_size + chunk_size - 1) // chunk_size
                
                logger.info(f"Downloading {self.file_size} bytes in {chunks} chunks")
                
                # Create download tasks
                tasks = []
                for i in range(chunks):
                    offset = i * chunk_size
                    limit = min(chunk_size, self.file_size - offset)
                    
                    task = asyncio.create_task(
                        self._download_chunk(offset, limit, f)
                    )
                    tasks.append(task)
                    self._active_tasks.add(task)
                    task.add_done_callback(self._active_tasks.discard)
                    
                    # Progress reporting
                    if progress_callback and i % 10 == 0:
                        await self._report_progress(progress_callback)
                
                await asyncio.gather(*tasks)
                await self._report_progress(progress_callback)
                
            logger.info(f"Download completed: {file_path}")
            return file_path
            
        finally:
            # Cleanup
            self._connection_maintained = False
            await maintenance_task
            for task in self._active_tasks:
                task.cancel()
            await asyncio.sleep(0.1)

    async def _report_progress(self, callback: Callable):
        """Report progress to callback"""
        try:
            if asyncio.iscoroutinefunction(callback):
                await callback(self.downloaded, self.file_size)
            else:
                callback(self.downloaded, self.file_size)
        except Exception as e:
            logger.error(f"Progress callback error: {e}")

async def stable_download(
    client: Client,
    message: Message,
    file_path: str,
    progress_callback: Optional[Callable] = None,
    speed_limit: Optional[float] = None,
    max_connections: int = 3
) -> str:
    """Stable download interface"""
    downloader = StableDownloader(
        client=client,
        message=message,
        speed_limit=int(speed_limit * 1024 * 1024) if speed_limit else None,
        max_connections=max_connections
    )
    return await downloader.download(file_path, progress_callback)
