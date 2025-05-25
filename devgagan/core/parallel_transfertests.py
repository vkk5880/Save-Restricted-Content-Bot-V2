# parallel_downloader_fixed.py
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
logger = logging.getLogger("ParallelPyrogramDownloader")

class ParallelPyrogramDownloader:
    def __init__(
        self,
        client: Client,
        message: Message,
        speed_limit: Optional[int] = None,
        max_connections: int = 5,
        chunk_size: int = 512 * 1024  # 512KB default (safe for most files)
    ):
        self.client = client
        self.message = message
        self.speed_limit = speed_limit  # in bytes per second
        self.max_connections = max_connections
        self.base_chunk_size = min(chunk_size, 512 * 1024)  # Never exceed 512KB initially
        self.file_size = 0
        self.downloaded = 0
        self.start_time = time.time()
        self.last_update = self.start_time
        self.semaphore = asyncio.Semaphore(max_connections)
        self.file_location = None
        self.file_id = None
        self._hash = hashlib.md5()
        self.adaptive_chunk_size = self.base_chunk_size
        
    async def _get_file_location(self) -> None:
        """Extract file location from Pyrogram message"""
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
            photo = self.message.photo
            file_id = FileId.decode(photo.file_id)
            self.file_location = types.InputPhotoFileLocation(
                id=file_id.media_id,
                access_hash=file_id.access_hash,
                file_reference=file_id.file_reference,
                thumb_size=""
            )
            self.file_size = photo.file_size
        else:
            raise ValueError("Unsupported message type for download")

    async def _get_chunk(self, offset: int, chunk_size: int) -> bytes:
        """Download a single chunk with retry and adaptive chunk sizing"""
        retries = 0
        max_retries = 3
        
        while retries < max_retries:
            try:
                # Ensure we don't request more than the remaining bytes
                actual_limit = min(chunk_size, self.file_size - offset)
                
                result = await self.client.invoke(
                    functions.upload.GetFile(
                        location=self.file_location,
                        offset=offset,
                        limit=actual_limit
                    ),
                    sleep_threshold=0
                )
                
                # If successful, consider increasing chunk size
                if retries == 0 and len(result.bytes) == chunk_size:
                    self.adaptive_chunk_size = min(
                        self.adaptive_chunk_size * 2,
                        1024 * 1024  # Max 1MB even if successful
                    )
                
                return result.bytes
                
            except FloodWait as e:
                logger.warning(f"Flood wait: Sleeping {e.value} seconds")
                await asyncio.sleep(e.value)
                retries += 1
            except BadRequest as e:
                if "LIMIT_INVALID" in str(e):
                    # Reduce chunk size on LIMIT_INVALID error
                    self.adaptive_chunk_size = max(
                        128 * 1024,  # Minimum 128KB
                        self.adaptive_chunk_size // 2
                    )
                    logger.warning(f"Reducing chunk size to {self.adaptive_chunk_size} bytes due to LIMIT_INVALID")
                    chunk_size = self.adaptive_chunk_size
                    actual_limit = min(chunk_size, self.file_size - offset)
                    retries += 1
                    await asyncio.sleep(1)
                else:
                    logger.error(f"Bad request error: {e}")
                    retries += 1
                    await asyncio.sleep(2 ** retries)
                
        raise Exception(f"Failed to download chunk after {max_retries} retries")

    async def _limit_speed(self, chunk_length: int) -> None:
        """Enforce speed limit if specified"""
        if not self.speed_limit:
            return
            
        elapsed = time.time() - self.last_update
        expected_time = chunk_length / self.speed_limit
        
        if elapsed < expected_time:
            sleep_time = expected_time - elapsed
            logger.debug(f"Speed limit sleep: {sleep_time:.2f}s")
            await asyncio.sleep(sleep_time)
            
        self.last_update = time.time()

    async def download(
        self,
        file_path: str,
        progress_callback: Optional[Callable] = None
    ) -> str:
        """Main download method"""
        await self._get_file_location()
        
        # For small files, use single connection and full file size
        if self.file_size <= 512 * 1024:  # 512KB or less
            self.max_connections = 1
            self.adaptive_chunk_size = self.file_size

        chunks = (self.file_size + self.adaptive_chunk_size - 1) // self.adaptive_chunk_size

        logger.info(
            f"Starting download: {self.file_size} bytes "
            f"in {chunks} chunks (adaptive size, starting at {self.adaptive_chunk_size} bytes)"
        )

        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        try:
            with open(file_path, "wb") as f:
                tasks = []
                for i in range(chunks):
                    offset = i * self.adaptive_chunk_size
                    limit = min(self.adaptive_chunk_size, self.file_size - offset)
                    tasks.append(
                        self._download_chunk(offset, limit, f, progress_callback)
                    )

                await asyncio.gather(*tasks)

            logger.info(f"Download complete: {file_path}")
            return file_path

        except Exception as e:
            logger.error(f"Download failed: {e}")
            if os.path.exists(file_path):
                os.remove(file_path)
            raise

    async def _download_chunk(
        self,
        offset: int,
        limit: int,
        file_handle: BinaryIO,
        progress_callback: Optional[Callable] = None
    ) -> None:
        """Download and write a single chunk"""
        async with self.semaphore:
            data = await self._get_chunk(offset, limit)
            await self._limit_speed(len(data))
            
            file_handle.seek(offset)
            file_handle.write(data)
            
            self.downloaded += len(data)
            self._hash.update(data)
            
            if progress_callback:
                try:
                    if asyncio.iscoroutinefunction(progress_callback):
                        await progress_callback(self.downloaded, self.file_size)
                    else:
                        progress_callback(self.downloaded, self.file_size)
                except Exception as e:
                    logger.error(f"Progress callback error: {e}")

async def download_file(
    client: Client,
    message: Message,
    file_path: str,
    progress_callback: Optional[Callable] = None,
    speed_limit: Optional[Union[int, float]] = None,  # in MB/s
    max_connections: int = 3  # More conservative default
) -> str:
    """
    Public interface for parallel file download
    :param speed_limit: Optional speed limit in MB/s
    """
    # Convert MB/s to bytes/s if specified
    byte_speed_limit = int(speed_limit * 1024 * 1024) if speed_limit else None
    
    downloader = ParallelPyrogramDownloader(
        client=client,
        message=message,
        speed_limit=byte_speed_limit,
        max_connections=max_connections
    )
    
    return await downloader.download(file_path, progress_callback)
