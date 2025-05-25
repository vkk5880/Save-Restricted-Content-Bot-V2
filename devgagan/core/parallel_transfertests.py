# parallel_downloader.py
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
        max_connections: int = 4,
        chunk_size: int = 1024 * 1024  # 1MB default
    ):
        self.client = client
        self.message = message
        self.speed_limit = speed_limit  # in bytes per second
        self.max_connections = max_connections
        self.chunk_size = chunk_size
        self.file_size = 0
        self.downloaded = 0
        self.start_time = time.time()
        self.last_update = self.start_time
        self.semaphore = asyncio.Semaphore(max_connections)
        self.file_location = None
        self.file_id = None
        self._hash = hashlib.md5()
        
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
        """Download a single chunk with retry logic"""
        retries = 0
        max_retries = 3
        
        while retries < max_retries:
            try:
                result = await self.client.invoke(
                    functions.upload.GetFile(
                        location=self.file_location,
                        offset=offset,
                        limit=chunk_size
                    )
                )
                return result.bytes
            except FloodWait as e:
                logger.warning(f"Flood wait: Sleeping {e.value} seconds")
                await asyncio.sleep(e.value)
                retries += 1
            except BadRequest as e:
                logger.error(f"Bad request error: {e}")
                retries += 1
                await asyncio.sleep(2 ** retries)
                
        raise Exception(f"Failed to download chunk after {max_retries} retries")

    def _calculate_chunk_size(self) -> int:
        """Dynamically adjust chunk size based on file size"""
        if self.file_size > 100 * 1024 * 1024:  # >100MB files
            return max(self.chunk_size, 4 * 1024 * 1024)  # 4MB chunks
        return min(self.chunk_size, self.file_size)

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

    async def _progress_callback(
        self, 
        current: int, 
        total: int,
        callback: Optional[Callable] = None
    ) -> None:
        """Handle progress updates"""
        if callback:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(current, total)
                else:
                    callback(current, total)
            except Exception as e:
                logger.error(f"Progress callback error: {e}")

    async def download(
        self,
        file_path: str,
        progress_callback: Optional[Callable] = None
    ) -> str:
        """Main download method"""
        await self._get_file_location()
        self.chunk_size = self._calculate_chunk_size()
        
        chunks = self.file_size // self.chunk_size
        if self.file_size % self.chunk_size != 0:
            chunks += 1

        logger.info(
            f"Starting download: {self.file_size} bytes "
            f"in {chunks} chunks ({self.chunk_size} bytes/chunk)"
        )

        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        try:
            with open(file_path, "wb") as f:
                offsets = [(i * self.chunk_size, min(self.chunk_size, self.file_size - i * self.chunk_size))
                          for i in range(chunks)]

                tasks = []
                for offset, limit in offsets:
                    task = asyncio.create_task(
                        self._download_chunk(offset, limit, f, progress_callback)
                    )
                    tasks.append(task)

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
            
            await self._progress_callback(self.downloaded, self.file_size, progress_callback)

    @property
    def md5_hash(self) -> str:
        """Get MD5 hash of downloaded file"""
        return self._hash.hexdigest()

    @property
    def download_speed(self) -> float:
        """Calculate current download speed"""
        elapsed = time.time() - self.start_time
        return self.downloaded / elapsed if elapsed > 0 else 0

async def download_file(
    client: Client,
    message: Message,
    file_path: str,
    progress_callback: Optional[Callable] = None,
    speed_limit: Optional[Union[int, float]] = None,  # in MB/s
    max_connections: int = 4
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

# Example usage
"""async def main():
    client = Client(
        "my_account",
        api_id=os.getenv("API_ID"),
        api_hash=os.getenv("API_HASH")
    )
    
    async with client:
        # Replace with actual message ID and chat ID
        message = await client.get_messages("me", 1)
        
        def progress(current, total):
            print(f"Downloaded {current / 1024 / 1024:.2f}MB of {total / 1024 / 1024:.2f}MB")
        
        file_path = await download_file(
            client=client,
            message=message,
            file_path="./downloads/sample.file",
            progress_callback=progress,
            speed_limit=10,  # 10 MB/s
            max_connections=8
        )
        
        print(f"File downloaded to: {file_path}")
        print(f"MD5 checksum: {hashlib.md5(open(file_path, 'rb').read()).hexdigest()}")

if __name__ == "__main__":
    asyncio.run(main())"""
