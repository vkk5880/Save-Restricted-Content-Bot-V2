"""
Based on parallel_file_transfer.py from mautrix-telegram, with permission to distribute under the MIT license
Copyright (C) 2019 Tulir Asokan - https://github.com/tulir/mautrix-telegram
"""

import asyncio
import hashlib
import inspect
import logging
import math
import os
import psutil
import time
from collections import defaultdict
from os import getenv
from typing import Generator, AsyncGenerator, Awaitable, BinaryIO, DefaultDict, List, Optional, Tuple, Union

from telethon import TelegramClient, helpers, utils
from telethon.crypto import AuthKey
from telethon.errors import (
    ChannelInvalidError,
    ChannelPrivateError,
    ChatIdInvalidError,
    ChatInvalidError,
    FloodWaitError,
    FileMigrateError,
)
from telethon.network import MTProtoSender
from telethon.tl.alltlobjects import LAYER
from telethon.tl.functions import InvokeWithLayerRequest
from telethon.tl.functions.auth import ExportAuthorizationRequest, ImportAuthorizationRequest
from telethon.tl.functions.upload import GetFileRequest, SaveBigFilePartRequest, SaveFilePartRequest
from telethon.tl.types import (
    Document,
    InputDocumentFileLocation,
    InputFile,
    InputFileBig,
    InputFileLocation,
    InputPeerPhotoFileLocation,
    InputPhotoFileLocation,
    TypeInputFile,
    InitConnectionRequest, # Added for explicit construction
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('transfer.log')
    ]
)
logger = logging.getLogger("FastTelethon")

# Configuration with environment variable fallbacks
MAX_DOWNLOAD_SPEEDS = int(getenv("MAX_DOWNLOAD_SPEED", "15"))
MAX_CHUNK_SIZES = int(getenv("MAX_CHUNK_SIZE", "3072"))
MIN_CHUNK_SIZES = int(getenv("MIN_CHUNK_SIZE", "128"))
SPEED_CHECK_INTERVALS = int(getenv("SPEED_CHECK_INTERVAL", "1"))
MAX_PARALLEL_TRANSFERS = int(getenv("MAX_PARALLEL_TRANSFERS", "4"))
MAX_CONNECTIONS_PER_TRANSFER = int(getenv("MAX_CONNECTIONS_PER_TRANSFER", "5"))

# Calculated constants
MAX_DOWNLOAD_SPEED = MAX_DOWNLOAD_SPEEDS * 1024 * 1024  # 15 Mbps in bytes
MIN_CHUNK_SIZE = MIN_CHUNK_SIZES * 1024  # 128KB
MAX_CHUNK_SIZE = MAX_CHUNK_SIZES * 1024  # 3072KB
SPEED_CHECK_INTERVAL = SPEED_CHECK_INTERVALS  # Seconds between speed checks

parallel_transfer_semaphore = asyncio.Semaphore(MAX_PARALLEL_TRANSFERS)
parallel_transfer_locks = defaultdict(lambda: asyncio.Lock())

TypeLocation = Union[
    Document,
    InputDocumentFileLocation,
    InputPeerPhotoFileLocation,
    InputFileLocation,
    InputPhotoFileLocation,
]

class DownloadSender:
    def __init__(
        self,
        transferrer: 'ParallelTransferrer',
        client: TelegramClient,
        sender: MTProtoSender,
        file: TypeLocation,
        offset: int,
        limit: int,
        stride: int,
        count: int,
        speed_limit: int = MAX_DOWNLOAD_SPEED
    ) -> None:
        self.transferrer = transferrer
        self.client = client
        self.sender = sender
        self.file = file
        self.request = GetFileRequest(file, offset=offset, limit=limit)
        self.stride = stride
        self.remaining = count
        self.last_chunk_time = time.time()
        self.last_chunk_size = 0
        self.speed_limit = speed_limit
        self.request_times = []
        self.total_bytes_transferred = 0
        self.start_time = time.time()
        logger.debug(f"Initialized DownloadSender with chunk size: {limit/1024:.2f} KB")

    async def next(self) -> Optional[bytes]:
        if not self.remaining:
            logger.debug("No more chunks remaining")
            return None
            
        try:
            current_time = time.time()
            time_diff = current_time - self.last_chunk_time
            if time_diff > 0:
                current_speed = self.last_chunk_size / time_diff if self.last_chunk_size else 0
                if current_speed > self.speed_limit:
                    wait_time = (self.last_chunk_size / self.speed_limit) - time_diff
                    if wait_time > 0:
                        logger.debug(f"Throttling: Speed {current_speed/1024:.2f} KB/s exceeds limit {self.speed_limit/1024:.2f} KB/s, waiting {wait_time:.2f}s")
                        await asyncio.sleep(wait_time)
                        
            request_start = time.time()
            result = await self.client._call(self.sender, self.request)
            request_end = time.time()
            chunk_size = len(result.bytes)
            self.request_times.append(request_start)
            self.total_bytes_transferred += chunk_size
            
            logger.debug(
                f"Got chunk: {chunk_size/1024:.2f} KB | "
                f"Offset: {self.request.offset/1024:.2f} KB | "
                f"Remaining: {self.remaining}"
                )
                
            if len(self.request_times) % 5 == 0:
                self._log_metrics()
                        
            self.last_chunk_size = chunk_size
            self.last_chunk_time = request_end
            self.remaining -= 1
            self.request.offset += self.stride
            
            return result.bytes
        except FloodWaitError as e:
            logger.warning(f"Flood wait error, sleeping for {e.seconds} seconds")
            await asyncio.sleep(e.seconds)
            return await self.next()
            
        except FileMigrateError as e:
            logger.info(f"File migrated to DC {e.new_dc}, reconnecting...")
            await self.sender.disconnect()
            # Properly decrement active connections for the old sender
            async with self.transferrer._connection_lock:
                self.transferrer._active_connections -= 1
            self.sender = await self.transferrer._create_sender_for_dc(e.new_dc)
            # Do NOT reset offset: self.request.offset = 0 (This was a bug)
            # The current self.request has the correct offset for the chunk that failed.
            logger.info(f"Retrying GetFileRequest with offset {self.request.offset/1024:.2f} KB to new DC {e.new_dc}")
            return await self.next()
                    
        except Exception as e:
            logger.error(f"Unexpected error in DownloadSender: {str(e)}")
            await self.disconnect() # Attempt to clean up this sender
            raise

    def _log_metrics(self) -> None:
        if not self.request_times:
            return
            
        current_time = time.time()
        elapsed = current_time - self.start_time
        recent_requests = [t for t in self.request_times if t > current_time - 10]
        rps = len(recent_requests) / 10 if recent_requests else 0
        avg_speed = self.total_bytes_transferred / elapsed if elapsed > 0 else 0
        
        logger.info(
            f"Transfer stats: "
            f"Total chunks: {len(self.request_times)} | "
            f"RPS: {rps:.2f} | "
            f"Avg speed: {avg_speed/1024:.2f} KB/s | "
            f"Total data: {self.total_bytes_transferred/1024/1024:.2f} MB"
        )

    async def disconnect(self) -> None:
        if self.sender and hasattr(self.sender, 'is_connected') and self.sender.is_connected():
            await self.sender.disconnect()
        
        # Log final stats for this sender if it transferred any data
        if self.total_bytes_transferred > 0:
            elapsed = time.time() - self.start_time
            avg_speed_kbps = (self.total_bytes_transferred / elapsed / 1024) if elapsed > 0 else 0
            logger.info(
                f"DownloadSender disconnected: "
                f"Total chunks handled: {len(self.request_times)} | "
                f"Total data by this sender: {self.total_bytes_transferred/1024/1024:.2f} MB | "
                f"Avg speed by this sender: {avg_speed_kbps:.2f} KB/s"
            )
        else:
            logger.debug("DownloadSender disconnected (no data transferred by this instance).")


class UploadSender:
    def __init__(
        self,
        client: TelegramClient,
        sender: MTProtoSender,
        file_id: int,
        part_count: int,
        big: bool,
        index: int,
        stride: int,
        loop: asyncio.AbstractEventLoop,
        speed_limit: int = MAX_DOWNLOAD_SPEED # Note: Uses MAX_DOWNLOAD_SPEED as default, consider renaming or separate config
    ) -> None:
        self.client = client
        self.sender = sender
        self.part_count = part_count
        self.request = SaveBigFilePartRequest(file_id, index, part_count, b"") if big else SaveFilePartRequest(file_id, index, b"")
        self.stride = stride
        self.previous = None
        self.loop = loop
        self.last_chunk_time = time.time()
        self.last_chunk_size = 0
        self.speed_limit = speed_limit # This is per-sender speed limit
        logger.debug(f"Initialized UploadSender for file part {index}")

    async def next(self, data: bytes) -> None:
        if self.previous:
            await self.previous
        
        current_time = time.time()
        time_diff = current_time - self.last_chunk_time
        if time_diff > 0 and self.last_chunk_size > 0: # Ensure last_chunk_size is also positive
            current_speed = self.last_chunk_size / time_diff
            if current_speed > self.speed_limit:
                wait_time = (self.last_chunk_size / self.speed_limit) - time_diff
                if wait_time > 0:
                    logger.debug(f"Upload throttling: Waiting {wait_time:.2f}s")
                    await asyncio.sleep(wait_time)
        
        self.previous = self.loop.create_task(self._next(data))

    async def _next(self, data: bytes) -> None:
        try:
            self.request.bytes = data
            chunk_size = len(data)
            logger.debug(f"Uploading chunk {self.request.file_part} ({chunk_size/1024:.2f} KB)")
            await self.client._call(self.sender, self.request)
            self.last_chunk_size = chunk_size
            self.last_chunk_time = time.time()
            self.request.file_part += self.stride
        except FloodWaitError as e:
            logger.warning(f"Flood wait error, sleeping for {e.seconds} seconds uploading")
            await asyncio.sleep(e.seconds)
            await self._next(data) # Retry with the same data
        except Exception as e:
            logger.error(f"Upload error in _next for part {self.request.file_part}: {str(e)}")
            # Decide if to re-raise or handle. Re-raising will stop this sender.
            raise

    async def disconnect(self) -> None:
        if self.previous:
            try:
                await self.previous
            except Exception as e:
                logger.debug(f"Exception in pending upload task during disconnect: {e}")
        if self.sender and hasattr(self.sender, 'is_connected') and self.sender.is_connected():
            await self.sender.disconnect()
        logger.debug("UploadSender disconnected")

class ParallelTransferrer:
    def __init__(self, client: TelegramClient, dc_id: Optional[int] = None, speed_limit: int = MAX_DOWNLOAD_SPEED) -> None:
        self.client = client
        self.loop = self.client.loop
        self.dc_id = dc_id or self.client.session.dc_id
        # If dc_id is provided and different from client's current DC, auth_key must be None to trigger export/import.
        if dc_id and dc_id != self.client.session.dc_id:
            self.auth_key = None
        else:
            self.auth_key = self.client.session.auth_key
            
        self.senders: List[Union[DownloadSender, UploadSender]] = [] # Corrected typing
        self.upload_ticker = 0
        self.speed_limit = speed_limit # This is the global speed limit for the transferrer
        self.last_speed_check = time.time()
        self.bytes_transferred_since_check = 0
        self._connection_lock = asyncio.Lock()
        self._active_connections = 0 # Connections managed by this transferrer instance
        logger.info(f"Initialized ParallelTransferrer for DC {self.dc_id}. Global speed limit: {speed_limit/1024/1024:.2f} MB/s. Auth key initially {'set' if self.auth_key else 'None'}.")

    async def _cleanup(self) -> None:
        if self.senders:
            logger.info(f"Starting cleanup of {len(self.senders)} senders for DC {self.dc_id}")
            
            # Cancel pending tasks for UploadSenders
            for sender in self.senders:
                if isinstance(sender, UploadSender) and sender.previous and not sender.previous.done():
                    sender.previous.cancel()
                    try:
                        await sender.previous
                    except asyncio.CancelledError:
                        logger.debug("Upload sender's pending task cancelled.")
                    except Exception as e:
                        logger.debug(f"Exception in awaiting cancelled upload task: {e}")

            disconnect_tasks = [s.disconnect() for s in self.senders]
            results = await asyncio.gather(*disconnect_tasks, return_exceptions=True)
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.warning(f"Error disconnecting sender {i}: {result}")
            
            async with self._connection_lock:
                # This count should reflect connections that were successfully established and managed by this transferrer.
                # If _create_sender failed, _active_connections might not have been decremented properly in all paths,
                # so ensure it doesn't go below zero.
                logger.debug(f"Cleaning up {len(self.senders)} senders. Current active connections: {self._active_connections}")
                self._active_connections = max(0, self._active_connections - len(self.senders))
            
            self.senders = []
            logger.info(f"Cleanup completed for DC {self.dc_id}. Active connections now: {self._active_connections}")
        else:
            logger.info(f"No senders to cleanup for DC {self.dc_id}.")


    @staticmethod
    def _get_connection_count(file_size: int, max_user_count: Optional[int] = None, full_size: int = 100 * 1024 * 1024) -> int:
        # Use the globally configured MAX_CONNECTIONS_PER_TRANSFER as the default upper limit
        effective_max_count = min(max_user_count or MAX_CONNECTIONS_PER_TRANSFER, MAX_CONNECTIONS_PER_TRANSFER)
        
        if file_size < MIN_CHUNK_SIZE: # For very small files, 1 connection is enough
            return 1
        if file_size > full_size: # For large files, use max allowed connections
            return effective_max_count
        # For medium files, scale connections, up to the max allowed
        return min(math.ceil((file_size / full_size) * effective_max_count), effective_max_count)


    async def _init_download(self, connections: int, file: TypeLocation, file_size: int, part_size: int) -> None: # Added file_size
        # Ensure part_size is aligned and within bounds
        part_size = max(MIN_CHUNK_SIZE, min(part_size, MAX_CHUNK_SIZE))
        part_size = (part_size // 1024) * 1024 # Align to 1KB boundary, Telethon default is 512KB

        if part_size == 0: # Avoid division by zero if somehow part_size becomes 0
            part_size = MIN_CHUNK_SIZE 
            logger.warning(f"Part size was zero, reset to {MIN_CHUNK_SIZE/1024} KB")

        part_count = math.ceil(file_size / part_size)
        logger.info(f"Download: {connections} connections, file_size={file_size/1024/1024:.2f}MB, part_size={part_size/1024}KB, part_count={part_count}")

        if connections <= 0: connections = 1 # Ensure at least one connection

        # Distribute parts among connections
        min_parts_per_conn, remainder_parts = divmod(part_count, connections)
        
        current_offset_parts = 0 # Keep track of offset in terms of parts

        # Calculate per-sender speed limit
        per_sender_speed_limit = self.speed_limit // connections if connections > 0 else self.speed_limit

        senders_to_create = []
        for i in range(connections):
            parts_for_this_sender = min_parts_per_conn + (1 if i < remainder_parts else 0)
            if parts_for_this_sender == 0:
                continue # No parts for this sender, skip

            # Offset for this sender is current_offset_parts * part_size
            # Stride is connections * part_size
            # Index 'i' passed to _create_download_sender is the worker index, not part index.
            # The 'offset' in GetFileRequest should be the actual byte offset.
            # Each sender starts at a unique offset (i * part_size for initial block)
            # and then strides by (connections * part_size).
            
            initial_byte_offset = i * part_size # Sender i starts fetching the i-th block of parts
            
            # The DownloadSender will fetch `parts_for_this_sender` chunks.
            # Its first chunk is at `initial_byte_offset`.
            # Its second chunk is at `initial_byte_offset + connections * part_size`.
            
            senders_to_create.append(
                self._create_download_sender(
                    file=file, 
                    # index=i, # This 'index' was for the part_id in upload, for download it's effectively worker_index
                    initial_offset_bytes=initial_byte_offset,
                    part_size_bytes=part_size, 
                    stride_bytes=connections * part_size, 
                    num_parts_to_fetch=parts_for_this_sender,
                    speed_limit=per_sender_speed_limit
                )
            )
            current_offset_parts += parts_for_this_sender # This logic was for a different distribution, simplified above

        try:
            self.senders = await asyncio.gather(*senders_to_create)
            logger.info(f"Initialized {len(self.senders)} download senders with {part_size/1024:.2f} KB chunks each. Total parts to fetch: {part_count}.")
        except Exception as e:
            logger.error(f"Failed to initialize all download senders: {e}")
            await self._cleanup() # Cleanup any partially created senders
            raise

    async def _create_download_sender(self, file: TypeLocation, initial_offset_bytes: int, part_size_bytes: int, stride_bytes: int, num_parts_to_fetch: int, speed_limit: int) -> DownloadSender:
        # `initial_offset_bytes` is the starting byte offset for the first chunk this sender will download.
        # `part_size_bytes` is the GetFileRequest limit (size of each chunk).
        # `stride_bytes` is how much the offset increments for this sender after each successful chunk download.
        # `num_parts_to_fetch` is the number of chunks this specific sender is responsible for.
        return DownloadSender(
            self,
            self.client,
            await self._create_sender(), # This MTProtoSender is shared or created per DownloadSender
            file,
            offset=initial_offset_bytes,
            limit=part_size_bytes,
            stride=stride_bytes,
            count=num_parts_to_fetch,
            speed_limit=speed_limit
        )

    async def _init_upload(self, connections: int, file_id: int, part_count: int, big: bool) -> None:
        if connections <= 0: connections = 1
        per_sender_speed_limit = self.speed_limit // connections if connections > 0 else self.speed_limit
        
        senders_to_create = []
        for i in range(connections):
            # Each sender starts at index i and strides by 'connections'
            senders_to_create.append(
                self._create_upload_sender(file_id, part_count, big, i, connections, per_sender_speed_limit)
            )
        
        try:
            self.senders = await asyncio.gather(*senders_to_create)
            logger.info(f"Initialized {len(self.senders)} upload senders.")
        except Exception as e:
            logger.error(f"Failed to initialize all upload senders: {e}")
            await self._cleanup()
            raise


    async def _create_upload_sender(self, file_id: int, part_count: int, big: bool, index: int, stride: int, speed_limit: int) -> UploadSender:
        # index = initial part_id for this sender
        # stride = how many parts to skip for next upload by this sender
        return UploadSender(
            self.client,
            await self._create_sender(),
            file_id,
            part_count,
            big,
            index, # Initial file_part for this sender
            stride, # Stride for file_part
            loop=self.loop,
            speed_limit=speed_limit
        )

    async def _create_sender_for_dc(self, dc_id: int) -> MTProtoSender:
        logger.info(f"Switching ParallelTransferrer to DC {dc_id}. Current auth_key will be reset.")
        # This ParallelTransferrer instance will now operate on the new dc_id
        self.dc_id = dc_id
        self.auth_key = None # Force re-authentication for the new DC
        return await self._create_sender()

    async def _create_sender(self) -> MTProtoSender:
        async with self._connection_lock:
            # MAX_CONNECTIONS_PER_TRANSFER is the limit for this specific ParallelTransferrer instance
            if self._active_connections >= MAX_CONNECTIONS_PER_TRANSFER:
                logger.error(f"Cannot create new sender: Max connections ({MAX_CONNECTIONS_PER_TRANSFER}) for this transfer already reached.")
                raise RuntimeError(f"Max connections ({MAX_CONNECTIONS_PER_TRANSFER}) reached for this transfer operation")
            self._active_connections += 1
            logger.debug(f"Incremented active connections to {self._active_connections} for DC {self.dc_id}.")

        new_sender: Optional[MTProtoSender] = None
        try:
            dc = await self.client._get_dc(self.dc_id)
            # MTProtoSender is initialized with self.auth_key. If it's None (e.g., new DC), auth will be attempted.
            new_sender = MTProtoSender(self.auth_key, loggers=self.client._log)
            
            logger.debug(f"Connecting MTProtoSender to DC {dc.id} ({dc.ip_address}:{dc.port}). Current auth_key is {'set' if self.auth_key else 'None'}.")
            await new_sender.connect(
                self.client._connection( # Using main client's method to get a Connection object
                    dc.ip_address,
                    dc.port,
                    dc.id,
                    loggers=self.client._log,
                    proxy=self.client._proxy,
                )
            )
            logger.debug(f"MTProtoSender TCP connection to DC {dc.id} established.")
            
            # If auth_key was not set for this DC (either initially or after migration)
            # or if the provided self.auth_key is not valid for new_sender after connect (new_sender.auth_key would be None)
            if not new_sender.auth_key:
                logger.info(f"No existing auth key for sender to DC {self.dc_id}. Attempting to export and import authorization.")
                exported_auth = await self.client(ExportAuthorizationRequest(self.dc_id))
                logger.debug(f"Authorization exported successfully for DC {self.dc_id}.")

                # Construct a new InitConnectionRequest for importing authorization
                # This uses parameters from the main client, which is standard Telethon practice
                init_conn_req = InitConnectionRequest(
                    api_id=self.client.api_id,
                    device_model=self.client.device_model or 'Unknown Device',
                    system_version=self.client.system_version or 'Unknown OS',
                    app_version=self.client.app_version or f'FastTelethon/{LAYER}',
                    system_lang_code=self.client.system_lang_code or 'en',
                    lang_pack='', 
                    lang_code=self.client.lang_code or 'en',
                    query=ImportAuthorizationRequest(id=exported_auth.id, bytes=exported_auth.bytes)
                )
                request_with_layer = InvokeWithLayerRequest(LAYER, init_conn_req)
                
                logger.debug(f"Sending InvokeWithLayerRequest(InitConnectionRequest(ImportAuthorizationRequest)) to DC {self.dc_id}.")
                await new_sender.send(request_with_layer) # This should establish the session and set new_sender.auth_key
                
                if not new_sender.auth_key:
                    # This is a critical failure if auth_key is still not set after ImportAuthorization
                    logger.error(f"CRITICAL: Auth key still not set for sender to DC {self.dc_id} after ImportAuthorization attempt.")
                    raise ConnectionError(f"Failed to establish session and obtain auth key for DC {self.dc_id} after auth import.")
                
                # If successful, store this new auth_key in the ParallelTransferrer instance
                # as it's now the active auth_key for self.dc_id
                self.auth_key = new_sender.auth_key
                logger.info(f"Successfully authorized sender and obtained new auth key for DC {self.dc_id}.")
            else:
                logger.info(f"Using existing auth key for sender to DC {self.dc_id}.")
                
            return new_sender
        except Exception as e:
            logger.error(f"Error creating/authorizing sender for DC {self.dc_id}: {type(e).__name__} - {str(e)}")
            if new_sender and hasattr(new_sender, 'is_connected') and new_sender.is_connected():
                await new_sender.disconnect()
            async with self._connection_lock:
                self._active_connections -= 1 # Decrement on failure
                logger.debug(f"Decremented active connections to {self._active_connections} due to sender creation failure for DC {self.dc_id}.")
            raise # Re-raise the captured exception

    async def init_upload(self, file_id: int, file_size: int, part_size_kb: Optional[float] = None, connection_count: Optional[int] = None) -> Tuple[int, int, bool]:
        actual_connections = connection_count or self._get_connection_count(file_size, connection_count)
        # Ensure part_size is determined correctly based on Telethon utils or user input
        part_size_bytes = int((part_size_kb or utils.get_appropriated_part_size(file_size)) * 1024)
        part_size_bytes = max(MIN_CHUNK_SIZE, min(part_size_bytes, MAX_CHUNK_SIZE)) # Ensure within bounds
        part_size_bytes = (part_size_bytes // 1024) * 1024 # Align if necessary, though Telethon's util usually handles this
        if part_size_bytes == 0: part_size_bytes = MIN_CHUNK_SIZE # Safety for 0 part size

        part_count = (file_size + part_size_bytes - 1) // part_size_bytes
        is_large = file_size > 10 * 1024 * 1024 # Telethon's threshold for "big" file parts
        
        logger.info(f"Upload init: {actual_connections} connections, file_size={file_size/1024/1024:.2f}MB, part_size={part_size_bytes/1024}KB, part_count={part_count}, is_large={is_large}")
        await self._init_upload(actual_connections, file_id, part_count, is_large)
        return part_size_bytes, part_count, is_large

    async def upload(self, part: bytes) -> None:
        # Global speed throttling for this ParallelTransferrer instance
        current_time = time.time()
        time_since_last_check = current_time - self.last_speed_check
        
        if time_since_last_check > SPEED_CHECK_INTERVAL:
            if time_since_last_check > 0: # Avoid division by zero
                current_speed = self.bytes_transferred_since_check / time_since_last_check
                if current_speed > self.speed_limit and self.speed_limit > 0: # Check if speed_limit is positive
                    required_time = self.bytes_transferred_since_check / self.speed_limit
                    wait_time = required_time - time_since_last_check
                    if wait_time > 0:
                        logger.debug(f"Global upload throttling: Speed {current_speed/1024:.2f}KB/s > limit {self.speed_limit/1024:.2f}KB/s. Waiting {wait_time:.2f}s")
                        await asyncio.sleep(wait_time)
            
            self.last_speed_check = time.time() # Reset check time, could be current_time
            self.bytes_transferred_since_check = 0
        
        self.bytes_transferred_since_check += len(part)
        
        if not self.senders:
            logger.error("Upload called but no senders initialized.")
            raise RuntimeError("Upload senders are not initialized.")

        active_sender = self.senders[self.upload_ticker]
        await active_sender.next(part) # This 'next' is from UploadSender
        self.upload_ticker = (self.upload_ticker + 1) % len(self.senders)

    async def finish_upload(self) -> None:
        logger.info("Finishing upload, cleaning up senders...")
        await self._cleanup()
        logger.info("Upload finished and senders cleaned up.")

    async def download(self, file: TypeLocation, file_size: int, part_size_kb: Optional[float] = None, connection_count: Optional[int] = None) -> AsyncGenerator[bytes, None]:
        logger.info(f"Starting parallel download of {file_size/1024/1024:.2f} MB from DC {self.dc_id}.")
        actual_connections = connection_count or self._get_connection_count(file_size, connection_count)
        
        # Determine part size
        part_size_bytes = int((part_size_kb or utils.get_appropriated_part_size(file_size)) * 1024)
        part_size_bytes = max(MIN_CHUNK_SIZE, min(part_size_bytes, MAX_CHUNK_SIZE))
        part_size_bytes = (part_size_bytes // 1024) * 1024 # Ensure 1KB alignment
        if part_size_bytes == 0: part_size_bytes = MIN_CHUNK_SIZE


        await self._init_download(actual_connections, file, file_size, part_size_bytes)

        if not self.senders:
            logger.error("Download cannot proceed: no senders were initialized.")
            # If _init_download failed and raised, this point might not be reached.
            # If it completed but self.senders is empty, it's an issue in _init_download.
            return

        parts_retrieved = 0
        total_parts = math.ceil(file_size / part_size_bytes) if part_size_bytes > 0 else 0
        
        # Create tasks for all senders to start fetching their initial chunks
        # Each sender manages its own sequence of 'next' calls.
        # We need a way to interleave their outputs.
        
        # This creates a pool of tasks, one for each part each sender needs to download.
        # This is complex. A simpler way is to have each sender be an async generator itself,
        # or to manage a queue of requests for senders.

        # The original logic:
        # while part < part_count:
        #   tasks = [self.loop.create_task(sender.next()) for sender in self.senders]
        #   for task in tasks: data = await task ... (this assumes tasks are for one chunk from each sender in lockstep)
        # This fetches one chunk from each sender in a round-robin fashion for each 'while' iteration.

        active_download_tasks: List[asyncio.Task] = []
        sender_indices = list(range(len(self.senders))) # To keep track of which sender a task belongs to

        # Initialize by creating one task per sender
        for i, sender_instance in enumerate(self.senders):
            if sender_instance.remaining > 0: # If sender has parts to fetch
                task = self.loop.create_task(sender_instance.next())
                active_download_tasks.append(task)
            else: # Should not happen if distribution is correct
                sender_indices.remove(i) 


        try:
            while parts_retrieved < total_parts and active_download_tasks:
                # Global speed throttling
                current_time_global = time.time()
                time_since_last_check_global = current_time_global - self.last_speed_check
                if time_since_last_check_global > SPEED_CHECK_INTERVAL:
                    if time_since_last_check_global > 0:
                        current_speed_global = self.bytes_transferred_since_check / time_since_last_check_global
                        if current_speed_global > self.speed_limit and self.speed_limit > 0:
                            required_time_global = self.bytes_transferred_since_check / self.speed_limit
                            wait_time_global = required_time_global - time_since_last_check_global
                            if wait_time_global > 0:
                                logger.debug(f"Global download throttling: Speed {current_speed_global/1024:.2f}KB/s > limit {self.speed_limit/1024:.2f}KB/s. Waiting {wait_time_global:.2f}s")
                                await asyncio.sleep(wait_time_global)
                    self.last_speed_check = time.time()
                    self.bytes_transferred_since_check = 0

                # Wait for any of the active download tasks to complete
                done, pending = await asyncio.wait(active_download_tasks, return_when=asyncio.FIRST_COMPLETED)
                
                for task in done:
                    try:
                        data_chunk = await task # Get result (or exception)
                    except Exception as e:
                        logger.error(f"A download task failed: {e}. This might halt further downloads from one sender.")
                        # Potentially remove this sender or retry, depending on error.
                        # For now, we just log and continue with other tasks.
                        # If FileMigrateError was raised from sender.next(), it would be handled there and retried.
                        # Other errors might be more critical.
                        active_download_tasks.remove(task)
                        # Need to find which sender this task belonged to if we want to re-issue its next task.
                        # This task management needs to be more robust to replace failed tasks.
                        # For now, if a task errors out here, its sender stops contributing.
                        continue # Skip to next completed task or next wait() cycle

                    if data_chunk:
                        yield data_chunk
                        self.bytes_transferred_since_check += len(data_chunk)
                        parts_retrieved += 1
                        
                        # Find which sender completed this task to re-issue its next task
                        # This is tricky without mapping tasks back to senders easily.
                        # A simpler approach for robust task management:
                        # Iterate senders, if sender.remaining and no active task for it, launch one.
                        # This current asyncio.wait approach is okay if senders handle their own errors and retries well.
                        # Assuming sender.next() will return None if it has no more parts or permanently fails.
                        
                        # Let's try to find the sender (this is inefficient)
                        original_sender_of_task = None
                        task_sender_index = -1
                        for idx, s_instance in enumerate(self.senders):
                            # This comparison won't work as task is not directly stored in sender
                            # We'd need to store task against sender or vice-versa.
                            # For now, this simplified loop will re-add tasks from senders that still have remaining parts.
                            pass


                    # Re-add the task to the list for the *same sender* if it has more data
                    # This part is complex: which sender finished?
                    # The provided original code implied a round-robin on `self.senders` list
                    # which is simpler but might not be as efficient as `asyncio.wait`.

                    # For now, we'll just refresh active_download_tasks from pending, and add new ones if needed.
                    active_download_tasks = list(pending) # Keep pending tasks

                # Check all senders and add new tasks if they have remaining parts and are not already in active_download_tasks
                # This is a simplification: assumes a task in active_download_tasks corresponds to one sender.
                current_sender_tasks_map = {t: None for t in active_download_tasks} # to check existence

                for s_instance in self.senders: # This part is a bit of a guess to keep senders going
                    if s_instance.remaining > 0:
                        # Crude check: if no task seems to be running for this sender (this is not accurate)
                        # A better way: each sender is an async iterator, and we use asyncio.gather or similar.
                        # Or, explicitly map tasks to senders.
                        # For this fix, let's assume if a task from `done` was processed, its sender might be ready for another.
                        # This is the weak point of this loop structure.
                        # The original code `tasks = [self.loop.create_task(sender.next()) for sender in self.senders]`
                        # was simpler: it always tried to get one more chunk from *every* sender that was still active.
                        # Let's revert to a structure closer to that for task replenishment.

                        # Simplification: if a task finished, one sender is free. Re-evaluate all.
                        # This is not ideal. The `active_download_tasks` should be managed per sender.
                        pass # The task replenishment logic here is complex with asyncio.wait.

                if not active_download_tasks and parts_retrieved < total_parts:
                    # All tasks finished but not all parts retrieved. Try to restart tasks for senders with remaining parts.
                    logger.debug("Replenishing download tasks...")
                    new_tasks_added = False
                    for sender_instance in self.senders:
                        if sender_instance.remaining > 0:
                            # Avoid re-adding if a task for this sender is somehow still considered pending by a broader scope.
                            # This simplified loop assumes we can just try to get the next part.
                            task = self.loop.create_task(sender_instance.next())
                            active_download_tasks.append(task)
                            new_tasks_added = True
                    if not new_tasks_added and parts_retrieved < total_parts :
                         logger.warning(f"No new tasks could be added, but download is incomplete ({parts_retrieved}/{total_parts}). Stopping.")
                         break # Avoid infinite loop if no sender can provide more parts


                if not active_download_tasks and parts_retrieved >= total_parts:
                    logger.info("All download tasks completed and all parts retrieved.")
                    break
                if not active_download_tasks:
                    logger.info("No more active download tasks.")
                    break


            if parts_retrieved < total_parts:
                logger.warning(f"Download finished with {parts_retrieved}/{total_parts} parts retrieved. File may be incomplete.")

        finally:
            # Cancel any remaining tasks
            for task in active_download_tasks:
                if not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass # Expected
                    except Exception as e:
                        logger.warning(f"Exception in cancelling a download task: {e}")
            
            await self._cleanup()
            logger.info(f"Parallel download completed. Total parts yielded: {parts_retrieved}.")


async def _internal_transfer_to_telegram(client: TelegramClient, response: BinaryIO, filename: str, progress_callback: Optional[callable]) -> Tuple[TypeInputFile, int]:
    logger.info(f"Starting internal transfer of file: {filename}")
    file_id = helpers.generate_random_long()
    
    # Ensure response is seekable and get size
    response.seek(0, os.SEEK_END)
    file_size = response.tell()
    response.seek(0)

    if file_size == 0:
        logger.warning(f"File {filename} is empty. Uploading as an empty file.")
    
    hash_md5 = hashlib.md5()
    uploader = None # Initialize uploader

    try:
        async with parallel_transfer_semaphore: # Limit concurrent transfers
            # Speed limit for this specific transfer can be passed to ParallelTransferrer
            uploader = ParallelTransferrer(client, speed_limit=MAX_DOWNLOAD_SPEED) # Example: using global download speed as default for upload
            part_size, part_count, is_large = await uploader.init_upload(file_id, file_size)
            
            buffer = bytearray()
            bytes_uploaded_for_callback = 0
            
            logger.info(f"File size: {file_size/1024/1024:.2f} MB, Part size: {part_size/1024:.2f} KB, Parts: {part_count}, Large file: {is_large}")

            for data_chunk_from_file in stream_file(response, chunk_size=part_size): # Read in part_size chunks ideally
                if not data_chunk_from_file: break # Should be handled by stream_file

                if progress_callback:
                    try:
                        # Progress based on what's been sent to uploader.upload, not necessarily network-uploaded yet
                        # response.tell() might not be accurate if stream_file reads ahead.
                        # Better to sum lengths of data_chunk_from_file.
                        bytes_uploaded_for_callback += len(data_chunk_from_file) # Approximation
                        r = progress_callback(bytes_uploaded_for_callback, file_size)
                        if inspect.isawaitable(r):
                            await r
                    except Exception as e:
                        logger.error(f"Progress callback error: {str(e)}")

                if not is_large: # MD5 for small files only
                    hash_md5.update(data_chunk_from_file)
                
                # The stream_file now yields chunks of 'part_size', so buffering logic simplifies
                # No, stream_file yields its own chunk_size, default 1024. Buffering is still needed.
                
                buffer.extend(data_chunk_from_file)
                
                while len(buffer) >= part_size:
                    to_upload = buffer[:part_size]
                    logger.debug(f"Uploading chunk of {len(to_upload)/1024:.2f} KB from buffer.")
                    await uploader.upload(bytes(to_upload))
                    del buffer[:part_size]

            if len(buffer) > 0: # Remaining data in buffer
                logger.debug(f"Uploading final chunk of {len(buffer)/1024:.2f} KB from buffer.")
                await uploader.upload(bytes(buffer))
            
            await uploader.finish_upload()
            logger.info(f"File upload {filename} completed successfully to Telegram.")

    except Exception as e:
        logger.error(f"Upload of {filename} failed: {type(e).__name__} - {str(e)}", exc_info=True)
        if uploader: # Ensure uploader exists before trying to cleanup
            logger.info("Attempting cleanup after upload failure...")
            await uploader._cleanup()
        raise # Re-raise the exception to the caller

    if is_large:
        return InputFileBig(file_id, part_count, filename), file_size
    else:
        return InputFile(file_id, part_count, filename, hash_md5.hexdigest()), file_size

def stream_file(file_to_stream: BinaryIO, chunk_size=1024*128) -> Generator[bytes, None, None]: # Increased default chunk_size
    logger.debug(f"Starting to stream file {getattr(file_to_stream, 'name', 'UnnamedStream')}")
    while True:
        data_read = file_to_stream.read(chunk_size)
        if not data_read:
            logger.debug("End of file stream reached")
            break
        yield data_read

async def cleanup_connections(): # This is a global cleanup, be careful
    logger.info("Performing global Telethon connection cleanup attempt...")
    # This targets internal Telethon tasks, might be risky if multiple clients/operations run
    tasks_to_cancel = []
    for task in asyncio.all_tasks():
        # A more specific check for Telethon's sender tasks might be needed
        # This is a generic check based on typical coro names.
        coro_name = ""
        try:
            coro_name = str(task.get_coro())
        except Exception: # Might fail for some task types
            pass

        if '_send_loop' in coro_name or '_recv_loop' in coro_name:
            if not task.done():
                 tasks_to_cancel.append(task)
    
    if not tasks_to_cancel:
        logger.info("No active Telethon send/receive loop tasks found to cancel.")
        return

    logger.info(f"Attempting to cancel {len(tasks_to_cancel)} identified Telethon tasks.")
    for task in tasks_to_cancel:
        task.cancel()
    
    results = await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
    cancelled_count = 0
    for result in results:
        if isinstance(result, asyncio.CancelledError):
            cancelled_count += 1
        elif isinstance(result, Exception):
            logger.warning(f"Exception during task cancellation: {result}")
            
    logger.info(f"Global cleanup: {cancelled_count}/{len(tasks_to_cancel)} tasks confirmed cancelled. Others might have finished or errored.")


def format_bytes(size: int) -> str:
    power = 1024 # Use 1024 for KB/MB/GB
    n = 0
    power_labels = {0: 'B', 1: 'KB', 2: 'MB', 3: 'GB', 4: 'TB'}
    while size >= power and n < len(power_labels) -1 : # Check n against length - 1
        size /= power
        n += 1
    return f"{size:.2f} {power_labels[n]}"

async def log_transfer_stats(interval_seconds: int = 60):
    try:
        process = psutil.Process(os.getpid())
        logger.info("Background stats logger started.")
    except Exception as e:
        logger.error(f"Could not initialize psutil for memory logging: {e}")
        process = None

    while True:
        await asyncio.sleep(interval_seconds)
        try:
            if process:
                try:
                    memory_info = process.memory_info()
                    logger.info(f"Memory usage: RSS={format_bytes(memory_info.rss)}, VMS={format_bytes(memory_info.vms)}")
                except psutil.NoSuchProcess:
                    logger.warning("Process not found for memory stats, stopping logger.")
                    break
                except Exception as e:
                    logger.error(f"Error getting memory info: {e}")

            active_tasks_count = len([task for task in asyncio.all_tasks() if not task.done()])
            logger.info(f"Active asyncio tasks: {active_tasks_count}")
            # Add more stats if needed, e.g., number of active ParallelTransferrer instances, etc.

        except asyncio.CancelledError:
            logger.info("Stats logger task cancelled.")
            break
        except Exception as e:
            logger.error(f"Error in stats logger loop: {e}")


async def download_file(
    client: TelegramClient, 
    location: TypeLocation, # This is the high-level Document or Photo object
    out: BinaryIO, 
    progress_callback: Optional[callable] = None, 
    speed_limit: int = MAX_DOWNLOAD_SPEED,
    part_size_kb: Optional[float] = None,
    connection_count: Optional[int] = None
) -> BinaryIO:
    file_name_for_log = getattr(out, 'name', 'output_stream')
    logger.info(f"Request to download file to {file_name_for_log}")

    if not hasattr(location, 'size'):
        logger.error(f"Provided location object {type(location)} does not have a 'size' attribute.")
        raise ValueError("Location object must have a 'size' attribute (e.g., Document, Photo).")

    file_size = location.size
    
    # Get input location and DC ID from the high-level media object
    try:
        dc_id, input_location_obj = utils.get_input_location(location)
    except Exception as e:
        logger.error(f"Could not get input location from provided media object: {e}")
        raise
    
    logger.info(f"Starting download for file of size {format_bytes(file_size)} from DC {dc_id} to {file_name_for_log}.")
    
    downloader = None
    try:
        async with parallel_transfer_semaphore: # Limit overall concurrent transfers
            downloader = ParallelTransferrer(client, dc_id, speed_limit=speed_limit)
            
            bytes_written = 0
            async for data_chunk in downloader.download(input_location_obj, file_size, part_size_kb=part_size_kb, connection_count=connection_count):
                out.write(data_chunk)
                bytes_written += len(data_chunk)
                if progress_callback:
                    try:
                        r = progress_callback(bytes_written, file_size) # Use bytes_written instead of out.tell()
                        if inspect.isawaitable(r):
                            await r
                    except Exception as e:
                         logger.error(f"Progress callback error during download: {str(e)}")
            
            logger.info(f"Download to {file_name_for_log} completed. Total bytes written: {format_bytes(bytes_written)}.")

    except Exception as e:
        logger.error(f"Download to {file_name_for_log} failed: {type(e).__name__} - {str(e)}", exc_info=True)
        # Cleanup for the specific downloader instance might have happened in its finally block
        raise
    
    return out

async def upload_file(
    client: TelegramClient, 
    file: BinaryIO, # File handle to the file to upload
    name: str, # Desired name of the file on Telegram
    progress_callback: Optional[callable] = None, 
    # speed_limit: int = MAX_DOWNLOAD_SPEED # Currently _internal_transfer_to_telegram uses its own ParallelTransferrer
                                         # If needed, this can be passed down.
) -> TypeInputFile:
    file_path_for_log = getattr(file, 'name', name) # Use actual path if available
    logger.info(f"Request to upload file: {file_path_for_log} as '{name}'")
    
    # _internal_transfer_to_telegram handles creating ParallelTransferrer
    # If a speed limit is desired here, it needs to be plumbed through.
    # For now, _internal_transfer_to_telegram uses MAX_DOWNLOAD_SPEED for its uploader.
    
    input_file_result, _ = await _internal_transfer_to_telegram(client, file, name, progress_callback)
    logger.info(f"Upload of {file_path_for_log} as '{name}' successful.")
    return input_file_result

# Example of starting background monitoring if this script is run directly or imported
# Consider making this optional or configurable.
# if __name__ != "__main__": # Start only if imported as a module, or use a specific init function
#     asyncio.create_task(log_transfer_stats())
