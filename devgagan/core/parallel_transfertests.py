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
from telethon.tl.functions import InvokeWithLayerRequest, InitConnectionRequest # CORRECTED IMPORT
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
        logger.debug(f"Initialized DownloadSender with chunk size: {limit/1024:.2f} KB for offset {offset/1024:.2f} KB, parts: {count}")

    async def next(self) -> Optional[bytes]:
        if not self.remaining:
            # logger.debug("No more chunks remaining for this sender.") # Can be too verbose
            return None
            
        try:
            current_time = time.time()
            time_diff = current_time - self.last_chunk_time
            if time_diff > 0 and self.last_chunk_size > 0 : # speed limit only if last_chunk_size is valid
                current_speed = self.last_chunk_size / time_diff
                if self.speed_limit > 0 and current_speed > self.speed_limit : # Check if speed_limit is positive
                    wait_time = (self.last_chunk_size / self.speed_limit) - time_diff
                    if wait_time > 0:
                        logger.debug(f"DS Throttling: Speed {current_speed/1024:.2f} KB/s exceeds limit {self.speed_limit/1024:.2f} KB/s, waiting {wait_time:.2f}s")
                        await asyncio.sleep(wait_time)
                        
            request_start = time.time()
            result = await self.client._call(self.sender, self.request)
            request_end = time.time()
            chunk_size = len(result.bytes)
            self.request_times.append(request_start)
            self.total_bytes_transferred += chunk_size
            
            # logger.debug( # Can be too verbose
            #     f"Got chunk: {chunk_size/1024:.2f} KB | "
            #     f"Offset: {self.request.offset/1024:.2f} KB | "
            #     f"Remaining for this sender: {self.remaining-1}" 
            # )
                
            if len(self.request_times) % 10 == 0: # Log metrics less frequently
                self._log_metrics()
                        
            self.last_chunk_size = chunk_size
            self.last_chunk_time = request_end # Should be request_end
            self.remaining -= 1
            self.request.offset += self.stride
            
            return result.bytes
        except FloodWaitError as e:
            logger.warning(f"DS Flood wait: sleeping for {e.seconds}s (Offset: {self.request.offset/1024:.2f} KB)")
            await asyncio.sleep(e.seconds)
            return await self.next() # Retry
            
        except FileMigrateError as e:
            logger.info(f"DS File migrated to DC {e.new_dc} (Offset: {self.request.offset/1024:.2f} KB). Reconnecting sender.")
            if self.sender and hasattr(self.sender, 'disconnect'):
                await self.sender.disconnect()
            
            async with self.transferrer._connection_lock:
                self.transferrer._active_connections -= 1
                logger.debug(f"DS Decremented active connections to {self.transferrer._active_connections} due to FileMigrate.")
            
            self.sender = await self.transferrer._create_sender_for_dc(e.new_dc)
            logger.info(f"DS Retrying GetFileRequest with offset {self.request.offset/1024:.2f} KB to new DC {e.new_dc}")
            return await self.next() # Retry with new sender, same request
                    
        except Exception as e:
            logger.error(f"DS Unexpected error (Offset: {self.request.offset/1024:.2f} KB): {type(e).__name__} - {str(e)}")
            if self.sender and hasattr(self.sender, 'disconnect'): # Ensure disconnect is callable
                 await self.disconnect() 
            raise

    def _log_metrics(self) -> None:
        if not self.request_times:
            return
            
        current_time = time.time()
        elapsed = current_time - self.start_time
        # Consider recent window for RPS, e.g., last 10-30 seconds
        check_window = 10 
        recent_requests_ts = [t for t in self.request_times if t > current_time - check_window]
        rps = len(recent_requests_ts) / check_window if recent_requests_ts else 0
        avg_speed = self.total_bytes_transferred / elapsed if elapsed > 0 else 0
        
        logger.debug( # Changed to debug as it can be verbose
            f"DS Stats: Total chunks: {len(self.request_times)} | "
            f"RPS (last {check_window}s): {rps:.2f} | "
            f"Avg speed: {avg_speed/1024:.2f} KB/s | "
            f"Total data this sender: {self.total_bytes_transferred/1024/1024:.2f} MB"
        )

    async def disconnect(self) -> None:
        # Check if sender exists and has disconnect method and is connected
        if self.sender and hasattr(self.sender, 'is_connected') and self.sender.is_connected() and hasattr(self.sender, 'disconnect'):
            await self.sender.disconnect()
        
        if self.total_bytes_transferred > 0:
            elapsed = time.time() - self.start_time
            avg_speed_kbps = (self.total_bytes_transferred / elapsed / 1024) if elapsed > 0 else 0
            logger.debug( # Changed to debug
                f"DS Disconnected: Chunks: {len(self.request_times)} | "
                f"Data: {self.total_bytes_transferred/1024/1024:.2f} MB | "
                f"Avg speed: {avg_speed_kbps:.2f} KB/s"
            )
        else:
            logger.debug("DS Disconnected (no data transferred by this instance).")


class UploadSender:
    def __init__(
        self,
        client: TelegramClient,
        sender: MTProtoSender,
        file_id: int,
        part_count: int,
        big: bool,
        index: int, # Initial part_id for this sender
        stride: int, # Stride for part_id
        loop: asyncio.AbstractEventLoop,
        speed_limit: int = MAX_DOWNLOAD_SPEED 
    ) -> None:
        self.client = client
        self.sender = sender
        self.part_count = part_count # Total parts for the file
        self.request = SaveBigFilePartRequest(file_id, index, part_count, b"") if big else SaveFilePartRequest(file_id, index, b"")
        self.stride = stride
        self.previous: Optional[asyncio.Task] = None # Task for the current/previous send operation
        self.loop = loop
        self.last_chunk_time = time.time()
        self.last_chunk_size = 0
        self.speed_limit = speed_limit 
        logger.debug(f"Initialized UploadSender for file_id {file_id}, initial part {index}, stride {stride}")

    async def next(self, data: bytes) -> None:
        if self.previous and not self.previous.done(): # Wait if a send is already in progress
            try:
                await self.previous
            except Exception as e:
                logger.warning(f"US Previous upload task ended with error: {e}. Continuing with new upload.")
        
        current_time = time.time()
        time_diff = current_time - self.last_chunk_time
        if time_diff > 0 and self.last_chunk_size > 0 and self.speed_limit > 0:
            current_speed = self.last_chunk_size / time_diff
            if current_speed > self.speed_limit:
                wait_time = (self.last_chunk_size / self.speed_limit) - time_diff
                if wait_time > 0:
                    logger.debug(f"US Throttling: Speed {current_speed/1024:.2f}KB/s > limit {self.speed_limit/1024:.2f}KB/s. Waiting {wait_time:.2f}s for part {self.request.file_part}")
                    await asyncio.sleep(wait_time)
        
        self.previous = self.loop.create_task(self._next(data))

    async def _next(self, data: bytes) -> None:
        try:
            self.request.bytes = data # Set data for the current part
            chunk_size = len(data)
            # logger.debug(f"US Uploading part {self.request.file_part}/{self.part_count} ({chunk_size/1024:.2f} KB)")
            await self.client._call(self.sender, self.request)
            self.last_chunk_size = chunk_size
            self.last_chunk_time = time.time()
            # Move to the next part this sender is responsible for
            self.request.file_part += self.stride 
        except FloodWaitError as e:
            logger.warning(f"US Flood wait: sleeping for {e.seconds}s (Part: {self.request.file_part})")
            await asyncio.sleep(e.seconds)
            await self._next(data) # Retry with the same data and original part number
        except Exception as e:
            logger.error(f"US Error uploading part {self.request.file_part}: {type(e).__name__} - {str(e)}")
            raise # Re-raise to allow ParallelTransferrer to handle

    async def disconnect(self) -> None:
        if self.previous and not self.previous.done():
            self.previous.cancel() # Cancel if task is still running
            try:
                await self.previous
            except asyncio.CancelledError:
                logger.debug(f"US Upload task for part {self.request.file_part if self.request else 'N/A'} cancelled during disconnect.")
            except Exception as e:
                logger.debug(f"US Exception in pending upload task during disconnect: {e}")
        
        if self.sender and hasattr(self.sender, 'is_connected') and self.sender.is_connected() and hasattr(self.sender, 'disconnect'):
            await self.sender.disconnect()
        logger.debug(f"US Disconnected (Last part: {self.request.file_part if self.request else 'N/A'})")


class ParallelTransferrer:
    def __init__(self, client: TelegramClient, dc_id: Optional[int] = None, speed_limit: int = MAX_DOWNLOAD_SPEED) -> None:
        self.client = client
        self.loop = self.client.loop # Ensure loop is from client
        self.target_dc_id = dc_id or self.client.session.dc_id # The DC this transferrer is intended for
        
        if self.target_dc_id != self.client.session.dc_id:
            self.current_auth_key = None # Must re-auth for a different DC
        else:
            self.current_auth_key = self.client.session.auth_key
            
        self.senders: List[Union[DownloadSender, UploadSender]] = []
        self.upload_ticker = 0
        self.global_speed_limit = speed_limit 
        self.last_global_speed_check = time.time()
        self.bytes_since_global_check = 0
        self._connection_management_lock = asyncio.Lock() # Renamed for clarity
        self._active_connections_count = 0
        logger.info(f"PT Initialized for DC {self.target_dc_id}. Global speed limit: {self.global_speed_limit/1024/1024:.2f} MB/s. Auth key initially {'set' if self.current_auth_key else 'None'}.")

    async def _cleanup(self) -> None:
        if not self.senders:
            logger.info(f"PT No senders to cleanup for DC {self.target_dc_id}.")
            return

        logger.info(f"PT Starting cleanup of {len(self.senders)} senders for DC {self.target_dc_id}")
        
        disconnect_tasks = [s.disconnect() for s in self.senders]
        results = await asyncio.gather(*disconnect_tasks, return_exceptions=True)
        
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.warning(f"PT Error disconnecting sender {i} (DC {self.target_dc_id}): {result}")
        
        async with self._connection_management_lock:
            # This count reflects connections managed by this transferrer.
            # Ensure it doesn't go below zero.
            initial_active = self._active_connections_count
            self._active_connections_count = max(0, self._active_connections_count - len(self.senders))
            logger.debug(f"PT Cleaned up {len(self.senders)} senders. Active connections changed from {initial_active} to {self._active_connections_count} for DC {self.target_dc_id}.")
        
        self.senders = []
        logger.info(f"PT Cleanup completed for DC {self.target_dc_id}. Active connections now: {self._active_connections_count}")


    @staticmethod
    def _get_connection_count(file_size: int, user_max_connections: Optional[int] = None, base_file_size_for_max_conn: int = 100 * 1024 * 1024) -> int:
        # Global max connections for a single transfer operation
        system_max_conn = MAX_CONNECTIONS_PER_TRANSFER 
        
        # Effective max is minimum of user preference (if any) and system limit
        if user_max_connections is not None:
            effective_max_conn = min(user_max_connections, system_max_conn)
        else:
            effective_max_conn = system_max_conn
        
        if effective_max_conn <= 0: effective_max_conn = 1 # Must be at least 1

        if file_size < MIN_CHUNK_SIZE * effective_max_conn : # If file is small enough that each conn gets < 1 chunk
             # Scale down connections if file is too small for many connections
            num_conn = math.ceil(file_size / MIN_CHUNK_SIZE) if MIN_CHUNK_SIZE > 0 else 1
            return min(num_conn, effective_max_conn) if num_conn > 0 else 1

        if file_size > base_file_size_for_max_conn: # For large files, use max allowed
            return effective_max_conn
        
        # For medium files, scale connections: (file_size / base_size) * max_conn
        # Ensure at least 1 connection
        calculated_conn = math.ceil((file_size / base_file_size_for_max_conn) * effective_max_conn)
        return min(max(1, calculated_conn), effective_max_conn)


    async def _init_download(self, connections_to_use: int, file_location_obj: TypeLocation, file_total_size: int, part_size_bytes: int) -> None:
        part_size_bytes = max(MIN_CHUNK_SIZE, min(part_size_bytes, MAX_CHUNK_SIZE))
        part_size_bytes = (part_size_bytes // 1024) * 1024 
        if part_size_bytes == 0: part_size_bytes = MIN_CHUNK_SIZE 

        if file_total_size == 0:
            logger.info("PT Download: File size is 0, no parts to download.")
            self.senders = []
            return

        total_parts_in_file = math.ceil(file_total_size / part_size_bytes)
        logger.info(f"PT Download init: Using {connections_to_use} connections for {file_total_size/1024/1024:.2f}MB file. Part size: {part_size_bytes/1024}KB. Total parts: {total_parts_in_file}.")

        if connections_to_use <= 0: connections_to_use = 1

        min_parts_per_sender, remainder_parts = divmod(total_parts_in_file, connections_to_use)
        
        per_sender_speed_limit = self.global_speed_limit // connections_to_use if connections_to_use > 0 and self.global_speed_limit > 0 else self.global_speed_limit

        creation_tasks = []
        current_part_offset_index = 0 # Index of the part, not byte offset

        for i in range(connections_to_use):
            num_parts_for_this_sender = min_parts_per_sender + (1 if i < remainder_parts else 0)
            if num_parts_for_this_sender == 0:
                continue 

            # Each sender starts at a specific part index and strides by total connections
            initial_byte_offset_for_sender = current_part_offset_index * part_size_bytes # This was initial way for blocks
            
            # Corrected: Sender 'i' handles parts i, i+N, i+2N, ... where N is connections_to_use
            # Its first request is for part 'i' (0-indexed)
            initial_part_index_for_sender = i 
            if initial_part_index_for_sender >= total_parts_in_file: # This sender would start beyond EOF
                continue

            initial_byte_offset_for_sender = initial_part_index_for_sender * part_size_bytes
            
            # Stride is now in terms of bytes for the GetFileRequest offset
            byte_stride_for_sender = connections_to_use * part_size_bytes
            
            creation_tasks.append(
                self._create_download_sender(
                    file=file_location_obj, 
                    initial_offset_bytes=initial_byte_offset_for_sender,
                    part_size_bytes=part_size_bytes, 
                    stride_bytes=byte_stride_for_sender, 
                    num_parts_to_fetch=num_parts_for_this_sender, # This count is how many items in its sequence it fetches
                    speed_limit=per_sender_speed_limit
                )
            )
            # current_part_offset_index += num_parts_for_this_sender # Not needed if using interleaved assignment

        if not creation_tasks:
            logger.warning(f"PT No download senders were scheduled for creation (e.g. file too small or 0 connections). File size {file_total_size}, parts {total_parts_in_file}, connections {connections_to_use}")
            self.senders = []
            return
            
        try:
            self.senders = await asyncio.gather(*creation_tasks)
            logger.info(f"PT Initialized {len(self.senders)} download senders. Each part: {part_size_bytes/1024:.2f} KB. Total file parts: {total_parts_in_file}.")
        except Exception as e:
            logger.error(f"PT Failed to initialize all download senders: {type(e).__name__} - {e}", exc_info=True)
            await self._cleanup() 
            raise

    async def _create_download_sender(self, file: TypeLocation, initial_offset_bytes: int, part_size_bytes: int, stride_bytes: int, num_parts_to_fetch: int, speed_limit: int) -> DownloadSender:
        mtp_sender = await self._create_sender() # Get a configured MTProtoSender
        return DownloadSender(
            self, self.client, mtp_sender, file,
            offset=initial_offset_bytes, limit=part_size_bytes,
            stride=stride_bytes, count=num_parts_to_fetch,
            speed_limit=speed_limit
        )

    async def _init_upload(self, connections_to_use: int, file_id: int, total_parts_in_file: int, is_large_file: bool) -> None:
        if connections_to_use <= 0: connections_to_use = 1
        per_sender_speed_limit = self.global_speed_limit // connections_to_use if connections_to_use > 0 and self.global_speed_limit > 0 else self.global_speed_limit
        
        creation_tasks = []
        for i in range(connections_to_use):
            # Sender 'i' handles parts i, i+N, i+2N ...
            # Initial part_id for this sender is 'i'
            initial_part_id_for_sender = i
            if initial_part_id_for_sender >= total_parts_in_file: # This sender has no parts to upload
                continue

            # Stride is number of connections (parts to skip for next upload by this sender)
            part_id_stride = connections_to_use
            
            creation_tasks.append(
                self._create_upload_sender(file_id, total_parts_in_file, is_large_file, 
                                           initial_part_id_for_sender, part_id_stride, 
                                           per_sender_speed_limit)
            )
        
        if not creation_tasks:
             logger.warning(f"PT No upload senders were scheduled (total parts {total_parts_in_file}, connections {connections_to_use})")
             self.senders = []
             return

        try:
            self.senders = await asyncio.gather(*creation_tasks)
            logger.info(f"PT Initialized {len(self.senders)} upload senders for {total_parts_in_file} total parts.")
        except Exception as e:
            logger.error(f"PT Failed to initialize all upload senders: {type(e).__name__} - {e}", exc_info=True)
            await self._cleanup()
            raise


    async def _create_upload_sender(self, file_id: int, total_parts: int, is_large: bool, initial_part_idx: int, part_idx_stride: int, speed_limit: int) -> UploadSender:
        mtp_sender = await self._create_sender()
        return UploadSender(
            self.client, mtp_sender, file_id, total_parts, is_large,
            initial_part_idx, part_idx_stride, loop=self.loop,
            speed_limit=speed_limit
        )

    async def _create_sender_for_dc(self, new_dc_id: int) -> MTProtoSender:
        # This method is called when a sender (e.g. DownloadSender) encounters FileMigrateError
        # It means this ParallelTransferrer instance must now operate on this new_dc_id for that specific sender.
        # However, the ParallelTransferrer itself has target_dc_id and current_auth_key.
        # If new_dc_id is different from self.target_dc_id, it means a sub-sender needs a connection to a new DC.
        # This structure implies that _create_sender should use new_dc_id and its specific auth.

        logger.info(f"PT Request to create sender for specific DC {new_dc_id} (current main target DC is {self.target_dc_id}).")
        # This new sender will have its own auth cycle if new_dc_id differs from client's session or previous target_dc_id's auth.
        # We pass new_dc_id and None for auth_key to _create_sender_internal to force auth if needed.
        return await self._create_sender_internal(dc_to_connect=new_dc_id, auth_key_for_dc=None)


    async def _create_sender(self) -> MTProtoSender:
        # This is for creating a sender for the ParallelTransferrer's main target_dc_id
        return await self._create_sender_internal(dc_to_connect=self.target_dc_id, auth_key_for_dc=self.current_auth_key)

    async def _create_sender_internal(self, dc_to_connect: int, auth_key_for_dc: Optional[AuthKey]) -> MTProtoSender:
        async with self._connection_management_lock:
            if self._active_connections_count >= MAX_CONNECTIONS_PER_TRANSFER:
                logger.error(f"PT Cannot create new sender for DC {dc_to_connect}: Max connections ({MAX_CONNECTIONS_PER_TRANSFER}) for this transfer already reached.")
                raise RuntimeError(f"Max connections ({MAX_CONNECTIONS_PER_TRANSFER}) reached")
            self._active_connections_count += 1
            logger.debug(f"PT Incremented active connections to {self._active_connections_count} (for DC {dc_to_connect}).")

        new_mtp_sender: Optional[MTProtoSender] = None
        try:
            dc_info = await self.client._get_dc(dc_to_connect)
            
            # Initialize sender. If auth_key_for_dc is None, it will attempt auth.
            new_mtp_sender = MTProtoSender(auth_key_for_dc, loggers=self.client._log)
            
            logger.debug(f"PT Connecting MTProtoSender to DC {dc_info.id} ({dc_info.ip_address}:{dc_info.port}). Provided auth_key is {'set' if auth_key_for_dc else 'None'}.")
            await new_mtp_sender.connect(
                self.client._connection(
                    dc_info.ip_address, dc_info.port, dc_info.id,
                    loggers=self.client._log, proxy=self.client._proxy,
                )
            )
            logger.debug(f"PT MTProtoSender TCP connection to DC {dc_info.id} established.")
            
            if not new_mtp_sender.auth_key: # True if auth_key_for_dc was None, or if internal state means no auth key yet
                logger.info(f"PT No valid auth key for sender to DC {dc_to_connect}. Exporting/Importing authorization.")
                exported_auth_obj = await self.client(ExportAuthorizationRequest(dc_to_connect))
                logger.debug(f"PT Authorization exported for DC {dc_to_connect}.")

                init_conn_req_obj = InitConnectionRequest(
                    api_id=self.client.api_id,
                    device_model=self.client.device_model or 'Unknown Device',
                    system_version=self.client.system_version or 'Unknown OS',
                    app_version=self.client.app_version or f'FastTelethon/{LAYER}', # Use client's app_version
                    system_lang_code=self.client.system_lang_code or 'en',
                    lang_pack='', 
                    lang_code=self.client.lang_code or 'en',
                    query=ImportAuthorizationRequest(id=exported_auth_obj.id, bytes=exported_auth_obj.bytes)
                )
                invoke_req = InvokeWithLayerRequest(LAYER, init_conn_req_obj)
                
                logger.debug(f"PT Sending InvokeWithLayerRequest(InitConnectionRequest(ImportAuthorizationRequest)) to DC {dc_to_connect}.")
                await new_mtp_sender.send(invoke_req) 
                
                if not new_mtp_sender.auth_key:
                    logger.error(f"PT CRITICAL: Auth key still not set for sender to DC {dc_to_connect} after ImportAuthorization.")
                    raise ConnectionError(f"Auth key not established for DC {dc_to_connect} after auth import.")
                
                logger.info(f"PT Successfully authorized sender and obtained new auth key for DC {dc_to_connect}.")
                # If this was for the ParallelTransferrer's main target DC, update its current_auth_key
                if dc_to_connect == self.target_dc_id:
                    self.current_auth_key = new_mtp_sender.auth_key
            else:
                logger.info(f"PT Using existing/provided auth key for sender to DC {dc_to_connect}.")
                
            return new_mtp_sender
        except Exception as e:
            logger.error(f"PT Error creating/authorizing sender for DC {dc_to_connect}: {type(e).__name__} - {str(e)}", exc_info=True)
            if new_mtp_sender and hasattr(new_mtp_sender, 'is_connected') and new_mtp_sender.is_connected():
                await new_mtp_sender.disconnect()
            async with self._connection_management_lock:
                self._active_connections_count -= 1 
                logger.debug(f"PT Decremented active connections to {self._active_connections_count} due to sender creation failure for DC {dc_to_connect}.")
            raise

    async def init_upload(self, file_id: int, file_size: int, part_size_kb: Optional[float] = None, connection_count: Optional[int] = None) -> Tuple[int, int, bool]:
        actual_connections = connection_count or self._get_connection_count(file_size, connection_count)
        
        # Determine part size, ensuring it's within Telethon's typical logic and our bounds
        calculated_part_size_kb = part_size_kb or utils.get_appropriated_part_size(file_size)
        part_size_bytes = int(calculated_part_size_kb * 1024)
        part_size_bytes = max(MIN_CHUNK_SIZE, min(part_size_bytes, MAX_CHUNK_SIZE))
        part_size_bytes = (part_size_bytes // 1024) * 1024 # Align to 1KB, though utils.get_appropriated_part_size often gives 512KB aligned
        if part_size_bytes == 0: part_size_bytes = MIN_CHUNK_SIZE

        total_parts = (file_size + part_size_bytes - 1) // part_size_bytes if file_size > 0 else 0
        is_large_file_format = file_size > 10 * 1024 * 1024 
        
        logger.info(f"PT Upload init: {actual_connections} connections, file_size={file_size/1024/1024:.2f}MB, part_size={part_size_bytes/1024}KB, total_parts={total_parts}, is_large_format={is_large_file_format}.")
        if total_parts == 0 and file_size > 0: # Should not happen if part_size_bytes > 0
             logger.warning(f"PT Upload init: Total parts is 0 for a non-empty file (size {file_size}). Check part size calculation. Forcing 1 part.")
             total_parts = 1
        elif file_size == 0:
            logger.info("PT Upload init: File size is 0. Will proceed with 0 parts if SaveFilePart handles it, or 1 part for metadata.")
            # Telegram might require at least one SaveFilePart even for empty files, or handle it via InputFile an empty MD5
            # For now, if file_size is 0, total_parts will be 0. This should be fine if is_large_file_format is false.
            # Let's ensure total_parts is at least 1 if not is_large_file_format and file_id is involved.
            if not is_large_file_format: # Small file logic might need a dummy part for MD5
                 total_parts = max(1, total_parts) # Ensure at least 1 part for small files for finalization
        
        await self._init_upload(actual_connections, file_id, total_parts, is_large_file_format)
        return part_size_bytes, total_parts, is_large_file_format

    async def upload(self, part_data: bytes) -> None:
        current_time = time.time()
        time_since_check = current_time - self.last_global_speed_check
        
        if time_since_check > SPEED_CHECK_INTERVAL: # Global speed check interval
            if time_since_check > 0 and self.bytes_since_global_check > 0 and self.global_speed_limit > 0:
                current_global_speed = self.bytes_since_global_check / time_since_check
                if current_global_speed > self.global_speed_limit:
                    required_duration = self.bytes_since_global_check / self.global_speed_limit
                    wait_duration = required_duration - time_since_check
                    if wait_duration > 0:
                        logger.debug(f"PT Global upload throttle: Speed {current_global_speed/1024:.2f}KB/s > limit {self.global_speed_limit/1024:.2f}KB/s. Waiting {wait_duration:.2f}s")
                        await asyncio.sleep(wait_duration)
            
            self.last_global_speed_check = time.time() 
            self.bytes_since_global_check = 0
        
        self.bytes_since_global_check += len(part_data)
        
        if not self.senders:
            logger.error("PT Upload called but no senders initialized.")
            raise RuntimeError("Upload senders are not initialized for ParallelTransferrer.")

        # Round-robin dispatch to upload senders
        active_sender_instance = self.senders[self.upload_ticker]
        await active_sender_instance.next(part_data) 
        self.upload_ticker = (self.upload_ticker + 1) % len(self.senders)

    async def finish_upload(self) -> None:
        logger.info("PT Finishing upload, ensuring all sender tasks complete and cleaning up...")
        # Ensure all pending writes from UploadSenders are awaited
        for sender_instance in self.senders:
            if isinstance(sender_instance, UploadSender) and sender_instance.previous and not sender_instance.previous.done():
                try:
                    await sender_instance.previous
                except Exception as e:
                    logger.warning(f"PT Exception awaiting final task for sender during finish_upload: {e}")
        await self._cleanup()
        logger.info("PT Upload finished and senders cleaned up.")

    async def download(self, file_loc_obj: TypeLocation, file_total_size: int, part_size_kb: Optional[float] = None, connection_count: Optional[int] = None) -> AsyncGenerator[bytes, None]:
        logger.info(f"PT Starting parallel download: size {file_total_size/1024/1024:.2f} MB from DC {self.target_dc_id}.")
        actual_connections_to_use = connection_count or self._get_connection_count(file_total_size, connection_count)
        
        calculated_part_size_kb = part_size_kb or utils.get_appropriated_part_size(file_total_size)
        part_size_bytes_val = int(calculated_part_size_kb * 1024)
        part_size_bytes_val = max(MIN_CHUNK_SIZE, min(part_size_bytes_val, MAX_CHUNK_SIZE))
        part_size_bytes_val = (part_size_bytes_val // 1024) * 1024 
        if part_size_bytes_val == 0: part_size_bytes_val = MIN_CHUNK_SIZE

        if file_total_size == 0:
            logger.info("PT Download: File size is 0. Yielding nothing.")
            yield b"" # Yield empty bytes to signify completion for zero-size file if needed by caller
            return

        await self._init_download(actual_connections_to_use, file_loc_obj, file_total_size, part_size_bytes_val)

        if not self.senders:
            logger.error("PT Download cannot proceed: no download senders were initialized.")
            return

        retrieved_part_count = 0
        # total_parts_expected = math.ceil(file_total_size / part_size_bytes_val) if part_size_bytes_val > 0 else 0
        # Calculate total_parts_expected based on sum of parts each sender will fetch
        total_parts_expected = sum(s.remaining for s in self.senders if isinstance(s, DownloadSender))

        logger.info(f"PT Download: All senders initialized. Expecting {total_parts_expected} parts in total across all senders.")

        # Manage tasks for each sender
        # Map sender index to its current task to avoid re-launching if already running
        sender_tasks: Dict[int, asyncio.Task] = {}
        
        try:
            while retrieved_part_count < total_parts_expected:
                # Global speed throttling
                current_time_g = time.time()
                time_since_check_g = current_time_g - self.last_global_speed_check
                if time_since_check_g > SPEED_CHECK_INTERVAL:
                    if time_since_check_g > 0 and self.bytes_since_global_check > 0 and self.global_speed_limit > 0:
                        current_speed_g = self.bytes_since_global_check / time_since_check_g
                        if current_speed_g > self.global_speed_limit:
                            required_time_g = self.bytes_since_global_check / self.global_speed_limit
                            wait_time_g = required_time_g - time_since_check_g
                            if wait_time_g > 0:
                                logger.debug(f"PT Global download throttle: Speed {current_speed_g/1024:.2f}KB/s > limit {self.global_speed_limit/1024:.2f}KB/s. Waiting {wait_time_g:.2f}s")
                                await asyncio.sleep(wait_time_g)
                    self.last_global_speed_check = time.time()
                    self.bytes_since_global_check = 0

                # Launch new tasks for senders that are ready and don't have an active task
                for idx, sender_instance in enumerate(self.senders):
                    if isinstance(sender_instance, DownloadSender) and sender_instance.remaining > 0:
                        if idx not in sender_tasks or sender_tasks[idx].done():
                            sender_tasks[idx] = self.loop.create_task(sender_instance.next())
                
                if not sender_tasks: # No tasks could be launched or all done
                    if retrieved_part_count < total_parts_expected:
                        logger.warning(f"PT No active download tasks, but expecting more parts ({retrieved_part_count}/{total_parts_expected}).")
                    break 

                done_tasks, _ = await asyncio.wait(sender_tasks.values(), return_when=asyncio.FIRST_COMPLETED)
                
                for task in done_tasks:
                    # Find which sender this task belonged to
                    task_owner_idx = -1
                    for idx, t in sender_tasks.items():
                        if t == task:
                            task_owner_idx = idx
                            break
                    
                    try:
                        data_chunk_content = await task 
                    except Exception as e:
                        logger.error(f"PT Download task for sender {task_owner_idx if task_owner_idx != -1 else 'Unknown'} failed: {type(e).__name__} - {e}")
                        if task_owner_idx != -1:
                            # This sender's task failed. Remove it. It might be restarted if its .next() handles errors and allows retries internally
                            # or if it was FileMigrateError which replaced the sender.
                            del sender_tasks[task_owner_idx] 
                        # Depending on error, may need to stop or mark sender as failed.
                        # If it was FileMigrate, sender.next() would have handled it by re-raising or returning another awaitable.
                        continue # Process next completed task

                    if data_chunk_content:
                        yield data_chunk_content
                        self.bytes_since_global_check += len(data_chunk_content)
                        retrieved_part_count += 1
                        # Task for this sender is done, it will be re-added in the next loop if sender has more 'remaining'
                        if task_owner_idx != -1:
                             del sender_tasks[task_owner_idx] # Allow re-scheduling for this sender
                    else: # Sender returned None, meaning it's exhausted or hit a non-retryable issue internally
                        if task_owner_idx != -1:
                            logger.debug(f"PT Sender {task_owner_idx} reported no more data (returned None).")
                            # Sender is considered finished, remove its task from management
                            del sender_tasks[task_owner_idx] 
                        # Check if remaining parts for this sender was > 0 before this, could indicate issue.
                        # if self.senders[task_owner_idx].remaining > 0:
                        #    logger.warning(f"PT Sender {task_owner_idx} returned None but still had {self.senders[task_owner_idx].remaining} parts marked remaining.")
                
                # If all retrievable parts are done, but some tasks might still be in sender_tasks if they were not in done_tasks
                if retrieved_part_count >= total_parts_expected:
                    logger.info("PT All expected parts have been retrieved.")
                    break
                
                # Check if all senders are exhausted (no remaining parts or tasks)
                all_senders_exhausted = True
                for idx, sender_instance in enumerate(self.senders):
                    if isinstance(sender_instance, DownloadSender) and sender_instance.remaining > 0:
                        if idx not in sender_tasks or sender_tasks[idx].done(): # if no active task or task is done
                             all_senders_exhausted = False # This sender could still provide parts
                             break
                if all_senders_exhausted and not any(not t.done() for t in sender_tasks.values()):
                     if retrieved_part_count < total_parts_expected:
                          logger.warning(f"PT All senders appear exhausted but download is incomplete ({retrieved_part_count}/{total_parts_expected}).")
                     break


            if retrieved_part_count < total_parts_expected:
                logger.warning(f"PT Download finished with {retrieved_part_count}/{total_parts_expected} parts retrieved. File may be incomplete.")

        finally:
            # Cancel any tasks that are still outstanding in sender_tasks
            for task in sender_tasks.values():
                if not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError: pass 
                    except Exception as e: logger.warning(f"PT Exception cancelling a download task in finally: {e}")
            
            await self._cleanup()
            logger.info(f"PT Parallel download process finished. Total parts yielded: {retrieved_part_count}.")


async def _internal_transfer_to_telegram(client: TelegramClient, file_stream: BinaryIO, filename_on_tg: str, progress_callback: Optional[callable]) -> Tuple[TypeInputFile, int]:
    logger.info(f"Starting internal Telegram transfer for file: {getattr(file_stream, 'name', filename_on_tg)}")
    
    # Generate a random file ID for Telegram's tracking
    telegram_file_id = helpers.generate_random_long()
    
    # Get file size
    file_stream.seek(0, os.SEEK_END)
    actual_file_size = file_stream.tell()
    file_stream.seek(0)

    if actual_file_size == 0:
        logger.warning(f"File {filename_on_tg} is empty. Handling as an empty file upload.")
    
    md5_hasher = hashlib.md5()
    uploader_instance: Optional[ParallelTransferrer] = None

    try:
        async with parallel_transfer_semaphore: # Limits concurrent _internal_transfer_to_telegram operations
            uploader_instance = ParallelTransferrer(client, speed_limit=MAX_DOWNLOAD_SPEED) # Default global speed limit for this upload
            
            # init_upload determines part_size, part_count, and if it's a "big file" for Telegram's API
            part_size_bytes, total_parts_count, use_large_file_api = await uploader_instance.init_upload(telegram_file_id, actual_file_size)
            
            upload_buffer = bytearray()
            accumulated_bytes_for_callback = 0
            
            logger.info(f"Upload Details: File size: {actual_file_size/1024/1024:.2f} MB, Part size: {part_size_bytes/1024:.2f} KB, Total parts: {total_parts_count}, Large file API: {use_large_file_api}")

            if actual_file_size == 0 and not use_large_file_api: # Handling for 0-byte small files
                # Small empty files might need an empty part or just MD5 of empty string.
                # Let's ensure at least one "upload" call if total_parts_count is 1 (due to small file logic in init_upload)
                if total_parts_count == 1 and part_size_bytes > 0 : # if init_upload decided 1 part
                     # md5_hasher.update(b"") # MD5 of empty content
                     await uploader_instance.upload(b"") # Send one empty part
                     logger.info("Uploaded one empty part for zero-byte small file.")
                elif total_parts_count == 0: # if init_upload said 0 parts
                     logger.info("Zero-byte file, no parts to upload based on init_upload.")
                     # md5_hasher.update(b"") still needs to be correct for InputFile
                if progress_callback: progress_callback(0,0)

            else: # Normal file upload logic
                for data_read_from_file in stream_file(file_stream, chunk_size=part_size_bytes): # Read in optimal chunks
                    if not data_read_from_file: break 

                    if not use_large_file_api: # MD5 for small files only, calculated progressively
                        md5_hasher.update(data_read_from_file)
                    
                    upload_buffer.extend(data_read_from_file)
                    
                    # Process full parts from buffer
                    while len(upload_buffer) >= part_size_bytes and part_size_bytes > 0:
                        part_to_send_to_telegram = upload_buffer[:part_size_bytes]
                        await uploader_instance.upload(bytes(part_to_send_to_telegram))
                        del upload_buffer[:part_size_bytes]
                        
                        accumulated_bytes_for_callback += len(part_to_send_to_telegram)
                        if progress_callback:
                            try:
                                cb_res = progress_callback(accumulated_bytes_for_callback, actual_file_size)
                                if inspect.isawaitable(cb_res): await cb_res
                            except Exception as e_cb: logger.error(f"Progress callback error: {str(e_cb)}")
                    
            # Send any remaining data in the buffer (last part, possibly smaller)
            if len(upload_buffer) > 0:
                await uploader_instance.upload(bytes(upload_buffer))
                accumulated_bytes_for_callback += len(upload_buffer)
                if progress_callback: # Final callback update
                     try:
                        cb_res = progress_callback(accumulated_bytes_for_callback, actual_file_size)
                        if inspect.isawaitable(cb_res): await cb_res
                     except Exception as e_cb: logger.error(f"Progress callback error (final): {str(e_cb)}")

            await uploader_instance.finish_upload()
            logger.info(f"File upload {filename_on_tg} processing completed by ParallelTransferrer.")

    except Exception as e_main_upload:
        logger.error(f"Upload of {filename_on_tg} failed: {type(e_main_upload).__name__} - {str(e_main_upload)}", exc_info=True)
        if uploader_instance: 
            logger.info("Attempting cleanup of uploader instance after failure...")
            await uploader_instance._cleanup()
        raise 

    if use_large_file_api:
        return InputFileBig(telegram_file_id, total_parts_count, filename_on_tg), actual_file_size
    else:
        # For small files, even if empty, total_parts_count should be from init_upload (likely 1)
        # MD5 of empty string if file was empty
        if actual_file_size == 0:
            md5_hasher.update(b"")

        return InputFile(telegram_file_id, total_parts_count, filename_on_tg, md5_hasher.hexdigest()), actual_file_size


def stream_file(file_to_stream: BinaryIO, chunk_size=1024*256) -> Generator[bytes, None, None]: # Default 256KB chunks
    # logger.debug(f"SF Streaming file: {getattr(file_to_stream, 'name', 'UnnamedStream')}, chunk_size={chunk_size/1024}KB")
    while True:
        data_read = file_to_stream.read(chunk_size)
        if not data_read:
            # logger.debug("SF End of file stream.")
            break
        yield data_read

async def cleanup_connections(): 
    logger.info("PT Global connection cleanup attempt...")
    tasks_to_cancel = []
    for task in asyncio.all_tasks():
        if task.done(): continue
        try:
            coro_name = str(task.get_coro())
            # Be specific to avoid cancelling unrelated tasks
            if '_send_loop' in coro_name and 'MTProtoSender' in coro_name or \
               '_recv_loop' in coro_name and 'MTProtoSender' in coro_name:
                 tasks_to_cancel.append(task)
        except Exception: pass # Some tasks might not have get_coro() or it might fail

    if not tasks_to_cancel:
        logger.info("PT Global cleanup: No active MTProtoSender send/receive loop tasks found.")
        return

    logger.info(f"PT Global cleanup: Attempting to cancel {len(tasks_to_cancel)} identified MTProtoSender tasks.")
    for task in tasks_to_cancel: task.cancel()
    
    results = await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
    cancelled_ok_count = sum(1 for r in results if isinstance(r, asyncio.CancelledError))
    logger.info(f"PT Global cleanup: {cancelled_ok_count}/{len(tasks_to_cancel)} tasks confirmed cancelled.")


def format_bytes(size_bytes: int) -> str:
    if size_bytes < 0: return "0 B" # Or handle error
    power = 1024 
    n = 0
    power_labels = {0: 'B', 1: 'KB', 2: 'MB', 3: 'GB', 4: 'TB'}
    while abs(size_bytes) >= power and n < len(power_labels) - 1 :
        size_bytes /= power
        n += 1
    return f"{size_bytes:.2f} {power_labels[n]}"

async def log_transfer_stats(interval_seconds: int = 30): # More frequent logging
    process = None
    try:
        process = psutil.Process(os.getpid())
        logger.info("Background stats logger initialized.")
    except Exception as e:
        logger.warning(f"psutil not available or failed to init for memory logging: {e}")

    while True:
        await asyncio.sleep(interval_seconds)
        try:
            if process:
                try:
                    mem_info = process.memory_info()
                    logger.debug(f"STATS: Memory RSS={format_bytes(mem_info.rss)}, VMS={format_bytes(mem_info.vms)}")
                except psutil.NoSuchProcess:
                    logger.warning("STATS: Process for memory stats vanished. Stopping logger.")
                    break
                except Exception as e_mem: logger.error(f"STATS: Error getting memory info: {e_mem}")

            active_async_tasks = sum(1 for t in asyncio.all_tasks() if not t.done())
            logger.debug(f"STATS: Active asyncio tasks: {active_async_tasks}")

        except asyncio.CancelledError:
            logger.info("Stats logger task was cancelled.")
            break
        except Exception as e_stat_loop:
            logger.error(f"STATS: Error in stats logger loop: {e_stat_loop}")


async def download_file(
    client: TelegramClient, 
    media_location: TypeLocation, 
    output_stream: BinaryIO, 
    progress_callback: Optional[callable] = None, 
    speed_limit_mbps: Optional[float] = None, # Allow user to specify in Mbps
    part_size_kb: Optional[float] = None,
    connection_count: Optional[int] = None
) -> BinaryIO:
    output_name_log = getattr(output_stream, 'name', 'output_stream')
    logger.info(f"Download request for media to: {output_name_log}")

    if not hasattr(media_location, 'size') or not isinstance(getattr(media_location, 'size', None), int):
        logger.error(f"Media location object {type(media_location)} lacks a valid integer 'size' attribute.")
        raise ValueError("Media location object must have an integer 'size' attribute (e.g., Document, Photo).")

    file_actual_size = media_location.size
    
    try:
        target_dc_id, input_media_location = utils.get_input_location(media_location)
    except Exception as e_loc:
        logger.error(f"Failed to get input location from media object: {e_loc}")
        raise
    
    effective_speed_limit_bytes = int((speed_limit_mbps or MAX_DOWNLOAD_SPEEDS) * 1024 * 1024)
    logger.info(f"Starting download: size {format_bytes(file_actual_size)}, DC {target_dc_id}, SpeedLimit: {effective_speed_limit_bytes/1024/1024:.2f}MB/s.")
    
    downloader_instance: Optional[ParallelTransferrer] = None
    try:
        async with parallel_transfer_semaphore: 
            downloader_instance = ParallelTransferrer(client, target_dc_id, speed_limit=effective_speed_limit_bytes)
            
            bytes_written_total = 0
            async for data_block in downloader_instance.download(input_media_location, file_actual_size, 
                                                                  part_size_kb=part_size_kb, 
                                                                  connection_count=connection_count):
                output_stream.write(data_block)
                bytes_written_total += len(data_block)
                if progress_callback:
                    try:
                        cb_res = progress_callback(bytes_written_total, file_actual_size)
                        if inspect.isawaitable(cb_res): await cb_res
                    except Exception as e_cb_dl: logger.error(f"Download progress callback error: {str(e_cb_dl)}")
            
            logger.info(f"Download to {output_name_log} completed. Total bytes written: {format_bytes(bytes_written_total)}.")

    except Exception as e_dl_main:
        logger.error(f"Download to {output_name_log} failed: {type(e_dl_main).__name__} - {str(e_dl_main)}", exc_info=True)
        raise
    
    return output_stream

async def upload_file(
    client: TelegramClient, 
    input_file_stream: BinaryIO, 
    telegram_filename: str, 
    progress_callback: Optional[callable] = None,
    speed_limit_mbps: Optional[float] = None # Added option to control upload speed
) -> TypeInputFile:
    input_file_log_name = getattr(input_file_stream, 'name', telegram_filename)
    logger.info(f"Upload request for file: {input_file_log_name} as '{telegram_filename}' on Telegram.")
    
    # _internal_transfer_to_telegram will create its own ParallelTransferrer.
    # To pass speed limit, _internal_transfer_to_telegram needs to accept it and pass to PT.
    # For now, _internal_transfer_to_telegram uses a default. This can be enhanced if needed.
    # Let's assume _internal_transfer_to_telegram should also respect speed_limit_mbps for consistency.
    # This requires modifying _internal_transfer_to_telegram signature and its PT instantiation.
    # (This change is not made in this iteration as it's a new feature, focusing on the ImportError fix)

    input_file_tg_obj, _ = await _internal_transfer_to_telegram(client, input_file_stream, telegram_filename, progress_callback)
    logger.info(f"Upload of {input_file_log_name} as '{telegram_filename}' completed.")
    return input_file_tg_obj

# To start background monitoring if this module is used:
# Call this from your main application setup if desired.
# Example:
# async def main():
#     # ... your client setup ...
#     asyncio.create_task(log_transfer_stats())
#     # ... rest of your app ...

# if __name__ == "__main__":
#     # Basic example setup if running this file directly (for testing)
#     # This is not a complete runnable example without a client and file.
#     async def test_run():
#         logging.basicConfig(level=logging.DEBUG) # More verbose for testing
#         logger.info("Test run started. Configure client and call upload/download.")
#         # Example: await log_transfer_stats()
#     asyncio.run(test_run())
