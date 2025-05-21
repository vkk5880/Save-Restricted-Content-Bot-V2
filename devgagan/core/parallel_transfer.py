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
import time
from collections import defaultdict
from os import getenv
from typing import (
    AsyncGenerator,
    Awaitable,
    BinaryIO,
    DefaultDict,
    List,
    Optional,
    Tuple,
    Union,
)

from telethon import TelegramClient, helpers, utils
from telethon.crypto import AuthKey
from telethon.errors import (
    ChannelInvalidError,
    ChannelPrivateError,
    ChatIdInvalidError,
    ChatInvalidError,
    FloodWaitError,
)
from telethon.network import MTProtoSender
from telethon.tl.alltlobjects import LAYER
from telethon.tl.functions import InvokeWithLayerRequest
from telethon.tl.functions.auth import (
    ExportAuthorizationRequest,
    ImportAuthorizationRequest,
)
from telethon.tl.functions.upload import (
    GetFileRequest,
    SaveBigFilePartRequest,
    SaveFilePartRequest,
)
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

filename = ""
log = logging.getLogger("FastTelethon")

TypeLocation = Union[
    Document,
    InputDocumentFileLocation,
    InputPeerPhotoFileLocation,
    InputFileLocation,
    InputPhotoFileLocation,
]

parallel_transfer_semaphore = asyncio.Semaphore(MAX_PARALLEL_TRANSFERS)
parallel_transfer_locks = defaultdict(lambda: asyncio.Lock())


class DownloadSender:
    def __init__(
        self,
        client: TelegramClient,
        sender: MTProtoSender,
        file: TypeLocation,
        offset: int,
        limit: int,
        stride: int,
        count: int,
        speed_limit: int = MAX_DOWNLOAD_SPEED
    ) -> None:
        self.client = client
        self.sender = sender
        self.request = GetFileRequest(file, offset=offset, limit=limit)
        self.stride = stride
        self.remaining = count
        self.last_chunk_time = time.time()
        self.last_chunk_size = 0
        self.speed_limit = speed_limit
        self.request_times = []
        self.total_bytes_transferred = 0
        self.start_time = time.time()
        log.debug(f"Initialized DownloadSender with chunk size: {limit/1024:.2f} KB")

    async def next(self) -> Optional[bytes]:
        if not self.remaining:
            log.debug("No more chunks remaining")
            return None
        
        # Dynamic throttling based on speed limit
        current_time = time.time()
        time_diff = current_time - self.last_chunk_time
        if time_diff > 0:
            current_speed = self.last_chunk_size / time_diff if self.last_chunk_size else 0
            if current_speed > self.speed_limit:
                wait_time = (self.last_chunk_size / self.speed_limit) - time_diff
                if wait_time > 0:
                    log.debug(f"Throttling: Speed {current_speed/1024:.2f} KB/s exceeds limit {self.speed_limit/1024:.2f} KB/s, waiting {wait_time:.2f}s")
                    await asyncio.sleep(wait_time)
        
        try:
            request_start = time.time()
            result = await self.client._call(self.sender, self.request)
            request_end = time.time()
            
            chunk_size = len(result.bytes)
            self.request_times.append(request_start)
            self.total_bytes_transferred += chunk_size
            
            log.debug(
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
            log.warning(f"Flood wait error, sleeping for {e.seconds} seconds")
            await asyncio.sleep(e.seconds)
            return await self.next()

    def _log_metrics(self) -> None:
        """Log performance metrics periodically"""
        if not self.request_times:
            return
            
        current_time = time.time()
        elapsed = current_time - self.start_time
        
        # Calculate requests per second
        recent_requests = [t for t in self.request_times if t > current_time - 10]
        rps = len(recent_requests) / 10 if len(recent_requests) > 0 else 0
        
        # Calculate average speed
        avg_speed = self.total_bytes_transferred / elapsed if elapsed > 0 else 0
        
        log.info(
            f"Transfer stats: "
            f"Total chunks: {len(self.request_times)} | "
            f"RPS: {rps:.2f} | "
            f"Avg speed: {avg_speed/1024:.2f} KB/s | "
            f"Total data: {self.total_bytes_transferred/1024/1024:.2f} MB"
        )

    async def disconnect(self) -> None:
        """Disconnect and log final stats"""
        await self.sender.disconnect()
        elapsed = time.time() - self.start_time
        log.info(
            f"Transfer complete: "
            f"Total chunks: {len(self.request_times)} | "
            f"Total data: {self.total_bytes_transferred/1024/1024:.2f} MB | "
            f"Avg speed: {self.total_bytes_transferred/elapsed/1024:.2f} KB/s"
        )


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
        speed_limit: int = MAX_DOWNLOAD_SPEED
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
        self.speed_limit = speed_limit

    async def next(self, data: bytes) -> None:
        if self.previous:
            await self.previous
        
        # Dynamic throttling
        current_time = time.time()
        time_diff = current_time - self.last_chunk_time
        if time_diff > 0:
            current_speed = self.last_chunk_size / time_diff
            if current_speed > self.speed_limit:
                wait_time = (self.last_chunk_size / self.speed_limit) - time_diff
                if wait_time > 0:
                    await asyncio.sleep(wait_time)
        
        self.previous = self.loop.create_task(self._next(data))

    async def _next(self, data: bytes) -> None:
        try:
            self.request.bytes = data
            chunk_size = len(data)
            await self.client._call(self.sender, self.request)
            self.last_chunk_size = chunk_size
            self.last_chunk_time = time.time()
            self.request.file_part += self.stride
        except FloodWaitError as e:
            log.warning(f"Flood wait error, sleeping for {e.seconds} seconds uploading")
            await asyncio.sleep(e.seconds)
            await self._next(data)

    async def disconnect(self) -> None:
        if self.previous:
            await self.previous
        await self.sender.disconnect()


class ParallelTransferrer:
    def __init__(self, client: TelegramClient, dc_id: Optional[int] = None, speed_limit: int = MAX_DOWNLOAD_SPEED) -> None:
        self.client = client
        self.loop = self.client.loop
        self.dc_id = dc_id or self.client.session.dc_id
        self.auth_key = None if dc_id and self.client.session.dc_id != dc_id else self.client.session.auth_key
        self.senders = None
        self.upload_ticker = 0
        self.speed_limit = speed_limit
        self.last_speed_check = time.time()
        self.bytes_transferred_since_check = 0
        self._connection_lock = asyncio.Lock()
        self._active_connections = 0

    async def _cleanup(self) -> None:
        if self.senders:
            await asyncio.gather(*[sender.disconnect() for sender in self.senders])
            async with self._connection_lock:
                self._active_connections -= len(self.senders)
            self.senders = None

    @staticmethod
    def _get_connection_count(file_size: int, max_count: int = MAX_CONNECTIONS_PER_TRANSFER, full_size: int = 100 * 1024 * 1024) -> int:
        if file_size > full_size:
            return min(max_count, MAX_CONNECTIONS_PER_TRANSFER)
        return min(math.ceil((file_size / full_size) * max_count), MAX_CONNECTIONS_PER_TRANSFER)

    async def _init_download(self, connections: int, file: TypeLocation, part_count: int, part_size: int) -> None:
        # Validate inputs
        if connections <= 0 or part_size <= 0:
            raise ValueError("Connections and part_size must be positive")

        # Ensure valid chunk size
        part_size = max(MIN_CHUNK_SIZE, min(part_size, MAX_CHUNK_SIZE))
        part_size = (part_size // 1024) * 1024  # Round to nearest KB

        minimum, remainder = divmod(part_count, connections)

        def get_part_count() -> int:
            nonlocal remainder
            if remainder > 0:
                remainder -= 1
                return minimum + 1
            return minimum

        try:
            # Create first sender (handles auth export if cross-DC)
            first_sender = await self._create_download_sender(
                file, 0, part_size, connections * part_size, get_part_count(), self.speed_limit
            )
            self.senders = [first_sender]

            # Create remaining senders
            remaining_parts = part_count - (part_count // connections)
            self.senders.extend(await asyncio.gather(*[
                self._create_download_sender(
                    file, i, part_size, connections * part_size,
                    part_count // connections + (1 if i < remaining_parts else 0),
                    self.speed_limit
                )
                for i in range(1, min(connections, MAX_CONNECTIONS_PER_TRANSFER))
            ]))

            log.info(f"Initialized {len(self.senders)} senders with {part_size/1024} KB chunks")
        except Exception as e:
            await self._cleanup()
            raise

    async def _create_download_sender(self, file: TypeLocation, index: int, part_size: int, stride: int, part_count: int, speed_limit: int) -> DownloadSender:
        return DownloadSender(
            self.client,
            await self._create_sender(),
            file,
            index * part_size,
            part_size,
            stride,
            part_count,
            speed_limit
        )

    async def _init_upload(self, connections: int, file_id: int, part_count: int, big: bool) -> None:
        self.senders = [
            await self._create_upload_sender(file_id, part_count, big, 0, connections, self.speed_limit),
            *await asyncio.gather(*[
                self._create_upload_sender(file_id, part_count, big, i, connections, self.speed_limit)
                for i in range(1, min(connections, MAX_CONNECTIONS_PER_TRANSFER))
            ])
        ]

    async def _create_upload_sender(self, file_id: int, part_count: int, big: bool, index: int, stride: int, speed_limit: int) -> UploadSender:
        return UploadSender(
            self.client,
            await self._create_sender(),
            file_id,
            part_count,
            big,
            index,
            stride,
            loop=self.loop,
            speed_limit=speed_limit
        )

    async def _create_sender(self) -> MTProtoSender:
        async with self._connection_lock:
            if self._active_connections >= MAX_CONNECTIONS_PER_TRANSFER:
                raise RuntimeError("Max connections reached")
            self._active_connections += 1

        try:
            dc = await self.client._get_dc(self.dc_id)
            sender = MTProtoSender(self.auth_key, loggers=self.client._log)
            await sender.connect(
                self.client._connection(
                    dc.ip_address,
                    dc.port,
                    dc.id,
                    loggers=self.client._log,
                    proxy=self.client._proxy,
                )
            )
            
            if not self.auth_key:
                auth = await self.client(ExportAuthorizationRequest(self.dc_id))
                self.client._init_request.query = ImportAuthorizationRequest(id=auth.id, bytes=auth.bytes)
                req = InvokeWithLayerRequest(LAYER, self.client._init_request)
                await sender.send(req)
                self.auth_key = sender.auth_key
                
            return sender
        except Exception as e:
            async with self._connection_lock:
                self._active_connections -= 1
            raise

    async def init_upload(self, file_id: int, file_size: int, part_size_kb: Optional[float] = None, connection_count: Optional[int] = None) -> Tuple[int, int, bool]:
        connection_count = connection_count or self._get_connection_count(file_size)
        part_size = (part_size_kb or utils.get_appropriated_part_size(file_size)) * 1024
        part_count = (file_size + part_size - 1) // part_size
        is_large = file_size > 10 * 1024 * 1024
        await self._init_upload(connection_count, file_id, part_count, is_large)
        return part_size, part_count, is_large

    async def upload(self, part: bytes) -> None:
        current_time = time.time()
        if current_time - self.last_speed_check > SPEED_CHECK_INTERVAL:
            speed = self.bytes_transferred_since_check / (current_time - self.last_speed_check)
            if speed > self.speed_limit:
                wait_time = (self.bytes_transferred_since_check / self.speed_limit) - (current_time - self.last_speed_check)
                if wait_time > 0:
                    await asyncio.sleep(wait_time)
            
            self.last_speed_check = time.time()
            self.bytes_transferred_since_check = 0
        
        self.bytes_transferred_since_check += len(part)
        await self.senders[self.upload_ticker].next(part)
        self.upload_ticker = (self.upload_ticker + 1) % len(self.senders)

    async def finish_upload(self) -> None:
        await self._cleanup()

    async def download(self, file: TypeLocation, file_size: int, part_size_kb: Optional[float] = None, connection_count: Optional[int] = None) -> AsyncGenerator[bytes, None]:
        # Validate file location
        if not isinstance(file, (InputDocumentFileLocation, InputPhotoFileLocation)):
            raise TypeError("Invalid file location type")

        connection_count = connection_count or self._get_connection_count(file_size)
        part_size = (part_size_kb or utils.get_appropriated_part_size(file_size)) * 1024
        part_size = max(MIN_CHUNK_SIZE, min(part_size, MAX_CHUNK_SIZE))
        part_size = (part_size // 1024) * 1024  # Round to nearest KB
        
        log.debug(f"Calculated chunk size: {part_size / 1024:.2f} KB")
        part_count = math.ceil(file_size / part_size)
        
        await self._init_download(connection_count, file, part_count, part_size)

        part = 0
        last_check_time = time.time()
        bytes_since_check = 0
        
        while part < part_count:
            current_time = time.time()
            if current_time - last_check_time > SPEED_CHECK_INTERVAL:
                speed = bytes_since_check / (current_time - last_check_time)
                if speed > self.speed_limit:
                    wait_time = (bytes_since_check / self.speed_limit) - (current_time - last_check_time)
                    if wait_time > 0:
                        await asyncio.sleep(wait_time)
                
                last_check_time = time.time()
                bytes_since_check = 0
            
            tasks = [self.loop.create_task(sender.next()) for sender in self.senders]
            
            for task in tasks:
                data = await task
                if not data:
                    break
                bytes_since_check += len(data)
                yield data
                part += 1
                
        await self._cleanup()


def stream_file(file_to_stream: BinaryIO, chunk_size=1024):
    while True:
        data_read = file_to_stream.read(chunk_size)
        if not data_read:
            break
        yield data_read


async def _internal_transfer_to_telegram(client: TelegramClient, response: BinaryIO, progress_callback: callable) -> Tuple[TypeInputFile, int]:
    file_id = helpers.generate_random_long()
    file_size = os.path.getsize(response.name)

    hash_md5 = hashlib.md5()
    async with parallel_transfer_semaphore:
        uploader = ParallelTransferrer(client)
        part_size, part_count, is_large = await uploader.init_upload(file_id, file_size)
        buffer = bytearray()
        
        for data in stream_file(response):
            if progress_callback:
                r = progress_callback(response.tell(), file_size)
                if inspect.isawaitable(r):
                    await r
            
            if not is_large:
                hash_md5.update(data)
            
            if len(buffer) == 0 and len(data) == part_size:
                await uploader.upload(data)
                continue
                
            new_len = len(buffer) + len(data)
            if new_len >= part_size:
                cutoff = part_size - len(buffer)
                buffer.extend(data[:cutoff])
                await uploader.upload(bytes(buffer))
                buffer.clear()
                buffer.extend(data[cutoff:])
            else:
                buffer.extend(data)
                
        if len(buffer) > 0:
            await uploader.upload(bytes(buffer))
            
        await uploader.finish_upload()
        
    return (InputFileBig(file_id, part_count, filename), file_size) if is_large else (InputFile(file_id, part_count, filename, hash_md5.hexdigest()), file_size)


async def download_file(client: TelegramClient, location: TypeLocation, out: BinaryIO, progress_callback: callable = None, speed_limit: int = MAX_DOWNLOAD_SPEED) -> BinaryIO:
    size = location.size
    dc_id, location = utils.get_input_location(location)
    
    async with parallel_transfer_semaphore:
        downloader = ParallelTransferrer(client, dc_id, speed_limit=speed_limit)
        downloaded = downloader.download(location, size)
        
        async for x in downloaded:
            out.write(x)
            if progress_callback:
                r = progress_callback(out.tell(), size)
                if inspect.isawaitable(r):
                    await r

    return out


async def upload_file(client: TelegramClient, file: BinaryIO, name: str, progress_callback: callable = None, speed_limit: int = MAX_DOWNLOAD_SPEED) -> TypeInputFile:
    global filename
    filename = name
    return (await _internal_transfer_to_telegram(client, file, progress_callback))[0]
