"""
> Based on parallel_file_transfer.py from mautrix-telegram, with permission to distribute under the MIT license
> Copyright (C) 2019 Tulir Asokan - https://github.com/tulir/mautrix-telegram
"""
import asyncio
import hashlib
import inspect
import logging
import math
import os
import time
from collections import defaultdict
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

from telethon.errors import (
    ChannelInvalidError, 
    ChannelPrivateError, 
    ChatIdInvalidError, 
    ChatInvalidError,
    FloodWaitError
)
from telethon import TelegramClient, helpers, utils
from telethon.crypto import AuthKey
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

# Transfer control configuration
MAX_DOWNLOAD_SPEEDS = int(getenv("MAX_DOWNLOAD_SPEED", "15"))
MAX_CHUNK_SIZES = int(getenv("MAX_CHUNK_SIZE", "512"))
SPEED_CHECK_INTERVALS = int(getenv("SPEED_CHECK_INTERVAL", "1"))
MAX_PARALLEL_TRANSFERS = int(getenv("MAX_PARALLEL_TRANSFERS", "4"))  # Maximum number of simultaneous file transfers
MAX_CONNECTIONS_PER_TRANSFER = int(getenv("MAX_CONNECTIONS_PER_TRANSFER", "5"))  # Maximum connections per file transfer

MAX_DOWNLOAD_SPEED = MAX_DOWNLOAD_SPEEDS * 1024 * 1024  # 15 Mbps in bytes (adjustable)
MIN_CHUNK_SIZE = 64 * 1024  # 64KB
MAX_CHUNK_SIZE = MAX_CHUNK_SIZES * 1024  # 512KB
FLOOD_WAIT_SLEEP = 5  # Seconds to sleep on flood wait error
SPEED_CHECK_INTERVAL = SPEED_CHECK_INTERVALS  # Seconds between speed checks

filename = ""

log: logging.Logger = logging.getLogger("FastTelethon")

TypeLocation = Union[
    Document,
    InputDocumentFileLocation,
    InputPeerPhotoFileLocation,
    InputFileLocation,
    InputPhotoFileLocation,
]

# Semaphore to limit parallel transfers
parallel_transfer_semaphore = asyncio.Semaphore(MAX_PARALLEL_TRANSFERS)


class DownloadSender:
    client: TelegramClient
    sender: MTProtoSender
    request: GetFileRequest
    remaining: int
    stride: int
    last_chunk_time: float
    last_chunk_size: int
    speed_limit: int

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
        self.sender = sender
        self.client = client
        self.request = GetFileRequest(file, offset=offset, limit=limit)
        self.stride = stride
        self.remaining = count
        self.last_chunk_time = time.time()
        self.last_chunk_size = 0
        self.speed_limit = speed_limit

    async def next(self) -> Optional[bytes]:
        if not self.remaining:
            return None
        
        # Calculate dynamic chunk size based on speed limit
        current_time = time.time()
        time_diff = current_time - self.last_chunk_time
        if time_diff > 0:
            current_speed = self.last_chunk_size / time_diff
            if current_speed > self.speed_limit:
                # If we're exceeding speed limit, wait to throttle
                wait_time = (self.last_chunk_size / self.speed_limit) - time_diff
                if wait_time > 0:
                    await asyncio.sleep(wait_time)
        
        try:
            result = await self.client._call(self.sender, self.request)
            chunk_size = len(result.bytes)
            self.last_chunk_size = chunk_size
            self.last_chunk_time = time.time()
            self.remaining -= 1
            self.request.offset += self.stride
            return result.bytes
        except FloodWaitError as e:
            # Use the wait time provided by the error
            log.warning(f"Flood wait error, sleeping for {e.seconds} seconds")
            await asyncio.sleep(e.seconds)
            return await self.next() # Retry the request after waiting

            #log.warning(f"Flood wait error, sleeping for {FLOOD_WAIT_SLEEP} seconds")
            #await asyncio.sleep(FLOOD_WAIT_SLEEP)
            #return await self.next()

    def disconnect(self) -> Awaitable[None]:
        return self.sender.disconnect()


class UploadSender:
    client: TelegramClient
    sender: MTProtoSender
    request: Union[SaveFilePartRequest, SaveBigFilePartRequest]
    part_count: int
    stride: int
    previous: Optional[asyncio.Task]
    loop: asyncio.AbstractEventLoop
    last_chunk_time: float
    last_chunk_size: int
    speed_limit: int

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
        if big:
            self.request = SaveBigFilePartRequest(file_id, index, part_count, b"")
        else:
            self.request = SaveFilePartRequest(file_id, index, b"")
        self.stride = stride
        self.previous = None
        self.loop = loop
        self.last_chunk_time = time.time()
        self.last_chunk_size = 0
        self.speed_limit = speed_limit

    async def next(self, data: bytes) -> None:
        if self.previous:
            await self.previous
        
        # Calculate dynamic chunk size based on speed limit
        current_time = time.time()
        time_diff = current_time - self.last_chunk_time
        if time_diff > 0:
            current_speed = self.last_chunk_size / time_diff
            if current_speed > self.speed_limit:
                # If we're exceeding speed limit, wait to throttle
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
            #log.warning(f"Flood wait error, sleeping for {FLOOD_WAIT_SLEEP} seconds")
            #await asyncio.sleep(FLOOD_WAIT_SLEEP)
            #await self._next(data)
            # Use the wait time provided by the error
            log.warning(f"Flood wait error, sleeping for {e.seconds} seconds uploading")
            await asyncio.sleep(e.seconds)
            await self._next(data) # Retry the request after waiting

    async def disconnect(self) -> None:
        if self.previous:
            await self.previous
        return await self.sender.disconnect()


class ParallelTransferrer:
    client: TelegramClient
    loop: asyncio.AbstractEventLoop
    dc_id: int
    senders: Optional[List[Union[DownloadSender, UploadSender]]]
    auth_key: AuthKey
    upload_ticker: int
    speed_limit: int
    last_speed_check: float
    bytes_transferred_since_check: int

    def __init__(self, client: TelegramClient, dc_id: Optional[int] = None, speed_limit: int = MAX_DOWNLOAD_SPEED) -> None:
        self.client = client
        self.loop = self.client.loop
        self.dc_id = dc_id or self.client.session.dc_id
        self.auth_key = (
            None
            if dc_id and self.client.session.dc_id != dc_id
            else self.client.session.auth_key
        )
        self.senders = None
        self.upload_ticker = 0
        self.speed_limit = speed_limit
        self.last_speed_check = time.time()
        self.bytes_transferred_since_check = 0

    async def _cleanup(self) -> None:
        await asyncio.gather(*[sender.disconnect() for sender in self.senders])
        self.senders = None

    @staticmethod
    def _get_connection_count(
        file_size: int, max_count: int = MAX_CONNECTIONS_PER_TRANSFER, full_size: int = 100 * 1024 * 1024
    ) -> int:
        if file_size > full_size:
            return min(max_count, MAX_CONNECTIONS_PER_TRANSFER)
        return min(math.ceil((file_size / full_size) * max_count), MAX_CONNECTIONS_PER_TRANSFER)

    async def _init_download(
        self, connections: int, file: TypeLocation, part_count: int, part_size: int
    ) -> None:
        minimum, remainder = divmod(part_count, connections)

        def get_part_count() -> int:
            nonlocal remainder
            if remainder > 0:
                remainder -= 1
                return minimum + 1
            return minimum

        # The first cross-DC sender will export+import the authorization, so we always create it
        # before creating any other senders.
        self.senders = [
            await self._create_download_sender(
                file, 0, part_size, connections * part_size, get_part_count(), self.speed_limit
            ),
            *await asyncio.gather(
                *[
                    self._create_download_sender(
                        file, i, part_size, connections * part_size, get_part_count(), self.speed_limit
                    )
                    for i in range(1, min(connections, MAX_CONNECTIONS_PER_TRANSFER))
                ]
            ),
        ]

    async def _create_download_sender(
        self,
        file: TypeLocation,
        index: int,
        part_size: int,
        stride: int,
        part_count: int,
        speed_limit: int
    ) -> DownloadSender:
        # Adjust chunk size based on speed limit
        adjusted_part_size = max(MIN_CHUNK_SIZE, min(part_size, MAX_CHUNK_SIZE))
        return DownloadSender(
            self.client,
            await self._create_sender(),
            file,
            index * adjusted_part_size,
            adjusted_part_size,
            stride,
            part_count,
            speed_limit
        )

    async def _init_upload(
        self, connections: int, file_id: int, part_count: int, big: bool
    ) -> None:
        self.senders = [
            await self._create_upload_sender(file_id, part_count, big, 0, connections, self.speed_limit),
            *await asyncio.gather(
                *[
                    self._create_upload_sender(file_id, part_count, big, i, connections, self.speed_limit)
                    for i in range(1, min(connections, MAX_CONNECTIONS_PER_TRANSFER))
                ]
            ),
        ]

    async def _create_upload_sender(
        self, file_id: int, part_count: int, big: bool, index: int, stride: int, speed_limit: int
    ) -> UploadSender:
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
            self.client._init_request.query = ImportAuthorizationRequest(
                id=auth.id, bytes=auth.bytes
            )
            req = InvokeWithLayerRequest(LAYER, self.client._init_request)
            await sender.send(req)
            self.auth_key = sender.auth_key
        return sender

    async def init_upload(
        self,
        file_id: int,
        file_size: int,
        part_size_kb: Optional[float] = None,
        connection_count: Optional[int] = None,
    ) -> Tuple[int, int, bool]:
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

    async def download(
        self,
        file: TypeLocation,
        file_size: int,
        part_size_kb: Optional[float] = None,
        connection_count: Optional[int] = None,
    ) -> AsyncGenerator[bytes, None]:
        connection_count = connection_count or self._get_connection_count(file_size)
        part_size = (part_size_kb or utils.get_appropriated_part_size(file_size)) * 1024
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
            
            tasks = []
            for sender in self.senders:
                tasks.append(self.loop.create_task(sender.next()))
            
            for task in tasks:
                data = await task
                if not data:
                    break
                bytes_since_check += len(data)
                yield data
                part += 1
                
        await self._cleanup()


parallel_transfer_locks: DefaultDict[int, asyncio.Lock] = defaultdict(
    lambda: asyncio.Lock()
)


def stream_file(file_to_stream: BinaryIO, chunk_size=1024):
    while True:
        data_read = file_to_stream.read(chunk_size)
        if not data_read:
            break
        yield data_read


async def _internal_transfer_to_telegram(
    client: TelegramClient, response: BinaryIO, progress_callback: callable
) -> Tuple[TypeInputFile, int]:
    file_id = helpers.generate_random_long()
    file_size = os.path.getsize(response.name)

    hash_md5 = hashlib.md5()
    async with parallel_transfer_semaphore:
        uploader = ParallelTransferrer(client, speed_limit=MAX_DOWNLOAD_SPEED)
        part_size, part_count, is_large = await uploader.init_upload(file_id, file_size)
        buffer = bytearray()
        for data in stream_file(response):
            if progress_callback:
                r = progress_callback(response.tell(), file_size)
                if inspect.isawaitable(r):
                    try:
                        await r
                    except BaseException:
                        pass
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
    if is_large:
        return InputFileBig(file_id, part_count, filename), file_size
    else:
        return InputFile(file_id, part_count, filename, hash_md5.hexdigest()), file_size


async def download_file(
    client: TelegramClient,
    location: TypeLocation,
    out: BinaryIO,
    progress_callback: callable = None,
    speed_limit: int = MAX_DOWNLOAD_SPEED
) -> BinaryIO:
    size = location.size
    dc_id, location = utils.get_input_location(location)
    # We lock the transfers because telegram has connection count limits
    async with parallel_transfer_semaphore:
        downloader = ParallelTransferrer(client, dc_id, speed_limit=speed_limit)
        downloaded = downloader.download(location, size)
        async for x in downloaded:
            out.write(x)
            if progress_callback:
                r = progress_callback(out.tell(), size)
                if inspect.isawaitable(r):
                    try:
                        await r
                    except BaseException:
                        pass

    return out


async def upload_file(
    client: TelegramClient,
    file: BinaryIO,
    name,
    progress_callback: callable = None,
    speed_limit: int = MAX_DOWNLOAD_SPEED
) -> TypeInputFile:
    global filename
    filename = name
    return (await _internal_transfer_to_telegram(client, file, progress_callback))[0]
