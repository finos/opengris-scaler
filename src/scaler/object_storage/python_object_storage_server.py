"""
Python-based Object Storage Server.

This server implements the same protocol as the C++ ObjectStorageServer,
but allows pluggable backends (memory, Redis, etc.).

Use this when you need Redis backend support. For maximum performance
with in-memory storage, use the default C++ server.
"""

import asyncio
import logging
import signal
import struct
import sys
from typing import Dict, List, Optional, Tuple

from scaler.object_storage.backend import ObjectStorageBackend
from scaler.object_storage.memory_backend import MemoryObjectStorageBackend
from scaler.protocol.capnp._python import _object_storage
from scaler.protocol.python.object_storage import (
    ObjectRequestHeader,
    ObjectResponseHeader,
    from_capnp_object_id,
    to_capnp_object_id,
)
from scaler.utility.identifiers import ObjectID

logger = logging.getLogger(__name__)


class PythonObjectStorageServer:
    """
    Python implementation of the Scaler Object Storage Server.

    This server uses asyncio for concurrency and supports pluggable
    storage backends via the ObjectStorageBackend interface.
    """

    def __init__(self, backend: Optional[ObjectStorageBackend] = None):
        """
        Initialize the server with a storage backend.

        Args:
            backend: Storage backend to use. Defaults to MemoryObjectStorageBackend.
        """
        self._backend = backend or MemoryObjectStorageBackend()
        self._server: Optional[asyncio.Server] = None
        self._shutdown_event = asyncio.Event()

        # For cross-process ready signaling (like C++ server uses pipe fds)
        self._ready_read_fd: Optional[int] = None
        self._ready_write_fd: Optional[int] = None

        # Track pending GET requests (waiting for objects that don't exist yet)
        # Maps object_id -> list of (writer, request_header) tuples
        self._pending_gets: Dict[bytes, List[Tuple[asyncio.StreamWriter, ObjectRequestHeader]]] = {}

        # Lock for thread-safe access to pending gets
        self._pending_lock = asyncio.Lock()

    def _init_ready_fds(self):
        """Initialize pipe for ready signaling."""
        import os

        self._ready_read_fd, self._ready_write_fd = os.pipe()

    def _set_ready(self):
        """Signal that server is ready."""
        import os

        if self._ready_write_fd is not None:
            os.write(self._ready_write_fd, b"1")
            os.close(self._ready_write_fd)
            self._ready_write_fd = None

    def _close_ready_fds(self):
        """Close ready signaling fds."""
        import os

        if self._ready_read_fd is not None:
            try:
                os.close(self._ready_read_fd)
            except OSError:
                pass
            self._ready_read_fd = None
        if self._ready_write_fd is not None:
            try:
                os.close(self._ready_write_fd)
            except OSError:
                pass
            self._ready_write_fd = None

    def wait_until_ready(self) -> None:
        """Block until the server is ready to accept connections."""
        import os
        import select

        if self._ready_read_fd is None:
            # Not using multiprocessing, just return
            return

        # Wait for ready signal with timeout
        try:
            readable, _, _ = select.select([self._ready_read_fd], [], [], 30.0)
            if readable:
                os.read(self._ready_read_fd, 1)
        except (OSError, ValueError):
            pass
        finally:
            try:
                os.close(self._ready_read_fd)
            except OSError:
                pass
            self._ready_read_fd = None

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Handle a single client connection."""
        peer = writer.get_extra_info("peername")
        logger.debug(f"New client connection from {peer}")

        try:
            # Exchange identities (YMQ-style framing)
            # Server sends identity FIRST, then reads client identity
            server_identity = b"PythonObjectStorageServer"
            await self._write_framed_message(writer, server_identity)

            # Read client identity
            client_identity = await self._read_framed_message(reader)
            logger.debug(f"Client identity: {client_identity[:50]}...")

            # Process requests
            while not self._shutdown_event.is_set():
                try:
                    request = await self._read_request(reader)
                    if request is None:
                        break

                    header, payload = request
                    await self._process_request(writer, header, payload)

                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Error processing request from {peer}: {e}")
                    break

        except Exception as e:
            logger.debug(f"Client {peer} disconnected: {e}")
        finally:
            # Clean up pending requests for this client
            await self._cleanup_client_pending_requests(writer)
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    async def _cleanup_client_pending_requests(self, writer: asyncio.StreamWriter):
        """Remove pending requests for a disconnected client."""
        async with self._pending_lock:
            for object_id in list(self._pending_gets.keys()):
                self._pending_gets[object_id] = [(w, h) for w, h in self._pending_gets[object_id] if w != writer]
                if not self._pending_gets[object_id]:
                    del self._pending_gets[object_id]

    async def _read_framed_message(self, reader: asyncio.StreamReader) -> bytes:
        """Read a length-prefixed message."""
        length_bytes = await reader.readexactly(8)
        (length,) = struct.unpack("<Q", length_bytes)
        if length == 0:
            return b""
        return await reader.readexactly(length)

    async def _write_framed_message(self, writer: asyncio.StreamWriter, data: bytes):
        """Write a length-prefixed message."""
        writer.write(struct.pack("<Q", len(data)))
        writer.write(data)
        await writer.drain()

    async def _read_request(self, reader: asyncio.StreamReader) -> Optional[Tuple[ObjectRequestHeader, bytes]]:
        """Read a request (header + optional payload)."""
        try:
            # Read header
            header_bytes = await self._read_framed_message(reader)
            if not header_bytes:
                return None

            with _object_storage.ObjectRequestHeader.from_bytes(bytes(header_bytes)) as msg:
                header = ObjectRequestHeader(msg)

            # Read payload if present (for SET and DUPLICATE requests)
            payload = b""
            request_type = header.request_type
            if request_type in (
                ObjectRequestHeader.ObjectRequestType.SetObject,
                ObjectRequestHeader.ObjectRequestType.DuplicateObjectID,
            ):
                payload = await self._read_framed_message(reader)

            return header, payload

        except asyncio.IncompleteReadError:
            return None

    async def _write_response(
        self,
        writer: asyncio.StreamWriter,
        object_id: ObjectID,
        response_id: int,
        response_type: ObjectResponseHeader.ObjectResponseType,
        payload: bytes = b"",
    ):
        """Write a response (header + optional payload)."""
        header = ObjectResponseHeader.new_msg(object_id, len(payload), response_id, response_type)
        header_bytes = header.get_message().to_bytes()

        await self._write_framed_message(writer, header_bytes)

        if payload:
            await self._write_framed_message(writer, payload)

    async def _process_request(self, writer: asyncio.StreamWriter, header: ObjectRequestHeader, payload: bytes):
        """Process a single request."""
        request_type = header.request_type
        object_id = header.object_id

        if request_type == ObjectRequestHeader.ObjectRequestType.SetObject:
            await self._handle_set(writer, header, payload)
        elif request_type == ObjectRequestHeader.ObjectRequestType.GetObject:
            await self._handle_get(writer, header)
        elif request_type == ObjectRequestHeader.ObjectRequestType.DeleteObject:
            await self._handle_delete(writer, header)
        elif request_type == ObjectRequestHeader.ObjectRequestType.DuplicateObjectID:
            await self._handle_duplicate(writer, header, payload)
        else:
            logger.warning(f"Unknown request type: {request_type}")

    async def _handle_set(self, writer: asyncio.StreamWriter, header: ObjectRequestHeader, payload: bytes):
        """Handle SET request."""
        object_id = header.object_id
        object_id_bytes = self._object_id_to_bytes(object_id)

        # Store in backend (metadata is empty for now - Scaler doesn't use it)
        self._backend.put(object_id_bytes, payload, b"")

        # Send response
        await self._write_response(writer, object_id, header.request_id, ObjectResponseHeader.ObjectResponseType.SetOK)

        # Check if there are pending GET requests for this object
        await self._fulfill_pending_gets(object_id, object_id_bytes, payload)

    async def _fulfill_pending_gets(self, object_id: ObjectID, object_id_bytes: bytes, payload: bytes):
        """Send responses to clients waiting for this object."""
        async with self._pending_lock:
            pending = self._pending_gets.pop(object_id_bytes, [])

        for pending_writer, pending_header in pending:
            try:
                # Respect max payload length
                max_len = pending_header.payload_length
                response_payload = payload[:max_len] if max_len < len(payload) else payload

                await self._write_response(
                    pending_writer,
                    object_id,
                    pending_header.request_id,
                    ObjectResponseHeader.ObjectResponseType.GetOK,
                    response_payload,
                )
            except Exception as e:
                logger.debug(f"Failed to send pending GET response: {e}")

    async def _handle_get(self, writer: asyncio.StreamWriter, header: ObjectRequestHeader):
        """Handle GET request."""
        object_id = header.object_id
        object_id_bytes = self._object_id_to_bytes(object_id)

        result = self._backend.get(object_id_bytes)

        if result is not None:
            data, _ = result
            # Respect max payload length
            max_len = header.payload_length
            response_payload = data[:max_len] if max_len < len(data) else data

            await self._write_response(
                writer, object_id, header.request_id, ObjectResponseHeader.ObjectResponseType.GetOK, response_payload
            )
        else:
            # Object doesn't exist yet - queue the request
            async with self._pending_lock:
                if object_id_bytes not in self._pending_gets:
                    self._pending_gets[object_id_bytes] = []
                self._pending_gets[object_id_bytes].append((writer, header))
            logger.debug(f"Queued GET request for object {object_id_bytes.hex()[:16]}...")

    async def _handle_delete(self, writer: asyncio.StreamWriter, header: ObjectRequestHeader):
        """Handle DELETE request."""
        object_id = header.object_id
        object_id_bytes = self._object_id_to_bytes(object_id)

        existed = self._backend.delete(object_id_bytes)

        response_type = (
            ObjectResponseHeader.ObjectResponseType.DelOK
            if existed
            else ObjectResponseHeader.ObjectResponseType.DelNotExists
        )

        await self._write_response(writer, object_id, header.request_id, response_type)

    async def _handle_duplicate(self, writer: asyncio.StreamWriter, header: ObjectRequestHeader, payload: bytes):
        """Handle DUPLICATE request (link new object ID to existing object's content)."""
        new_object_id = header.object_id
        new_object_id_bytes = self._object_id_to_bytes(new_object_id)

        # Parse original object ID from payload
        with _object_storage.ObjectID.from_bytes(bytes(payload)) as msg:
            original_object_id = from_capnp_object_id(msg)
        original_object_id_bytes = self._object_id_to_bytes(original_object_id)

        result = self._backend.get(original_object_id_bytes)

        if result is not None:
            data, metadata = result
            # Create copy with new ID
            self._backend.put(new_object_id_bytes, data, metadata)

            await self._write_response(
                writer, new_object_id, header.request_id, ObjectResponseHeader.ObjectResponseType.DuplicateOK
            )
        else:
            # Original doesn't exist yet - queue the request
            async with self._pending_lock:
                if original_object_id_bytes not in self._pending_gets:
                    self._pending_gets[original_object_id_bytes] = []
                # Store as a special duplicate request
                self._pending_gets[original_object_id_bytes].append(
                    (writer, header)  # We'll need special handling for this
                )
            logger.debug(f"Queued DUPLICATE request for object {original_object_id_bytes.hex()[:16]}...")

    @staticmethod
    def _object_id_to_bytes(object_id: ObjectID) -> bytes:
        """Convert ObjectID to bytes for backend storage.

        ObjectID is already a 32-byte bytes subclass, so just return it directly.
        """
        return bytes(object_id)

    async def _run_server(self, host: str, port: int):
        """Main server loop."""
        self._server = await asyncio.start_server(self._handle_client, host, port)

        addr = self._server.sockets[0].getsockname()
        logger.info(f"PythonObjectStorageServer listening on {addr[0]}:{addr[1]}")

        # Signal ready via pipe for cross-process signaling
        self._set_ready()

        async with self._server:
            await self._server.serve_forever()

    def run(
        self,
        host: str,
        port: int,
        identity: str = "PythonObjectStorageServer",
        log_level: str = "INFO",
        log_format: str = "%(levelname)s: %(message)s",
        log_paths: Tuple[str, ...] = ("/dev/stdout",),
        multiprocessing_ready: bool = False,
    ):
        """
        Run the object storage server.

        This method blocks until the server is shut down.

        Args:
            host: Host address to bind to
            port: Port to listen on
            identity: Server identity string
            log_level: Logging level
            log_format: Logging format string
            log_paths: Paths to log to
            multiprocessing_ready: If True, use pipe-based ready signaling for multiprocessing
        """
        # Initialize ready signaling for multiprocessing
        if multiprocessing_ready:
            self._init_ready_fds()

        # Set up signal handlers
        def handle_signal(signum, frame):
            logger.info(f"Received signal {signum}, shutting down...")
            self._shutdown_event.set()
            if self._server:
                self._server.close()

        signal.signal(signal.SIGTERM, handle_signal)
        signal.signal(signal.SIGINT, handle_signal)

        # Run the async server
        try:
            asyncio.run(self._run_server(host, port))
        except KeyboardInterrupt:
            pass
        finally:
            self._backend.close()
            self._close_ready_fds()
            logger.info("PythonObjectStorageServer stopped")


def create_backend(backend_type: str, redis_config: Optional[dict] = None) -> ObjectStorageBackend:
    """
    Create a storage backend based on configuration.

    Args:
        backend_type: "memory" or "redis"
        redis_config: Configuration dict for Redis backend (url, max_object_size_mb, key_prefix, connection_pool_size)

    Returns:
        ObjectStorageBackend instance
    """
    if backend_type == "memory":
        return MemoryObjectStorageBackend()
    elif backend_type == "redis":
        try:
            from scaler.object_storage.redis_backend import RedisObjectStorageBackend
        except ImportError:
            raise ImportError(
                "Redis backend requires the 'redis' package. " "Install with: pip install opengris-scaler[redis]"
            )

        config = redis_config or {}
        return RedisObjectStorageBackend(
            redis_url=config.get("url", "redis://localhost:6379/0"),
            max_object_size_mb=config.get("max_object_size_mb", 100),
            key_prefix=config.get("key_prefix", "scaler:obj:"),
            connection_pool_size=config.get("connection_pool_size", 10),
        )
    else:
        raise ValueError(f"Unknown backend type: {backend_type}. Use 'memory' or 'redis'.")
