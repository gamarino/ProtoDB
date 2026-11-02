"""Framed append-only log files (8.3d re-review 10, R2; ruling R58).

Every append-only log of the local tier -- the acceptor log (``roots.log`` and the per-node
``*.roots`` files of a shared storage), ``ranges.log`` and ``segments.log`` -- uses this file
format and opens through :func:`open_log`.

File layout::

    header  LOG_MAGIC (8 bytes) | LOG_FORMAT_VERSION (u16 LE) | kind (4 ASCII bytes)
    frame   length (u32 LE, > 0) | crc32 (u32 LE) | payload (``length`` bytes)

The CRC32 of a frame covers its four length bytes and its payload, so a damaged length is
detected like a damaged payload.  The kind names the log (``b"ROOT"``, ``b"RANG"``,
``b"SEGS"``), so one log is never read as another.

Reading a log (:func:`scan`) walks the frames from the header.  At the first position that
does not hold a valid frame it distinguishes two cases:

* **a torn tail**: no valid frame starts anywhere after that position.  The region is what
  an interrupted append left (a failed ``write``, a ``SIGKILL`` or a power loss before the
  bytes were synced): it is dropped, and every frame before it is valid;
* **mid-log corruption**: a valid frame starts somewhere after it.  The log lost or damaged
  bytes that later entries were written after; nothing silently drops those later entries,
  :class:`~proto_db.errors.ProtoCorruptionError` is raised instead.

A header that is only partly there (a prefix of the expected header, possibly followed by
zero bytes, and nothing else) is a torn tail too: the log is new.  Any other header (a
different magic, kind or a file of another format) raises ``ProtoCorruptionError``, a
different version ``ProtoNotSupportedError``.

Opening a log for appends (:func:`open_log`) truncates a torn tail to the last valid frame
and ``fsync``-s the file before it returns the descriptor, so an append never lands after
torn bytes (where the next reader would stop before it).  This covers the recovery of a
handle poisoned by a failed append (close and reopen) and a crash or power loss in the
middle of an append alike.  A trim is logged at warning level.
"""
import errno
import logging
import os
import struct
import zlib
from typing import NamedTuple

from ..errors import ProtoCorruptionError, ProtoNotSupportedError, attach_secondary_errors

__all__ = ["LOG_MAGIC", "LOG_FORMAT_VERSION", "HEADER_SIZE", "FRAME_OVERHEAD", "ScanResult",
           "encode_header", "encode_frame", "scan", "read_fd", "read_log", "open_log", "log_contents",
           "write_all", "prepare_replacement", "fsync_dir"]

logger = logging.getLogger(__name__)

LOG_MAGIC = b"PDB2LOG\0"
#: Version of the framed log format; bump it when the header or the framing changes.
LOG_FORMAT_VERSION = 1
_HEADER = struct.Struct("<8sH4s")
_FRAME = struct.Struct("<II")
_LENGTH = struct.Struct("<I")
HEADER_SIZE = _HEADER.size
FRAME_OVERHEAD = _FRAME.size
_O_CLOEXEC = getattr(os, "O_CLOEXEC", 0)
_O_BINARY = getattr(os, "O_BINARY", 0)


class ScanResult(NamedTuple):
    """What :func:`scan` found: the ``payloads`` of the valid frames, in order, ``valid_end``
    the offset just past the last valid frame (0 when even the header is missing or torn),
    and ``torn`` True when bytes past ``valid_end`` form a torn tail to drop."""

    payloads: list
    valid_end: int
    torn: bool


def encode_header(kind: bytes) -> bytes:
    """The header of a log of ``kind`` (4 ASCII bytes)."""
    return _HEADER.pack(LOG_MAGIC, LOG_FORMAT_VERSION, kind)


def encode_frame(payload: bytes) -> bytes:
    """Frame ``payload`` (non-empty) for appending to a log."""
    length = _LENGTH.pack(len(payload))
    return length + _LENGTH.pack(zlib.crc32(payload, zlib.crc32(length))) + payload


def _frame_at(data, pos: int, size: int):
    """The payload of a valid frame at ``pos`` of ``data`` (``size`` bytes), or ``None``."""
    if pos + FRAME_OVERHEAD > size:
        return None
    length, crc = _FRAME.unpack_from(data, pos)
    end = pos + FRAME_OVERHEAD + length
    if length == 0 or end > size:
        return None
    payload = data[pos + FRAME_OVERHEAD:end]
    if zlib.crc32(payload, zlib.crc32(data[pos:pos + 4])) != crc:
        return None
    return payload


def _first_valid_frame(data, start: int, size: int):
    """The offset of the first valid frame starting at or after ``start``, or ``None``."""
    for pos in range(start, size - FRAME_OVERHEAD):
        if _frame_at(data, pos, size) is not None:
            return pos
    return None


def scan(data: bytes, kind: bytes, path: str) -> ScanResult:
    """Decode the log ``data`` of ``kind`` read from ``path`` (see the module docstring).

    Raises ``ProtoCorruptionError`` for mid-log corruption or a foreign header and
    ``ProtoNotSupportedError`` for another format version.
    """
    view = memoryview(data)
    size = len(data)
    expected = encode_header(kind)
    if size < HEADER_SIZE or bytes(view[:HEADER_SIZE]) != expected:
        head = bytes(view[:HEADER_SIZE])
        keep = 0
        while keep < len(head) and head[keep] == expected[keep]:
            keep += 1
        if not any(view[keep:]):
            return ScanResult([], 0, size > 0)       # a new log, or its header torn
        magic, version, found = _HEADER.unpack(head) if size >= HEADER_SIZE else (None, None, None)
        if magic == LOG_MAGIC and version != LOG_FORMAT_VERSION:
            raise ProtoNotSupportedError(
                f"The log {path} has format version {version}; this version reads {LOG_FORMAT_VERSION}")
        if magic == LOG_MAGIC and version == LOG_FORMAT_VERSION and found != kind:
            raise ProtoCorruptionError(f"The log {path} is a {found!r} log, not a {kind!r} log")
        raise ProtoCorruptionError(f"The log {path} does not start with a valid {kind!r} log header")
    payloads = []
    pos = HEADER_SIZE
    while pos < size:
        payload = _frame_at(view, pos, size)
        if payload is None:
            later = _first_valid_frame(view, pos + 1, size)
            if later is not None:
                raise ProtoCorruptionError(
                    f"The log {path} is corrupt: the entry at offset {pos} is invalid but a valid entry "
                    f"follows at offset {later} (not a torn tail; nothing was truncated)")
            return ScanResult(payloads, pos, True)
        payloads.append(bytes(payload))
        pos += FRAME_OVERHEAD + len(payload)
    return ScanResult(payloads, pos, False)


def read_fd(fd: int) -> bytes:
    """The whole contents of the file open on ``fd`` (positioned reads)."""
    size = os.fstat(fd).st_size
    parts = []
    offset = 0
    while offset < size:
        chunk = os.pread(fd, size - offset, offset)
        if not chunk:
            break
        parts.append(chunk)
        offset += len(chunk)
    return b"".join(parts)


def read_log(path: str, kind: bytes) -> list:
    """The payloads of the valid frames of the log at ``path`` (``[]`` when it does not exist);
    a torn tail is ignored (not trimmed), mid-log corruption raises ``ProtoCorruptionError``.
    ``OSError`` propagates."""
    try:
        with open(path, "rb") as f:
            data = f.read()
    except FileNotFoundError:
        return []
    return scan(data, kind, path).payloads


def write_all(fd: int, data) -> None:
    """Write every byte of ``data`` to the descriptor ``fd`` (opened with ``O_APPEND``): a
    short write is continued, a write that makes no progress raises ``OSError``."""
    view = memoryview(data)
    while view:
        n = os.write(fd, view)
        if n <= 0:
            raise OSError(errno.EIO, f"write returned {n}")
        view = view[n:]


def fsync_dir(directory: str) -> None:
    """Make the entries of ``directory`` durable (no-op where directories cannot be synced)."""
    if os.name == "nt":  # pragma: no cover - directories cannot be fsynced on Windows
        return
    fd = os.open(directory, os.O_RDONLY | _O_CLOEXEC)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def log_contents(kind: bytes, payloads) -> bytes:
    """The complete contents of a log of ``kind`` holding ``payloads`` (a compaction's file)."""
    return encode_header(kind) + b"".join(encode_frame(p) for p in payloads)


def open_log(path: str, kind: bytes, *, durability: str) -> tuple:
    """Open the log of ``kind`` at ``path`` for appends; return ``(fd, payloads)``.

    The file (and its directory) is created when missing; a new log gets its header.  A torn
    tail is truncated to the last valid frame and the file ``fsync``-ed before the descriptor
    is returned (module docstring), whatever ``durability``; the header of a new log and the
    directory entries of a new file are synced under ``"fsync"`` durability.  Raises
    ``ProtoCorruptionError`` (mid-log corruption, foreign header), ``ProtoNotSupportedError``
    (another format version) or ``OSError``; the descriptor is closed on every failure.
    """
    directory = os.path.dirname(path)
    created_dir = not os.path.isdir(directory)
    os.makedirs(directory, exist_ok=True)
    existed = os.path.exists(path)
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_APPEND | _O_BINARY | _O_CLOEXEC, 0o644)
    try:
        result = scan(read_fd(fd), kind, path)
        if result.torn:
            size = os.fstat(fd).st_size
            logger.warning("The log %s ends in a torn tail of %d bytes (an interrupted append); truncated "
                           "to its last valid entry at offset %d", path, size - result.valid_end,
                           result.valid_end)
            os.ftruncate(fd, result.valid_end)
            os.fsync(fd)
        if result.valid_end == 0:
            write_all(fd, encode_header(kind))
            if durability == "fsync" or result.torn:
                os.fsync(fd)
        if durability == "fsync" and not existed:
            fsync_dir(directory)
            if created_dir:
                fsync_dir(os.path.dirname(directory))
    except BaseException as e:
        try:
            os.close(fd)
        except OSError as close_error:
            attach_secondary_errors(e, [close_error])
        raise
    return fd, result.payloads


def prepare_replacement(path: str, data: bytes, flags: int) -> int:
    """Replace the file ``path`` by one holding ``data`` and return a descriptor open on the
    new file (``flags`` plus ``O_APPEND``), ready for appends.

    ``data`` is written to ``path + ".tmp"`` through the descriptor returned, ``fsync``-ed,
    and only then renamed over ``path``; the descriptor follows the file through the rename.
    On any failure the descriptor is closed and the error raised: ``path`` is either intact
    or replaced by the complete new contents, and the caller's descriptor on the old file is
    untouched.  The caller swaps its descriptor attribute to the one returned before it closes
    the old one (8.3d re-review 9, B1), then makes the directory entry durable."""
    tmp = path + ".tmp"
    fd = os.open(tmp, flags | os.O_CREAT | os.O_TRUNC | os.O_APPEND, 0o644)
    try:
        write_all(fd, data)
        os.fsync(fd)
        os.replace(tmp, path)
    except BaseException as e:
        try:
            os.close(fd)
        except OSError as close_error:
            attach_secondary_errors(e, [close_error])
        raise
    return fd
