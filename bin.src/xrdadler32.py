#!/usr/bin/env python3
"""
Return the size (bytes) and adler32 checksum of a file.

Caching logic (via xattr "user.XrdCks.adler32"):
  1. xattr present AND fmTime matches file mtime  -> use cached checksum
  2. xattr present BUT fmTime doesn't match       -> recompute + update xattr
  3. xattr absent                                 -> compute + write xattr

XrdCksData struct layout (network / big-endian byte order):

    Offset  Size  Field
    ------  ----  -----
     0      16    Name     - checksum algorithm name (null-padded ASCII)
    16       8    fmTime   - file modification time (int64, Unix seconds)
    24       4    bufSize  - length of the checksum value (int32)
    28       2    Flags    - status flags (uint16)
    30       2    Length   - number of valid bytes in csVal (uint16)
    32      32    csVal    - raw checksum bytes, zero-padded to 32 bytes
    64      32    padding  - reserved / zero

Total struct size: 96 bytes.
"""

import os
import struct
import sys
import zlib
from datetime import datetime, timezone
from enum import Enum


# ── constants ──────────────────────────────────────────────────────────────────
XATTR_NAME  = "user.XrdCks.adler32"
ALGO_NAME   = b"adler32"
CS_VAL_SIZE = 32
STRUCT_SIZE = 96
HEADER_FMT  = ">16s q i H H"   # Name(16) fmTime(8) bufSize(4) Flags(2) Length(2)
HEADER_SIZE = struct.calcsize(HEADER_FMT)   # = 32
CHUNK_SIZE  = 1 << 20           # 1 MiB read buffer


# ── checksum ───────────────────────────────────────────────────────────────────

def compute_adler32(path: str) -> int:
    """Stream-compute the adler32 checksum of `path`."""
    checksum = 1
    with open(path, "rb") as fh:
        while chunk := fh.read(CHUNK_SIZE):
            checksum = zlib.adler32(chunk, checksum)
    return checksum & 0xFFFFFFFF


# ── struct packing / unpacking ─────────────────────────────────────────────────

def pack_xrdcks(checksum: int, fm_time: int) -> bytes:
    """Serialise an XrdCksData record for an adler32 checksum (96 bytes)."""
    name_field = ALGO_NAME.ljust(16, b"\x00")
    cs_length  = 4
    header     = struct.pack(HEADER_FMT, name_field, fm_time, cs_length, 0, cs_length)
    cs_field   = struct.pack(">I", checksum).ljust(CS_VAL_SIZE, b"\x00")
    payload    = header + cs_field
    payload   += b"\x00" * (STRUCT_SIZE - len(payload))
    return payload


def unpack_xrdcks(raw: bytes) -> tuple[int, int]:
    """
    Deserialise a raw XrdCksData byte string.

    Returns:
        (checksum, fm_time) where checksum is a 32-bit int and
        fm_time is a Unix timestamp int.

    Raises:
        ValueError if the data is malformed or the algorithm is not adler32.
    """
    if len(raw) < HEADER_SIZE:
        raise ValueError(f"xattr too short: {len(raw)} bytes")

    name_bytes, fm_time, _buf_size, _flags, length = struct.unpack_from(HEADER_FMT, raw, 0)
    name = name_bytes.split(b"\x00", 1)[0].decode("ascii")

    if name != "adler32":
        raise ValueError(f"unexpected algorithm in xattr: '{name}'")
    if length == 0 or HEADER_SIZE + length > len(raw):
        raise ValueError(f"invalid checksum length field: {length}")

    cs_bytes = raw[HEADER_SIZE : HEADER_SIZE + length]
    checksum = int.from_bytes(cs_bytes, "big")
    return checksum, fm_time


# ── xattr I/O ──────────────────────────────────────────────────────────────────

def read_xattr(path: str) -> bytes | None:
    """Return raw xattr bytes, or None if the attribute doesn't exist."""
    try:
        return os.getxattr(path, XATTR_NAME)
    except OSError as exc:
        if exc.errno in (61, 95, 2):  # ENODATA / EOPNOTSUPP / ENOENT
            return None
        raise


def write_xattr(path: str, raw: bytes) -> None:
    """Write raw bytes to the xattr, with a helpful error for unsupported fs."""
    try:
        os.setxattr(path, XATTR_NAME, raw)
    except OSError as exc:
        pass


# ── main logic ─────────────────────────────────────────────────────────────────

def xrd_get_size_and_adler32(path: str) -> tuple[int, int]:
    """
    Return (size_bytes, adler32_checksum(hex)) for `path`.

    Reads from the xattr cache when possible; recomputes and updates
    the cache when the file has been modified or the xattr is absent.
    """
    stat    = os.stat(path)
    size    = stat.st_size
    mtime   = int(stat.st_mtime)

    # ── try to read cached value ───────────────────────────────────────────────
    raw = read_xattr(path)
    if raw is not None:
        try:
            checksum, fm_time = unpack_xrdcks(raw)
            if fm_time == mtime:
                return size, f'{checksum:08x}'
        except ValueError:
           pass   # treat corrupt xattr as missing

    # ── (re)compute and cache ──────────────────────────────────────────────────
    checksum = compute_adler32(path)
    write_xattr(path, pack_xrdcks(checksum, mtime))
    return size, f'{checksum:08x}'


# ── CLI ────────────────────────────────────────────────────────────────────────

def main() -> None:
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <file>", file=sys.stderr)
        sys.exit(1)

    path = sys.argv[1]

    if not os.path.isfile(path):
        print(f"Error: '{path}' is not a regular file", file=sys.stderr)
        sys.exit(1)

    size, checksum = xrd_get_size_and_adler32(path)

    print(f'{checksum} {path}')


if __name__ == "__main__":
    main()
