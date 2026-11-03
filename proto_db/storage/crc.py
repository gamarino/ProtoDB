"""CRC-32 arithmetic for the linear torn-tail searches (8.3d re-reviews 12 and 13; rulings R60, R61).

The own-segment record search (:func:`proto_db.storage.tiered._first_valid_record`) and the
log frame search (:func:`proto_db.storage.logfile._first_valid_frame`) must check the payload
CRC of every candidate whose self-validating header is valid and whose extent fits.  Computing
that CRC directly costs the whole claimed payload, so crafted header-valid candidates make a
direct search quadratic.  Both searches instead derive the CRC of a long payload from CRCs of
the searched region up to the payload's start and up to its end, kept at checkpoints
(:class:`PrefixCrc`), combined with the GF(2) arithmetic of zlib's ``crc32_combine``
(:func:`crc_shift`): ``crc32(A + B) == crc_shift(crc32(A), len(B)) ^ crc32(B)``.  The result
is exact, and each candidate costs at most one checkpoint interval per prefix lookup.
"""
import threading
import zlib

__all__ = ["crc_shift", "PrefixCrc"]

_CRC_POLY = 0xEDB88320                          # CRC-32 polynomial, reflected (zlib)


def _crc_multmodp(a: int, b: int) -> int:
    """``a * b`` modulo the CRC-32 polynomial (reflected bit order), as zlib's ``multmodp``."""
    m = 1 << 31
    p = 0
    while True:
        if a & m:
            p ^= b
            if (a & (m - 1)) == 0:
                break
        m >>= 1
        b = (b >> 1) ^ _CRC_POLY if b & 1 else b >> 1
    return p


def _crc_x2n_table() -> list:
    table = [1 << 30]                            # x^1
    for _ in range(31):
        table.append(_crc_multmodp(table[-1], table[-1]))
    return table


_CRC_X2N = _crc_x2n_table()


def _crc_constant_tables(constant: int) -> tuple:
    """Four 256-entry tables giving ``constant * v`` (modulo the polynomial) for a 32-bit ``v``
    as the XOR of one entry per byte of ``v`` (multiplication is linear over GF(2))."""
    tables = []
    for j in range(4):
        table = [0] * 256
        for bit in range(8):
            table[1 << bit] = _crc_multmodp(constant, 1 << (8 * j + bit))
        for v in range(3, 256):
            low = v & -v
            if v != low:
                table[v] = table[v ^ low] ^ table[low]
        tables.append(tuple(table))
    return tuple(tables)


_CRC_SHIFT_TABLES = {}                           # k -> tables multiplying by x^(2^k)
_CRC_SHIFT_LOCK = threading.Lock()


def crc_shift(crc: int, nbytes: int) -> int:
    """``crc * x^(8 * nbytes)`` modulo the polynomial: the term ``crc32_combine`` applies to the
    CRC of a prefix followed by ``nbytes`` more bytes (as zlib's ``x2nmodp`` and ``multmodp``,
    one table-driven multiplication by ``x^(2^k)`` per set bit ``k`` of ``8 * nbytes``)."""
    p = crc
    k = 3
    n = nbytes
    while n:
        if n & 1:
            tables = _CRC_SHIFT_TABLES.get(k)
            if tables is None:
                with _CRC_SHIFT_LOCK:
                    tables = _CRC_SHIFT_TABLES.get(k)
                    if tables is None:
                        tables = _CRC_SHIFT_TABLES[k] = _crc_constant_tables(_CRC_X2N[k & 31])
            t0, t1, t2, t3 = tables
            p = t0[p & 0xFF] ^ t1[(p >> 8) & 0xFF] ^ t2[(p >> 16) & 0xFF] ^ t3[p >> 24]
        n >>= 1
        k += 1
    return p


class PrefixCrc:
    """CRC32 of ``data[base:offset]`` for any offset, from checkpoints every ``interval`` bytes
    computed on demand (each lookup costs at most one interval beyond the checkpoints already
    computed; all checkpoints together cost one pass over the bytes they cover).  An instance
    is used by one search on one thread."""

    def __init__(self, data, base: int, interval: int):
        self._data = data
        self._base = base
        self._interval = interval
        self._points = [0]                       # CRC of data[base:base + i * interval]

    def at(self, offset: int) -> int:
        """The CRC32 of ``data[base:offset]``."""
        interval = self._interval
        index = (offset - self._base) // interval
        points = self._points
        while len(points) <= index:
            start = self._base + (len(points) - 1) * interval
            points.append(zlib.crc32(self._data[start:start + interval], points[-1]))
        start = self._base + index * interval
        return zlib.crc32(self._data[start:offset], points[index])

    def payload_crc(self, payload: int, end: int) -> int:
        """The CRC32 of ``data[payload:end]`` (``base <= payload <= end``), from two prefix
        lookups (``crc32_combine``: ``crc(end) == crc_shift(crc(payload), end - payload) ^
        crc32(data[payload:end])``, ``crc`` being the CRC from ``base``)."""
        return self.at(end) ^ crc_shift(self.at(payload), end - payload)
