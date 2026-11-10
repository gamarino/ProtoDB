Storage format specification
============================

This page specifies the bytes ProtoDB writes: the storage files, the atom records and the
canonical values inside them, the logs of a cluster node and the messages cluster nodes
exchange. It is written for an implementer in another language (protoBaseST, in Smalltalk,
first of all) who must read and write the same database without reading the Python code.

The on-disk database is polyglot (ruling R83a). An implementation conforms when a database
written by either implementation, cluster logs included, is read correctly by the other. This
specification and the conformance vectors in ``tests/conformance/`` are that contract
(decision D57). The vectors are produced by the library's own encoders and checked byte for
byte by ``tests/conformance/test_conformance.py``; ``tests/conformance/README.md`` explains how
another implementation uses them.

**The code is the authority.** Every statement here describes what the code of this revision
does. Where the design document (``docs/superpowers/specs/2026-11-01-protodb-v2-design.md``)
says otherwise, `Differences from the design document`_ lists the difference.

.. contents::
   :local:
   :depth: 2


Status, versions and conformance
--------------------------------

The key words MUST, MUST NOT, SHOULD and MAY are used as in RFC 2119. A section is
**normative** unless it is marked *implementation-specific*.

.. list-table:: Format numbers of this revision
   :header-rows: 1
   :widths: 30 10 60

   * - Number
     - Value
     - Where it is written
   * - Storage format (``FORMAT_VERSION``)
     - 3
     - every segment header and every root record; versions 2 and 3 are read
   * - Framed log format (``LOG_FORMAT_VERSION``)
     - 2
     - the header of every framed log file
   * - Cluster protocol (``PROTOCOL_VERSION``)
     - 13
     - the ``v`` field of every cluster message; no other version is accepted

There are three levels of conformance:

- **Reader**: decodes every structure below, refuses every invalid input the specification
  says is refused, and decodes the vectors to their manifest.
- **Writer**: also produces structures that a reader of this revision accepts. Writers SHOULD
  produce the canonical bytes (the vectors' bytes), so that two implementations writing the
  same content write the same bytes; the only freedom left is the order of the attributes in
  an atom's state map (`Atom records`_).
- **Cluster member**: also speaks the cluster wire protocol and keeps the acceptor log. The
  behaviour of the coordination protocol (leases, permissions, acceptance rules) is not part
  of this page; it is specified in the coordinator documentation and decisions D44 to D55.
  This page gives every byte layout that behaviour uses.


Conventions
-----------

**Byte order.** Every fixed-size integer of the storage files and logs is **little-endian**.
The only big-endian integer is the 4-byte length prefix of a cluster wire frame.

**CRC32.** Every checksum is the CRC-32 of zlib (ISO-HDLC: reflected polynomial ``0xEDB88320``,
initial value ``0xFFFFFFFF``, final XOR ``0xFFFFFFFF``), written as a ``u32`` little-endian.
The CRC-32 of the ASCII bytes ``123456789`` is ``0xCBF43926``.

**UUIDs.** A UUID is written as its 16 bytes in RFC 4122 order (Python ``uuid.UUID.bytes``).
Where a UUID names a file or an object it is written as 32 lowercase hexadecimal digits with
no dashes (``uuid.hex``).

**msgpack.** Every structured payload is MessagePack (https://msgpack.org) with the *bin*
types enabled: a byte string is ``bin 8/16/32`` and a text string is ``fixstr``/``str 8/16/32``
holding UTF-8. Writers use the smallest encoding of every item, as msgpack-python 1.x does:

- a non-negative integer as the smallest of positive fixint, ``uint 8/16/32/64`` (so
  ``2**63 - 1`` is ``0xcf`` followed by 8 bytes); a negative integer as the smallest of negative
  fixint, ``int 8/16/32/64``;
- a float always as ``float 64`` (``0xcb``), never ``float 32``;
- ``fixarray``/``array 16/32`` and ``fixmap``/``map 16/32``;
- ``nil``, ``true`` and ``false``.

The extension types (``ext``, ``fixext``, the timestamp extension) are never written, and a
reader of this revision refuses them where a canonical value is expected. Readers MUST accept
any valid encoding of an integer (a positive ``int 64``, say); byte equality with the vectors
is only expected from writers. Every msgpack map key in the structures below is a string
(readers decode with ``strict_map_key``); maps keyed by other values are canonical ``@map``
values.

**Text.** Strings are sequences of Unicode scalar values encoded in UTF-8. A string holding a
lone surrogate cannot be written (the reference implementation fails with
``UnicodeEncodeError`` when packing; see `Known gaps`_).


Storage layouts
---------------

Three storages share the segment, record and root formats.

File storage (one directory, shared by processes)
    ::

        space.root        the published root record, replaced atomically
        space.root.tmp    scratch file of a publish in progress; ignored by readers
        space.lock        lock file of the root lock (no data)
        <uuid-hex>.seg    segments: a header, then records

    A process writes only segments it created (opened with ``O_CREAT | O_EXCL`` under a fresh
    UUID4 name) and never appends to another process's segment. To publish a root, a writer
    takes an exclusive ``flock`` on ``space.lock`` (non-blocking attempts until a timeout;
    ``msvcrt.locking`` of the first byte on Windows), reads ``space.root``, checks that its
    ``epoch`` is the one its transaction started from, writes the new root to
    ``space.root.tmp``, ``fsync``-s it, renames it over ``space.root`` and ``fsync``-s the
    directory. The new root's ``epoch`` is the old one plus one (1 for the first root). Every
    record a root references is flushed (and, in the default durability, ``fsync``-ed) before
    the root is written. An implementation that shares a directory with ProtoBase processes
    MUST use the same lock and the same protocol.

Cloud storage (an object store)
    ::

        <prefix>space.root                  the published root object, compare-and-swap
        <prefix>segments/<uuid-hex>.seg     immutable segment objects

    A segment object is uploaded once, whole, and never modified. The root object is written
    with a conditional write on the etag read (``put_if_absent`` for the first root).

Tiered storage (one cluster node)
    The local directory of a node::

        <uuid-hex>.seg     own segments, and copies of other nodes' segments (records at their
                           original offsets; a copy may have holes)
        segments.log       the node's own segments in creation order (log kind SEGS)
        roots.log          the acceptor log (log kind ROOT)
        ranges.log         byte ranges shipped with each accepted commit (log kind RANG)
        tier.lock          held by the live instance (no data)
        roots.log.lock     held by the open acceptor log (no data)

    and in the object store::

        <prefix>space.root                         the persisted root object, with its term
        <prefix>segments/<uuid-hex>.seg            segment objects, sealed by their writer
        <prefix>segments/<uuid-hex>.<len>.sealed   empty marker: the sealed object's length in
                                                   bytes, 16 lowercase hexadecimal digits
        <prefix>segments/<uuid-hex>.<end>.part     the first <end> bytes (16 hexadecimal
                                                   digits) of a segment, uploaded from a copy

    A cluster over a shared file storage keeps each node's acceptor log in
    ``<directory>/cluster/<node_id>.roots`` (log kind ROOT), with ``<node_id>.roots.lock`` beside it.

The lock files carry no data: they exist to be locked (``flock``, exclusive, non-blocking).
Implementation-specific: the segment size limit (256 MiB by default for a file storage), when
segments are sealed and shipped, caches and buffers.


Segments and records
--------------------

Segment header (26 bytes)
~~~~~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 15 15 70

   * - Offset
     - Size
     - Content
   * - 0
     - 8
     - magic ``PDB2SEG\0`` (``50 44 42 32 53 45 47 00``)
   * - 8
     - 2
     - format version, ``u16``: 3 (writers); 2 or 3 accepted by readers
   * - 10
     - 16
     - the segment's UUID; it MUST equal the UUID in the file or object name

Another magic is ``ProtoCorruptionError``; another version is ``ProtoNotSupportedError``. The
first record starts at offset 26, so the smallest valid atom pointer offset is 26.

Record (15-byte header, then the payload)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 15 15 70

   * - Offset
     - Size
     - Content
   * - 0
     - 2
     - magic ``C1 D2`` (``0xC1`` is the one byte msgpack never uses as a type byte)
   * - 2
     - 1
     - kind: ``1`` ATOM (a msgpack atom payload), ``2`` BLOB (raw bytes)
   * - 3
     - 4
     - payload length, ``u32`` (at most ``2**32 - 1``)
   * - 7
     - 4
     - CRC32 of the payload
   * - 11
     - 4
     - CRC32 of the 11 bytes before it (magic, kind, length, payload CRC)
   * - 15
     - length
     - the payload

The header is self-validating: a reader trusts a record's length only when the header CRC
matches and the kind is 1 or 2, without reading the payload. A record is valid when, in
addition, the payload is complete and its CRC matches. An invalid header, a truncated
payload or a payload CRC mismatch is ``ProtoCorruptionError``.

Records follow one another with no padding. An **atom pointer** is the pair (segment UUID,
offset of the record's first header byte). Records are immutable: a pointer names one
content forever.

The BLOB kind is reserved for raw payloads; this revision defines it and reads it, but no
built-in structure writes one.

A file storage never truncates a segment (another process may be writing it); a reader
simply never follows a pointer into torn bytes, because no published root references them. A
tiered storage owns its directory and truncates a torn tail of its own segments when it opens
(implementation-specific repair; the rule matches the log rule below).

Root records
~~~~~~~~~~~~

``space.root`` holds ``crc32 (u32) | msgpack body``, where the CRC covers the body and the
body is the map, in this key order::

    {"version": 3, "segment": <16 bytes>, "offset": <int >= 0>, "epoch": <int >= 0>}

``segment`` and ``offset`` point at the record of the space's :ref:`SpaceRoot <spaceroot>` atom;
``epoch`` counts the publications (the compare-and-swap token). A reader checks the CRC and the
field types (``ProtoCorruptionError`` otherwise) and accepts ``version`` 2 or 3, reading either
as the current version (the shape did not change). Extra keys are ignored. A missing
``space.root`` file means the space is empty.

The cloud storage's root object adds ``"recent"``: an array of ``[segment 16 bytes, offset,
epoch]`` for up to ``root_history`` previously published roots, newest first. The tiered
storage's persisted root object adds ``"recent": []`` and ``"term"``: the label of the lease that
committed the root (0 when absent). Both are read by the same decoder, which ignores the extra
keys.

Example (``framing/space.root`` of the vectors: segment ``0123456789abcdef0123456789abcdef``,
offset 26, epoch 7)::

    7efee88c 84 a7"version" 03 a7"segment" c410 0123...cdef a6"offset" 1a a5"epoch" 07


Atom records
------------

The payload of an ATOM record is a msgpack map with exactly three keys, written in this
order::

    {"t": <type tag>, "oid": <16 bytes>, "s": <state map>}

``t``
    The **type tag**: an integer alias of a built-in type (the table below) or the type name as
    a string. A reader maps the tag to a type name; an unknown alias or a non-integer,
    non-string tag is ``ProtoCorruptionError``, and so is a name no class is registered for.

``oid``
    The atom's identity: 16 bytes (the reference implementation draws a UUID4). It is persisted
    and survives every load; two atoms with equal state are still different atoms.

``s``
    The **state map**: attribute name (a string) to the attribute's canonical value
    (`Canonical values`_). Only public attributes are persisted. The order of the attributes is
    not significant: readers MUST accept any order, and the reference implementation writes
    the order in which the object's attributes were set (which differs, for example, between
    an empty collection and a later version of it). Writers that want byte equality with
    ProtoBase reproduce the order shown by the vectors.

A payload with other keys, or whose ``t`` names another type than the one the reference was
made with, is ``ProtoCorruptionError``.

Type names (D57)
~~~~~~~~~~~~~~~~

A type name is language-neutral: two or more ASCII identifiers (``[A-Za-z_][A-Za-z0-9_]*``)
joined by dots, such as ``acme.sales.Order``. It names a type of the format, never a module of
an implementation; each implementation maps names to its own classes. Built-in types use the
prefix ``proto_db.``; applications SHOULD use a prefix of their own.

- In ProtoBase a class declares its name as ``__type_name__`` in its own body. A class that
  does not declare one is registered under its Python ``module.QualifiedName`` for reading
  data written before D57 only; saving an instance raises ``ProtoValidationError``.
- A class lists in ``__type_aliases__`` the names it was persisted under before. An alias reads
  as the class, and references spelled with either name reach the same atom. New data is
  always written under the declared name.
- A persisted name is never renamed without an alias, and built-in names are never renamed:
  code older than a rename cannot read the new name.

A reader MUST keep accepting every name it ever accepted. Data written before D57 by
ProtoBase may contain Python-default names of application classes (``myapp.models.Person``);
an implementation that needs to read such data maps those names like any other.

Built-in type aliases
~~~~~~~~~~~~~~~~~~~~~

The alias of a built-in type is its index in this table. The table is **append-only**: an
alias, once written, names the same type forever (D51). A built-in type is always written as
its alias; readers also accept its name (format 2 wrote names).

.. list-table::
   :header-rows: 1
   :widths: 8 50 42

   * - Alias
     - Type name
     - Persisted?
   * - 0
     - ``proto_db.avl.KeyNode``
     - yes
   * - 1
     - ``proto_db.avl.PosNode``
     - yes
   * - 2
     - ``proto_db.avl.ValueBox``
     - yes
   * - 3
     - ``proto_db.collections.IndexSet``
     - yes
   * - 4
     - ``proto_db.collections.IndexedCollection``
     - no (abstract)
   * - 5
     - ``proto_db.collections.List``
     - yes
   * - 6
     - ``proto_db.collections.Dictionary``
     - yes
   * - 7
     - ``proto_db.collections.RepeatedKeysDictionary``
     - yes
   * - 8
     - ``proto_db.collections.HashDictionary``
     - yes
   * - 9
     - ``proto_db.collections.sets._HashBuckets``
     - no (abstract)
   * - 10
     - ``proto_db.collections.Set``
     - yes
   * - 11
     - ``proto_db.collections.CountedSet``
     - yes
   * - 12
     - ``proto_db.indexing.Index``
     - no (abstract)
   * - 13
     - ``proto_db.indexing.keyed.KeyGroupIndex``
     - no (abstract)
   * - 14
     - ``proto_db.indexing.EqualityIndex``
     - yes
   * - 15
     - ``proto_db.indexing.RangeIndex``
     - yes
   * - 16
     - ``proto_db.indexing.VectorIndex``
     - yes
   * - 17
     - ``proto_db.transactions.SpaceRoot``
     - yes
   * - 18
     - ``proto_db.transactions.DatabaseRoot``
     - yes
   * - 19
     - ``proto_db.core.dbobject.DBObject``
     - yes
   * - 20
     - ``proto_db.avl._Node``
     - no (abstract)
   * - 21
     - ``proto_db.avl.CountedKeyNode``
     - yes (added in format 3, D56)

Lazy loading (implementation-specific, portable)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A reference decodes to a *stub*: an object of the referenced type that holds only the
pointer, and loads its record on first use. This is a library technique, not a format
feature (R83a): nothing in the format requires it, and an implementation may load eagerly. The
same holds for every cache (decoded atoms shared by pointer, the record and page caches, the
query result cache) and for residency (unloading cold atoms): none of them is visible in the
bytes.


Canonical values
----------------

An attribute value is a **canonical value**. Its wire form is the msgpack data below; the
first column is the value's kind in the conformance manifest's value notation.

.. list-table::
   :header-rows: 1
   :widths: 14 38 48

   * - Kind
     - Wire form
     - Notes
   * - nil
     - msgpack ``nil``
     -
   * - bool
     - ``true`` / ``false``
     - a separate kind from integers (see `Order, equality and hashing`_)
   * - int
     - msgpack integer when ``-2**63 <= n <= 2**64 - 1``, else ``{"@int": text}``
     - ``text`` is the signed lowercase hexadecimal form (``format(n, "x")``: ``-1a``)
   * - float
     - msgpack ``float 64``
     - NaN, infinities and ``-0.0`` are kept bit for bit
   * - str
     - msgpack string
     - stored inline (D7)
   * - bytes
     - msgpack ``bin``
     -
   * - decimal
     - ``{"@dec": text}``
     - the exact decimal text, sign, exponent and trailing zeros kept: ``1.20``, ``-1.5E+30``,
       ``-0``, ``NaN``, ``sNaN``, ``-Infinity``
   * - date
     - ``{"@date": [year, month, day]}``
     - proleptic Gregorian, year 1 to 9999
   * - time
     - ``{"@time": [hour, minute, second, microsecond, fold, tz]}``
     -
   * - datetime
     - ``{"@dt": [year, month, day, hour, minute, second, microsecond, fold, tz]}``
     - ``fold`` (0 or 1) tells the first from the second of a repeated local time (PEP 495)
   * - timedelta
     - ``{"@td": [days, seconds, microseconds]}``
     - normalized: ``0 <= seconds < 86400``, ``0 <= microseconds < 10**6``, days may be negative
   * - uuid
     - ``{"@uuid": <16 bytes>}``
     -
   * - list
     - msgpack array of canonical values
     - read back as an immutable list
   * - tuple
     - ``{"@tup": [values...]}``
     - a tuple is orderable when its items are; a list is not
   * - map
     - ``{"@map": [[key, value], ...]}``
     - keys are any canonical values; insertion order is kept; a msgpack map is never used for
       a value
   * - missing
     - ``{"@missing": 0}``
     - the MISSING sentinel: an absent value, distinct from nil; the body MUST be the integer 0
   * - mutable
     - ``{"@mut": name}``
     - a ``MutableObject`` handle: the name (non-empty string) of a cell of the database's
       mutable table (D35); not an atom
   * - ref
     - ``{"@ref": [segment, offset, tag]}``
     - a nested atom: ``segment`` its 16 segment bytes, ``offset`` an integer >= 0, ``tag`` its
       type alias or name (as ``t`` of an atom record)

``tz`` is ``nil`` (a naive value), ``["zone", key]`` for an IANA time zone (``key`` such as
``"America/Argentina/Buenos_Aires"``) or ``["fixed", offset, name]`` for a fixed offset:
``offset`` in microseconds east of UTC (``-86400 * 10**6 < offset < 86400 * 10**6``) and
``name`` the zone's name. A reader rebuilds a fixed offset with that name; ProtoBase writes the
name its ``timezone`` object reports, which for an unnamed offset is ``"UTC"`` (offset 0) or
``"UTC+05:30"`` / ``"UTC-03:00"``. Any other kind of time zone cannot be stored
(``ProtoValidationError`` when writing).

Every tagged form is a msgpack map with **exactly one** key. A reader refuses with
``ProtoCorruptionError``: an unknown tag; a map with zero or several keys; a body of the wrong
shape or type (an ``@int`` body that is not hexadecimal, a ``@ref`` segment that is not 16
bytes or a string of 32 hexadecimal digits, an alias outside the table, a ``@missing`` body
other than 0, an empty ``@mut`` name, an unknown time zone key). The vectors include one input
of each kind of refusal.

Reserved tags
    ``@sym`` is reserved for symbols (the maintainer's runtime decision of 2026-11-08, option
    (c2): a symbol is an immutable value identified by its name, ``{"@sym": name}``, compared,
    hashed and ordered by name). **It is not implemented by this revision**: a ProtoBase reader
    refuses ``@sym`` as an unknown tag, so an implementation MUST NOT write it until ProtoBase
    reads it. When it is added it will be an additive change: a new tag, and a new rank after
    the mutable rank in the order below. Tags starting with ``@`` are reserved to the format.

Format 2 references
    Format 2 wrote ``@ref`` as ``[segment as 32 hexadecimal digits, offset, type name]``.
    Readers MUST accept it; writers MUST NOT write it.

Example bytes (from the vectors)::

    2**64              81 a4"@int" b1"10000000000000000"
    Decimal("1.20")    81 a4"@dec" a4"1.20"
    1.5                cb 3ff8000000000000
    2**63 - 1          cf 7fffffffffffffff
    ("x" tuple)        81 a4"@tup" 93 01 a1"x" 81 a4"@tup" 91 c3       for (1, "x", (True,))
    MISSING            81 a8"@missing" 00
    MutableObject      81 a4"@mut" a7"counter"
    @ref, built-in     81 a4"@ref" 93 c410 <16 bytes> 1a 06            Dictionary at offset 26
    @ref, application  81 a4"@ref" 93 c410 <16 bytes> 64 b3"conformance.Invoice"


Order, equality and hashing
---------------------------

Keyed structures (dictionaries, sets, hash maps, indexes) place their keys by two functions of
the value model. A **reader** that only iterates needs neither; a reader that looks a key up,
and every **writer** of a keyed structure, MUST compute them exactly as specified, or its
lookups miss and its trees are disordered.

Order keys
~~~~~~~~~~

Every orderable value has an order key ``(rank, payload)``. Keys compare by rank first, then
by payload within one rank. The ranks, in order:

.. list-table::
   :header-rows: 1
   :widths: 8 22 70

   * - Rank
     - Values
     - Payload and its order
   * - 0
     - MISSING
     - 0
   * - 1
     - nil
     - 0
   * - 2
     - bool
     - 0 for false, 1 for true
   * - 3
     - int, float, decimal
     - the exact numeric value; ``int``, ``float`` and ``decimal`` compare exactly, never through
       a binary conversion (``10**16 + 1 != 10**16``, ``0.1`` the float is not ``0.1`` the
       decimal); ``-0.0 == 0.0 == 0``; infinities order at the ends; **NaN is not orderable**
   * - 4
     - str
     - code point order
   * - 5
     - bytes
     - unsigned lexicographic order
   * - 6
     - date
     - the day
   * - 7
     - naive time
     - microseconds since midnight
   * - 8
     - aware time
     - microseconds since midnight minus the UTC offset in microseconds (may be negative)
   * - 9
     - naive datetime
     - the date and time
   * - 10
     - aware datetime
     - microseconds since 0001-01-01T00:00 of the local wall time, minus the UTC offset
       resolved with ``fold``: the UTC instant
   * - 11
     - timedelta
     - the signed duration
   * - 12
     - uuid
     - the 128-bit unsigned integer of its 16 bytes, big-endian
   * - 13
     - tuple
     - lexicographic over the items' order keys; a shorter prefix orders first
   * - 14
     - atom
     - the 128-bit integer of its ``oid`` (big-endian)
   * - 15
     - mutable handle
     - its name, in code point order

Two values are **equal keys** exactly when their order keys are equal: ``1``, ``1.0`` and
``Decimal("1.00")`` are one key, ``True`` and ``1`` are two, and two aware datetimes of one
instant in different zones are one key. NaN (float or decimal), lists, maps and tuples holding
one are not orderable; using one as a key is ``ProtoValidationError``. The vectors list values
in ascending order (``order.ascending``), groups of equal keys and unorderable values.

Hash keys
~~~~~~~~~

``hash_key(value)`` is a signed 64-bit integer, identical in every process, platform and
release (D24). It is the BLAKE2b digest (``digest_size = 8``, no key, salt or personalization)
of an encoding of the value's order key, read as a **signed little-endian** 64-bit integer. The
encoding ``feed(rank, payload)`` is:

1. the rank as one byte;
2. then, by rank:

   - numbers (rank 3): 8 bytes, little-endian, of the value's residue modulo the prime
     ``P = 2**61 - 1``. For an integer ``n``: ``n mod P`` (non-negative). For a finite float:
     its exact ratio ``n/d`` gives ``n * d**(P-2) mod P``. For a finite decimal with sign ``s``,
     digits ``m`` and exponent ``e``: ``r = m * 10**e mod P`` (``10**e`` as the modular inverse of
     ``10**-e`` when ``e < 0``), and ``P - r mod P`` when negative. Equal numbers therefore give
     equal residues. Positive and negative infinity are the 4 ASCII bytes ``+inf`` and ``-inf``
     instead (no length prefix);
   - tuples (rank 13): the number of items as 8 bytes little-endian, then ``feed`` of each
     item's order key;
   - strings (ranks 4 and 15): the length in bytes as 8 bytes little-endian, then the UTF-8
     bytes;
   - bytes (rank 5): the length as 8 bytes little-endian, then the bytes;
   - every other rank: the payload as an integer -- 0 for MISSING and nil, 0/1 for bool, the
     proleptic Gregorian ordinal of a date (0001-01-01 is 1), microseconds since
     0001-01-01T00:00 for a naive datetime, the aware payloads of the table above, microseconds
     for a time or timedelta, the 128-bit value of a UUID or an atom's oid -- written as its
     two's-complement little-endian bytes of length ``bit_length(v) // 8 + 1`` (so 0 is one byte
     ``00`` and 255 is ``ff 00``), preceded by that length as 8 bytes little-endian.

Equal keys have equal hashes. The vectors list the hash of 30 values (``hashes``).

Exact tokens
~~~~~~~~~~~~

Key-group and vector indexes keep, beside a value's order key, its **exact token**, which
tells apart values that are equal keys but not identical (``1``, ``1.0`` and
``Decimal("1.00")``). An exact token is a tuple stored as a canonical value; it starts with a
type tag:

.. list-table::
   :header-rows: 1
   :widths: 20 80

   * - Value
     - Token
   * - nil
     - ``("N",)``
   * - bool / int
     - ``("b", 0|1)`` / ``("i", n)``
   * - float
     - ``("f", hex)``, the C99 hexadecimal text of the float (Python ``float.hex``: ``0x1.8000000000000p+0``)
   * - decimal
     - ``("D", text)``, the ``@dec`` text
   * - str / bytes
     - ``("s", s)`` / ``("y", b)``
   * - date
     - ``("d", ordinal)``
   * - time / datetime
     - ``("t" | "dt", iso, fold, tz)``: ``iso`` is the ISO 8601 text of the naive wall value
       (``2026-11-10T12:00:00``, with ``.ffffff`` only when the microsecond is not 0), and
       ``tz`` is nil for a naive value, else ``(offset in microseconds, repr)``
   * - timedelta
     - ``("td", days, seconds, microseconds)``
   * - uuid
     - ``("u", 128-bit integer)``
   * - tuple / list / map
     - ``("T", tokens)`` / ``("FL", tokens)`` / ``("FM", ((key token, value token), ...))``: a
       stored list or map is the immutable kind, tagged ``FL`` and ``FM``
   * - mutable handle
     - ``("@mut", name)``
   * - atom
     - ``("a",)``

``repr`` is the Python representation of the time zone object, which a non-Python writer
reproduces as text: ``datetime.timezone.utc`` for offset 0 named ``UTC``;
``datetime.timezone(datetime.timedelta(seconds=19800))`` for an offset with its default name;
``datetime.timezone(datetime.timedelta(days=-1, seconds=75600), 'ART')`` for a named offset
(the timedelta normalized as Python prints it); ``zoneinfo.ZoneInfo(key='America/New_York')``
for an IANA zone. A value of a subclass of a built-in type gets its Python
``module.QualifiedName`` as its tag. These Python spellings are a known portability defect
(`Known gaps`_); the cost of a mismatch is bounded: a token that differs for one value only
makes an index keep two members for it, never a wrong answer. The vectors list 19 tokens
(``exact_tokens``).


Atom types and their persisted fields
-------------------------------------

Every structure is an immutable atom; a new version is a new atom that shares the unchanged
subtrees of the old one by pointer. ``ref`` below means an ``@ref`` to an atom of the type
named; ``tree`` means a ref to the root node of an AVL tree, or nil for the empty tree.

AVL nodes
~~~~~~~~~

``proto_db.avl.PosNode`` (positional trees): ``value``, ``left``, ``right``, ``height``,
``count``.

``proto_db.avl.KeyNode`` (keyed trees): ``key``, ``value``, ``left``, ``right``, ``height``,
``count``.

``proto_db.avl.CountedKeyNode``: a KeyNode whose ``value`` is a tuple ``(payload, count, ...)``
with an integer ``count`` at index 1, plus ``weight``, the sum of ``value[1]`` over its subtree.

Invariants a writer MUST keep: ``left`` and ``right`` are refs to nodes of the same kind as
the parent, or nil; ``height = 1 + max(height(left), height(right))`` with nil counting 0;
``count = 1 + count(left) + count(right)``; ``|height(left) - height(right)| <= 1`` (AVL); in a
keyed tree every key of ``left`` orders strictly below ``key`` and every key of ``right``
strictly above, by order key; in a positional tree the in-order sequence is the element order
(``count`` gives the positions); ``weight = value[1] + weight(left) + weight(right)``.

``proto_db.avl.ValueBox``: ``value``. A node value that is a string or bytes of at least 256
characters or bytes is stored through a box (D51): the node's ``value`` is then a ref to a
ValueBox whose ``value`` is the string. A reader unboxes every node value that is a ref to a
ValueBox; boxes appear only as node values, never as keys and never elsewhere. Writers SHOULD
box from 256 up (the threshold is a writer's choice that does not change the meaning, but it
changes the bytes).

Collections
~~~~~~~~~~~

Every collection persists ``count`` and ``indexes`` (a ref to an IndexSet, empty by default)
plus the fields below.

``proto_db.collections.IndexSet``
    ``root``: a KeyNode tree from index name (str) to a ref to the index.

``proto_db.collections.List``
    ``root``: a PosNode tree of the elements in order. ``count``: the number of elements.

``proto_db.collections.Dictionary``
    ``root``: a KeyNode tree ordered by the order key of the key; node value = the mapped
    value. ``count``: the number of keys.

``proto_db.collections.HashDictionary``
    ``root``: a KeyNode tree whose key is ``hash_key(key)`` and whose value is a *bucket*: a
    tuple of ``(key, value)`` tuples whose keys share that hash, none equal to another. ``count``:
    the number of keys.

``proto_db.collections.Set``
    ``root``: a KeyNode tree whose key is ``hash_key(element)`` and whose value is a bucket
    tuple of the elements sharing that hash. ``count``: the number of elements.

``proto_db.collections.CountedSet``
    As Set, but a bucket holds ``(element, count)`` tuples with ``count >= 1``; ``count`` is the
    number of distinct elements and ``total_count`` the sum of the counts.

``proto_db.collections.RepeatedKeysDictionary``
    ``root``: a KeyNode tree ordered by key whose values are refs to non-empty CountedSets
    (each with an empty IndexSet) of the records under that key. ``count``: distinct
    ``(key, record)`` pairs; ``total_count``: every record occurrence.

Bucket values are never boxed. Set elements, dictionary keys and hash keys MUST be orderable.

Indexes
~~~~~~~

Every index persists its definition, ``name`` (a non-empty str) and ``key_spec``, a tuple
``(kind, arg)``: ``("path", "a.b")`` (a dotted attribute path), ``("element", nil)`` (the
element itself) or ``("extractor", name)`` (a function registered by name in each process;
only the name is persisted). An extractor is a per-process registration: an implementation
that writes or queries such an index MUST provide a function of that name that computes the
same keys (D56: a name and its declared version are the extractor's identity).

``proto_db.indexing.EqualityIndex`` and ``proto_db.indexing.RangeIndex`` (key-group indexes)
    ``groups``: a CountedKeyNode tree ordered by the extracted key, whose values are *groups*
    ``(members, total)``; ``side``: the group of the elements whose key is unorderable (a
    tuple, ``(nil, 0)`` when empty); ``total_count``: every stored occurrence. ``members`` is a
    CountedKeyNode tree mapping a **member key** to ``(element, count)``, and ``total`` the
    occurrences in the group. A member key is ``(equality part, exact token)``: the equality
    part is ``(0, element)`` for an orderable element and ``(1, surrogate)`` for an unorderable
    one, where the surrogate is ``("l", items)`` for a list, ``("d", pairs sorted by key)`` for a
    map, ``("t", items)`` for a tuple holding an unorderable value, ``("n",)`` for NaN and
    ``("v", value)`` for any other value, items encoded the same way. An element absent from
    the key path is filed under MISSING. The two classes differ only in the plans they offer.

    Indexes written before format 3's CountedKeyNode (plain KeyNode trees, no ``weight``) are
    still read and answer every query; they offer no rank access until rebuilt (D56).

``proto_db.indexing.VectorIndex``
    ``dim`` (int >= 1), ``metric`` (``"cosine"``, ``"l2"`` or ``"ip"``), ``backend``
    (``"exact"``, ``"ivf_flat"`` or ``"hnsw"``), ``params`` (the complete backend parameters as
    a tuple of ``(name, value)`` pairs sorted by name: exact ``numpy``; ivf_flat ``nlist``,
    ``nprobe``, ``retrain_growth``, ``seed``, ``train_iters``; hnsw ``M``, ``ef_construction``,
    ``ef_search``, ``seed``), ``store`` (a KeyNode tree mapping ``(list number, member key)`` to
    ``(element, count, vector)``, the vector a tuple of ``dim`` floats; the list number is the IVF
    list, -1 while untrained, 0 for the other backends), ``invalid`` (the group of the elements
    without a valid vector), ``total_count``, ``centroids`` (a tuple of vectors, empty unless
    trained) and ``trained_size``. ``numpy`` and the hnsw graph are Python implementation
    choices (the graph is rebuilt in each process and never persisted); another implementation
    MUST preserve ``params`` as written.

Records
~~~~~~~

``proto_db.core.dbobject.DBObject``
    An immutable record: its state map is the record's attributes, any names that are not
    ``oid``, ``atom_pointer`` or ``context`` and do not start with ``_``. Reading an absent
    attribute gives nil.

Application types
    A subclass of DBObject (or of any atom) with a declared name, such as
    ``conformance.Invoice`` in the vectors, is written with that name as its tag and the same
    state map rules.

.. _spaceroot:

Space and database roots
~~~~~~~~~~~~~~~~~~~~~~~~

``proto_db.transactions.SpaceRoot``
    The atom ``space.root`` points to. ``databases``: a ref to a Dictionary from database
    name (str) to a ref to its current DatabaseRoot. A root committed under a cluster lease
    (D46) also carries ``cluster_term`` (the term of the lease, int), ``cluster_epoch`` (its
    cluster epoch, int) and ``cluster_previous`` (a ref to the SpaceRoot it was built on, or
    nil). These three are optional: **absent means nil** (a root committed outside a
    cluster).

``proto_db.transactions.DatabaseRoot``
    One version of a database:

    - ``objects``: a ref to a Dictionary from root name (str) to the root's value;
    - ``mutables``: a ref to a Dictionary from mutable cell name (str) to the cell's value (D35).
      Required: a DatabaseRoot without it was written before D35 and cannot be opened
      (``ProtoCorruptionError``);
    - ``version``: an int, 0 for the version ``new_database`` creates, then +1 per commit;
    - ``committed_at``: an aware datetime in UTC (``["fixed", 0, "UTC"]``);
    - ``previous``: a ref to the previous version's DatabaseRoot, nil for version 0;
    - ``origin``: 16 bytes drawn when the database is created and carried unchanged by every
      later version, the database's identity across processes (D56). **Optional: absent means
      nil**, for databases created before it existed; such a database never gets one.

To read a database: decode ``space.root``, load its SpaceRoot, look the database's name up in
``databases``, then read ``objects`` and ``mutables`` of that DatabaseRoot. History is the chain
of ``previous`` links; every version stays readable (nothing is ever deleted).

To commit a new version a writer appends the new atoms (children before the atoms that refer
to them), then a new DatabaseRoot (``version`` + 1, ``previous`` the version it was built on,
``origin`` carried, ``committed_at`` now in UTC), a new ``databases`` Dictionary and a new
SpaceRoot, and publishes the SpaceRoot's pointer as the new root (`Storage layouts`_). Conflict
detection between concurrent writers (read sets, merges of concurrently written collections)
is the library's job and leaves no trace in the format, except the epoch check of the publish.


Framed log files
----------------

Every append-only log of a node (``roots.log``, ``ranges.log``, ``segments.log``, a shared
storage's ``cluster/<node_id>.roots``) has this layout.

Header (14 bytes): magic ``PDB2LOG\0`` (8 bytes), the log format version (``u16``, 2), and the
kind (4 ASCII bytes: ``ROOT``, ``RANG`` or ``SEGS``). A reader checks all three: a header that is
only a prefix of the expected one (possibly followed by zero bytes, and nothing else) is a new
log; any other header is ``ProtoCorruptionError``, another version ``ProtoNotSupportedError``.

Then frames, one per entry:

.. list-table::
   :header-rows: 1
   :widths: 15 15 70

   * - Offset
     - Size
     - Content
   * - 0
     - 2
     - magic ``C1 F7``
   * - 2
     - 4
     - payload length, ``u32``, greater than 0
   * - 6
     - 4
     - CRC32 of the payload
   * - 10
     - 4
     - CRC32 of the 10 bytes before it
   * - 14
     - length
     - the payload

A reader walks the frames from the header. At the first position that does not hold a valid
frame it decides between:

- a **torn tail**, which is dropped: fewer bytes than a frame header remain; or the header is
  valid and its frame extends past the end of the file; or no valid frame starts after it
  (after the frame's extent when its header is valid, at any later offset when it is not);
- **mid-log corruption**, ``ProtoCorruptionError``: such a search finds a valid frame. Entries
  after damaged bytes are never silently dropped.

A writer that opens a log for appending first truncates a torn tail and ``fsync``-s the file,
so that an append never lands after torn bytes.

``segments.log`` (kind ``SEGS``)
    One entry per own segment, in creation order: the segment's 16 UUID bytes.

``ranges.log`` (kind ``RANG``)
    One entry per accepted commit: a 44-byte head ``root segment (16) | root offset (u64) |
    label (u64) | epoch (u64) | count (u32)``, then ``count`` ranges of 32 bytes ``segment (16) |
    start (u64) | end (u64)``: the byte ranges of the records shipped with that commit. A
    compaction rewrites the file with the live entries.

The acceptor log (kind ``ROOT``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

What a node must remember across a crash to keep its promises (D46, D50, D54). A root is
written as ``(epoch, segment, offset)`` with epoch 0, a zero segment and offset 0 meaning the
empty space; its *label* is the term of the lease that replicated it. Entry payloads, by their
first byte:

.. list-table::
   :header-rows: 1
   :widths: 14 14 72

   * - Kind
     - Size
     - Payload (little-endian)
   * - 1 accepted
     - 41
     - ``kind (u8) | label (u64) | epoch (u64) | segment (16) | offset (u64)``: the node accepted
       this root
   * - 2 committed
     - 41
     - same layout: the node knows this root committed (knowledge, appended without its own
       ``fsync``)
   * - 3 promise
     - 41
     - same layout, ``label`` = the lease term granted, root fields zero
   * - 4 rejoined
     - 41
     - same layout, every field after the kind zero: the log holds what the node knows (a log
       without it is new, wiped, or its rejoin was interrupted)
   * - 5 records
     - 93 + items
     - head ``kind | label (u64) | epoch (u64) | segment (16) | offset (u64) | seq (u64) | pred
       label (u64) | pred epoch (u64) | pred segment (16) | pred offset (u64) | count (u32)``,
       then ``count`` items ``segment (16) | offset (u64) | length (u32)`` each followed by
       ``length`` bytes of a framed record (header included): an accepted root with every record
       its replication carried
   * - 6 replica
     - 98 + ranges + items
     - head ``kind | flags (u8) | label | epoch | segment | offset | seq | pred label | pred epoch
       | pred segment | pred offset | range count (u32) | record count (u32)``, then the ranges (32
       bytes each, as in ``ranges.log``), then the record items as in kind 5. Flag bit 0
       (``ACCEPTED``) marks an accepted root; without it the entry only makes the records durable
       on a data replica. Other flag bits are ``ProtoCorruptionError``.

``seq`` is the write permission the root was committed under (0 under a plain lease); ``pred``
is the root it was built on, all zero when unknown (``pred`` label 0 and epoch 0). Kinds 1 to 4
are exactly 41 bytes; another size, an unknown kind, a kind-5 or kind-6 entry cut inside an
item or with trailing bytes is ``ProtoCorruptionError``. Every promise, accepted root and
rejoined marker is ``fsync``-ed before the message it justifies is sent, whatever the
storage's durability mode. A reader takes the highest accepted and committed roots in the
order ``(label, epoch, segment bytes, offset)``, and the highest promise. Kind-5 and kind-6
records are *redone* (written at their offsets) when the storage opens; a compaction
summarizes entries a checkpoint covered (implementation-specific timing).


Cluster wire protocol
---------------------

Nodes exchange **frames** over TCP: a 4-byte **big-endian** length, then that many bytes of
msgpack. A frame longer than 64 MiB (``MAX_FRAME_BYTES``) is a protocol error and the receiver
drops the connection; a node never stores a record larger than 16 MiB (``MAX_RECORD_BYTES``),
and a replication splits its records into chunks of at most 16 MiB (``CHUNK_BYTES``). There is
no handshake: a connection carries frames from its first byte.

Every message is a msgpack map with ``v`` (the protocol version, 13), ``type`` and ``from``
(the sender's node id, a string), then the fields of its type, written in the order of the
table. A message of another version, an unknown type, a missing field or an ill-typed field is
dropped by the receiver; extra fields are ignored.

Field types:

- *int*: a non-negative integer; *number*: a non-negative integer or float; *str*, *bool*;
- *uuid*: 16 bytes; *optional bytes* / *optional str*: the type or nil;
- *root*: nil (unknown), ``[0, nil, nil, label]`` (the empty space, known) or ``[epoch, segment
  16 bytes, offset, label]``; a 3-item form without the label is read with label 0;
- *records*: an array of ``[segment 16 bytes, offset, framed record bytes or nil]``;
- *ranges*: nil or an array of ``[segment 16 bytes, start, end]`` with ``start <= end``;
- *list of str*; *after*: nil or ``[seq > 0, holder str]``.

.. list-table:: Message types (besides ``v``, ``type`` and ``from``)
   :header-rows: 1
   :widths: 16 84

   * - Type
     - Fields
   * - ``request``
     - term, rid, inc, vs (ints)
   * - ``grant``
     - term, rid, inc (ints); root, committed (roots); certain (bool)
   * - ``reject``
     - term, rid, inc; root; certain; reason (str); highest (int); remaining (number)
   * - ``release``
     - term, inc; root, committed; certain, held (bools); next (optional str); more (bool)
   * - ``records``
     - term, rid, inc, index (ints); records
   * - ``warm``
     - term; records
   * - ``replicate``
     - term, rid, inc; root, committed (roots); records; chunks (int); base (root);
       base_ranges (ranges); seq (int); pred (root); vs (int); copies (list of str); ranges
   * - ``store``
     - term, rid, inc; root; records; chunks; ranges; copies; vs
   * - ``propose``
     - term, seq, inc; root, pred
   * - ``renew``
     - term, rid, inc, vs
   * - ``permit_req``
     - term, rid, inc
   * - ``permit``
     - term, rid, seq, inc; root; certain; valid (number); after
   * - ``permit_no``
     - term, rid, inc; reason (str)
   * - ``done``
     - term, seq, inc; outcome (str: ``committed``, ``none`` or ``unknown``); root; copies; ranges
   * - ``ack``
     - term, rid, inc; ok (bool); root; stored (bool)
   * - ``want``
     - (none)
   * - ``query``
     - rid, inc, ballot, vs
   * - ``answer``
     - rid, inc; root, committed; certain; promised (int); state (str: ``ok``, ``empty`` or
       ``unknown``); ballot (int)
   * - ``fetch``
     - rid, inc; segment (uuid); offset (int)
   * - ``fetched``
     - rid, inc; segment (uuid); offset; data (optional bytes: a framed record)
   * - ``commit_query``
     - rid, inc; root
   * - ``commit_info``
     - rid, inc; persisted (bool); ranges

Values a member must compute like the others:

- **Ballots.** A lease term (label) is proposer-qualified: its low 12 bits (``RANK_BITS``) are
  the proposer's rank, the index of its node id in the sorted (code point order) list of
  every member id. The smallest ballot of rank ``r`` above ``floor`` is
  ``((floor - r) // 4096 + 1) * 4096 + r`` (floor division). A refound adds ``2**44``.
- **Voter-set fingerprint** (``vs``): the CRC32 of the UTF-8 bytes of the voter ids, sorted in
  code point order and joined with ``","``. The default voters are the first three member ids in
  that order (every member of a smaller cluster).
- ``inc`` is a random integer a node draws at each start (its incarnation); ``rid`` numbers
  requests within an incarnation.

The meaning of each message, and the rules that make the protocol safe, are those of the
coordinator documentation and of decisions D44 to D55; the field list above is complete for
protocol 13. The vectors hold one frame of every type.


What is normative and what is not
---------------------------------

Normative: every byte layout on this page; the type names and the alias table; the value
encoding; the order, hash and exact-token functions (for writers of keyed structures); the
structure invariants of the AVL trees and collections; the field names, their optionality and
the meaning of absence; the root-publication protocol of a shared directory (lock, epoch,
rename); the log reading rules (torn tail versus corruption); the cluster frame and message
layouts and the values members compute (ballots, fingerprint).

Implementation-specific (nothing on disk depends on them, and an implementation MAY do
otherwise): lazy loading and stubs (portable as a technique, R83a); the decoded-atom cache, the
record and page caches, residency and unloading; the query result cache and query plans; the
per-version operation logs of collections (never persisted) and the merge of concurrent
writers; group commit; durability modes (``fsync``, ``flush``, ``none``) other than the acceptor
log's unconditional sync; segment size, sealing, shipping and checkpoint timing; the
``ValueBox`` threshold (it changes bytes, not meaning); pager tokens (a client-side msgpack
token, never stored in the database); the ``numpy`` parameter of exact vector indexes and the
hnsw graph.


Format history
--------------

Every change since format 1 and how a reader of this revision treats data written before it.

.. list-table::
   :header-rows: 1
   :widths: 18 52 30

   * - Change
     - What changed
     - Old data
   * - Format 1 to 2 (D46, fix round 13)
     - Self-validating record headers (magic, header CRC); version 1 framed ``length | kind |
       crc32 over kind and payload``
     - Format 1 is not readable (there were no users to migrate)
   * - Log format 1 to 2 (D46, fix round 13)
     - Self-validating frame headers; version 1 framed ``length | crc32 over length and payload``
     - Log format 1 is not readable (``ProtoNotSupportedError``)
   * - Format 2 to 3 (D51)
     - ``@ref`` segment as 16 bytes instead of 32 hexadecimal digits; built-in types as integer
       aliases in ``@ref`` and ``t``; large node values in ValueBox atoms
     - Format 2 segments, records and roots are read; new segments are format 3 (a format-2
       reader cannot read them)
   * - Additive in format 3 (D56)
     - New built-in type ``proto_db.avl.CountedKeyNode`` (alias 21) for key-group index trees,
       with ``weight``
     - Plain KeyNode index trees are read and answer every query, without rank access
   * - Additive in format 3 (D56, R99 I2)
     - New optional ``DatabaseRoot.origin``
     - Absent reads as nil; such a database is identified by name only, permanently
   * - Additive in format 3 (D46, D50, D54)
     - Optional ``SpaceRoot`` cluster fields; acceptor-log entry kinds 5 and 6
     - Absent fields read as nil; a reader older than kind 5 refuses such a log, never misreads it
   * - Type names (D57)
     - New classes declare language-neutral names; Python-default names are no longer written
     - Every old name still reads (legacy registration, ``__type_aliases__``); no format bump

The cluster protocol has no compatibility across versions: every node of a cluster runs the
same protocol version (13 here; the history of versions 6 to 13 is in
``proto_db/cluster/protocol.py``). Before D35 a DatabaseRoot had no ``mutables``; such a
database cannot be opened.


Differences from the design document
------------------------------------

The design document (section 6) predates several decisions; the code differs as follows.

- The root record is ``crc32 | msgpack{version, segment, offset, epoch}``, the CRC as a 4-byte
  prefix over the msgpack bytes, not a ``crc`` field inside the map; ``segment`` is 16 bytes.
- ``@ref`` is ``[16 segment bytes, offset, alias or name]`` (format 3), not ``[segment hex,
  offset, type name]``.
- New atom types no longer default ``__type_name__`` to ``module.ClassName`` (D57).
- There is no ``proto_db.tools.inspect`` module in this revision.


Known gaps
----------

Found while writing this specification; none is fixed here.

- **Symbols** (``@sym``) are decided but not implemented: an implementation cannot store them
  yet without breaking the round trip.
- **Exact tokens** carry Python spellings (``repr`` of a time zone, the qualified name of a value
  of a subclass of a built-in type). A non-Python writer must reproduce them to share index
  members with ProtoBase; a mismatch costs a duplicate index member, not a wrong answer.
- **Lone surrogates.** ``canonical`` accepts a Python string holding a lone surrogate, the hash
  function encodes it (``surrogatepass``), but msgpack cannot pack it: the save fails with
  ``UnicodeEncodeError`` instead of ``ProtoValidationError``. Strings in the format are valid
  UTF-8.
- **Extractor and vector backends** are per-process functions: an index on an extractor, or an
  hnsw index, is only fully usable by an implementation that provides the same function or
  backend.


Conformance vectors
-------------------

``tests/conformance/`` holds the vectors (``vectors/``), their manifest (``manifest.json``, a
JSON description of what every file means, in a language-neutral notation), the generator
(``generate.py``) and the test that checks the committed files byte for byte
(``test_conformance.py``). They cover every value kind, the refused inputs, the order, hash and
exact-token functions, record, segment and root framing (current and format 2), a complete
database written through the public API (every persistable built-in type and one application
type, three versions of one database, a mutable cell), a cluster commit stamped with its lease, the three log
kinds with every acceptor-log entry kind, and one frame of every cluster message type.
``tests/conformance/README.md`` explains the notation and how another implementation uses them.
