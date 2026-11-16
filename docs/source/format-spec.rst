Storage format specification
============================

This page specifies the bytes ``proto_db`` writes: the storage files, the atom records and the
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
     - 5
     - every segment header and every root record; versions 2 to 5 are read, and 4 is written
       unless the data uses a feature of 5 (`Format stability`_)
   * - Framed log format (``LOG_FORMAT_VERSION``)
     - 3
     - the header of every framed log file; versions 2 and 3 are read, and 2 is written unless the
       log is an acceptor log holding a versioned entry (kind 7, `The acceptor log (kind ROOT)`_)
   * - Cluster protocol (``PROTOCOL_VERSION``)
     - 14
     - the ``v`` field of every cluster message; no other version is accepted

There are three levels of conformance:

- **Reader**: decodes every structure below, refuses every invalid input the specification
  says is refused, and decodes the vectors to their manifest.
- **Writer**: also produces structures that a reader of this revision accepts. A writer MUST
  write every value in its canonical encoding (`Canonical values`_) and MUST compute order keys,
  hashes and exact tokens as specified, since those decide where readers look things up.
  Beyond that, writers SHOULD produce the vectors' bytes, but some choices are left to them and
  change bytes without changing meaning:

  - the order of the attributes in an atom's state map (`Atom records`_);
  - the shape of every AVL tree, which depends on the operation history and on bulk building
    versus incremental insertion (only the invariants of `AVL nodes`_ are normative);
  - the order of the entries inside a hash bucket;
  - the ValueBox threshold;
  - when an IVF index is trained and the centroids training produces (`Vector indexes`_);
  - which atoms are new in a version and which are shared with the previous one.
- **Cluster member**: also speaks the cluster wire protocol and keeps the acceptor log. The
  behaviour of the coordination protocol (leases, permissions, acceptance rules) is not part
  of this page; it is specified in :doc:`coordinator-protocol` and decisions D44 to D55.
  This page gives every byte layout that behaviour uses.

Format stability
----------------

This revision is the **freeze point** of the format: the tag ``v2.0.0rc1`` marks it, and
implementations in other languages (protoBaseST first) build against it.

**What a version number covers.** From the tag on, a change to any item below raises the version
number named for it, and the conformance vectors are regenerated in the same commit. A change
that only adds readable inputs (a new reader-side tolerance) raises nothing and adds vectors.

``FORMAT_VERSION`` (segment headers and root records) covers:

- the value encoding: every tag and its body, the one spelling per value, the map-key rule, the
  nesting limit (`Value nesting depth`_: 1022 containers per value, so every reader accepts
  records 1024 containers deep; a reader that stops earlier, at 509 levels for example, is not
  conforming from ``v2.0.0rc2`` on);
- the atom envelope, the type-name grammar, the built-in alias table (an added built-in type is
  a change: an older reader refuses its name), and the reserved attribute and root names;
- the persisted fields of every built-in type: their names, types, whether each is required,
  the meaning of absence and of every value;
- the record framing, the segment header and the root record;
- the order keys and their ranks, the hash function and the exact tokens;
- the structure invariants: AVL balance and weights, key order, the hash-bucket layout, the
  structure of every collection and of the space and database roots;
- the index member keys, the index maintenance contract and the IVF list assignment;
- what readers and writers do with opaque atoms (`Atoms of unknown types (D59)`_);
- the file-storage publication protocol (segment creation, the lock, the epoch, the rename and
  every ``fsync`` and its order) and the checks of `A minimal external writer (file storage)`_.

``LOG_FORMAT_VERSION`` (the header of every framed log file) covers the framed log layout and
the acceptor log's entry kinds and fields. ``PROTOCOL_VERSION`` (every cluster message) covers
the cluster frames and messages. A change to one of these two does not raise
``FORMAT_VERSION``. What `What is normative and what is not`_ marks implementation-specific is
covered by no number.

**Which version a writer writes.** A writer writes, in each segment header, the **lowest**
version sufficient for every record of that segment, and in each root record the lowest version
sufficient for everything reachable from it; that is never lower than the version of the root
it replaces. The base of this revision is 4. So a space whose data never uses a feature added
after 4 keeps version-4 segments and roots under any later writer, and readers of this revision
keep opening it. Once a feature of version *n* is written, its segment is version *n*, and every
later root of the space is too (a root is shared by all the databases of a space): a reader
that predates *n* refuses that segment or root with ``ProtoNotSupportedError``, never misreads
it. A reader of version *n* reads every version up to *n*. Processes of different revisions may
share a directory while only features all of them read are written; ``proto_db`` writes 4 in this
revision (``proto_db.storage.format.WRITE_FORMAT_VERSION``) for data that uses no later feature, and
``FEATURE_VERSIONS`` lists the version of every feature added later: ``parents`` (an atom whose
envelope has the key ``"p"``) is version 5, and so is ``registry`` (a record of one of the
runtime registry's built-in types, aliases 22 and 23, `Registry roots`_).

A segment header is written before the segment's records, so ``proto_db`` opens a new segment
for a record that needs a later version than the open segment's, and a segment's version is the
version of its **first** record (a segment of version 5 may hold later records of version 4; a
record never lands in a segment of a lower version than it needs). Every feature is visible in
the bytes of the records that use it. A node that writes replicated or recovered records into a
segment file it did not create (a cluster copy, a redo from the acceptor log) writes the header
version of the segment's first record, and the highest version it writes while it lacks that
record. A root record's version is the highest of 4, the version of the root it replaces and the
highest segment version its writer has written; in cluster mode the committer's root version
travels with the root (the fifth element of a wire *root*, `Cluster wire protocol`_, and the
versioned entry of the acceptor log), and a node that persists the root writes the highest of
that version and its own, so a root over format-5 data is format 5 whichever node persists it.
"Uses a feature" therefore means "used or attempted by this writer": a writer that appended a
version-5 record no committed root references (a commit that saved records and then failed) writes
version-5 roots from then on, so a reader of version 4 refuses that space although no reachable
record needs 5; this is conservative (nothing is lost or misread) and accepted.

**The pinned vectors.** ``tests/conformance/vectors.sha256`` lists the SHA-256 of every file
under ``tests/conformance/vectors/`` (``<hex digest>  <path>``, one line per file, sorted by path
in code point order, ``/`` as the separator), then the digest of ``manifest.json``. The SHA-256
of that list file is ``37b286fd97df098769aee26c222f76d295bf747b1a02f9441e52bd54e1ca1695``. ``test_conformance.py`` checks that the committed files
match the list and that this page states the list's digest. Each vector file is governed by
the number that covers what it holds: ``vectors/cluster/*.log`` by ``LOG_FORMAT_VERSION``,
``vectors/cluster/messages/`` by ``PROTOCOL_VERSION``, every other file by ``FORMAT_VERSION``.
A change to an existing vector file is a change of its number's items; a new vector adds lines
and changes the manifest's line, and raises nothing by itself.

Two extensions were decided after the freeze. Both are **additive**: no existing byte, order,
hash or token changes, data written without them keeps its version and its meaning, and they
are marked by the version rule above. The registry is implemented (format 5); symbols are not, and
symbol references are not part of format 5 (see `Symbol references`_ below).

Symbols (``@sym``)
    A new value kind, ``{"@sym": name}`` (option (c2) of the runtime-symbols investigation; see
    the reserved tags under `Canonical values`_), with a new order rank 16, after the mutable
    rank 15, and a hash and token derived from its encoding like every other kind. It is the
    format-5 feature ``@sym``: a segment holding one is version 5. A reader of format 4 refuses
    that segment (``ProtoNotSupportedError``) and, reading a record directly, refuses ``@sym``
    as an unknown tag (``ProtoCorruptionError``). The cluster ``PROTOCOL_VERSION`` rises in the
    same change, since cluster messages carry values. **Until then an implementation MUST
    refuse to store a symbol** (`Canonical values`_): stored as a string, it would become
    another key when ``@sym`` lands, and lookups by the symbol would silently miss it.

The per-database class registry
    Its persisted state is database roots whose names start with ``__``. **Root names starting
    with ``__`` are reserved for the format from ``v2.0.0rc1`` on**: ``proto_db`` refuses to
    write or remove one through the application API (``ProtoValidationError``); only the
    registry writes its own roots, at commit. Two of them are implemented, as the format-5
    features ``parents`` (an object's parents, the atom envelope key ``p``, `Atom records`_) and
    ``registry`` (the roots ``__currentModules`` and ``__catalog``, whose values are records of
    two new built-in types, `Registry roots`_). A space whose data never uses them keeps its
    version. ``__currentSymbols`` is **not** a defined root of format 5: it stays reserved only by
    the general ``__`` rule. Data written before the reservation
    may hold a root named like a registry root: a reader identifies the registry's roots by name
    **and** by the type of their value, and refuses a ``__`` root whose value is not of the
    registry's type (``ProtoCorruptionError``) rather than reading it as registry state. Such
    a database is corrupt for the registry: the application API cannot repair it (it neither
    writes nor removes ``__`` roots), so a repair is the job of tooling that rewrites the root,
    not of the API.

.. _symbol references:

Symbol references
    Symbol references are **not part of format 5**. The design of 2026-11-14 (a string already
    interned in the database written as a short content id, transparent to values, keys, hashes
    and tokens) was measured and deferred: with segment compression, references plus compression
    were larger than compression alone on the measured data set (the measurement and the verdict
    are in ``docs/superpowers/2026-11-14-symbol-measurement.md``). No feature named ``symbols``
    exists: ``FEATURE_VERSIONS`` lists only ``parents`` and ``registry``. The root
    ``__currentSymbols`` is not defined; a writer MUST NOT write it, and ``proto_db`` refuses to
    (the ``__`` rule above). Should the feature be adopted later it will be an additive change
    marked by a new format-5 feature, with its own entry in this page.

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
any valid encoding of an integer (a positive ``int 64``, say) and accept a ``float 32``, read
as the float 64 of the same value (``proto_db`` does both); a writer MUST NOT write either form.
The canonical encoding of a value, which exact tokens are made of, is always the smallest
form. Every msgpack map key in the structures below is a string: an atom envelope or a state
map with a key of another type (``bin``, an integer) is ``ProtoCorruptionError``, and a cluster
message with one is dropped (``strict_map_key``). Maps keyed by other values are canonical
``@map`` values.

**Text.** Strings are sequences of Unicode scalar values encoded in UTF-8. A string holding a
surrogate code point (U+D800 to U+DFFF, a lone surrogate) has no UTF-8 encoding and cannot be
written: ``proto_db`` refuses it with ``ProtoValidationError`` when the value is validated, before
anything is packed (R128).


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
    UUID4 name), ``fsync``-s the directory after creating it so that its directory entry is
    durable before any root can reference it, and never appends to another process's segment.
    To publish a root, a writer
    takes an exclusive ``flock`` on ``space.lock`` (non-blocking attempts until a timeout;
    ``msvcrt.locking`` of the first byte on Windows), reads ``space.root``, checks that its
    ``epoch`` is the one its transaction started from, writes the new root to
    ``space.root.tmp``, ``fsync``-s it, renames it over ``space.root`` and ``fsync``-s the
    directory. The new root's ``epoch`` is the old one plus one (1 for the first root). Every
    record a root references is flushed (and, in the default durability, ``fsync``-ed) before
    the root is written. An implementation that shares a directory with ``proto_db`` processes
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
    ``<directory>/cluster/<name>.roots`` (log kind ROOT), with ``<name>.roots.lock`` beside it.
    ``<name>`` is the node id itself when it matches ``[A-Za-z0-9._-]{1,64}``, and otherwise the
    lowercase hexadecimal of the id's UTF-8 bytes.

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
     - format version, ``u16``: 4 (writers); 2, 3 or 4 accepted by readers
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

    {"version": 4, "segment": <16 bytes>, "offset": <int >= 0>, "epoch": <int >= 0>}

``segment`` and ``offset`` point at the record of the space's :ref:`SpaceRoot <spaceroot>` atom;
``epoch`` counts the publications (the compare-and-swap token). A reader checks the CRC and the
field types (``ProtoCorruptionError`` otherwise) and accepts ``version`` 2, 3 or 4, reading
each as the current version (the shape did not change); another version is
``ProtoNotSupportedError``. Extra keys are ignored by the file storage's decoder. A missing
``space.root`` file means the space is empty.

The cloud storage's root object adds ``"recent"`` after ``epoch``: an array of ``[segment 16
bytes, offset, epoch]`` for up to ``root_history`` previously published roots, newest first.
The tiered storage's persisted root object adds, in this order, ``"recent": []`` and
``"term"``: the label of the lease that committed the root (0 when absent), so its keys are
``version, segment, offset, epoch, recent, term``. Each of the two storages decodes its own
object and validates its own extra key: a ``recent`` that is not an array of such triples is
``ProtoCorruptionError`` in the cloud decoder, and a ``term`` that is not a non-negative integer
is ``ProtoCorruptionError`` in the tiered decoder; other extra keys are ignored.

Example (``framing/space.root`` of the vectors: segment ``0123456789abcdef0123456789abcdef``,
offset 26, epoch 7)::

    7efee88c 84 a7"version" 03 a7"segment" c410 0123...cdef a6"offset" 1a a5"epoch" 07


Atom records
------------

The payload of an ATOM record is a msgpack map with exactly three keys, written in this
order::

    {"t": <type tag>, "oid": <16 bytes>, "s": <state map>}

or, for an object that has parents (the feature ``parents``, format version 5), with exactly
four keys, written in this order::

    {"t": <type tag>, "oid": <16 bytes>, "s": <state map>, "p": <parents>}

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
    ``proto_db`` reproduce the order shown by the vectors.

``p``
    The object's **parents** (format version 5, feature ``parents``), in order: a ``@tup``
    value (`Canonical values`_) of at least one item. Each item is either a reference to a
    record of ``proto_db.core.dbobject.DBObject`` or of an application subtype of it (an
    ``@ref``, like any nested atom; a writer saves an unsaved parent before its child, once),
    or a **registry name**: a non-empty string, written as a plain msgpack string. No other
    kind of item is a parent. A writer writes ``p`` only for an object that has parents: an
    object without parents writes the three-key payload, byte for byte as format 4, so its
    record keeps version 4. Only a ``DBObject`` record (or a record of a type the reader has no
    class for, `Atoms of unknown types (D59)`_, whose parents a reader keeps) may carry ``p``:
    on a record of any other built-in type it is ``ProtoCorruptionError``, and so is a ``p``
    that is not a non-empty tuple of those items, or that holds one item twice (the same record
    reference or the same name; a writer never writes a parent twice). A record that has ``p`` is of version 5, so
    it lands in a segment of version 5 or later (`Format stability`_). An object may not be its
    own parent; ``proto_db`` refuses to write one whose parent has its ``oid``
    (``ProtoValidationError``). What a parent means for reading an attribute (resolution along
    the parents) is behaviour, not format: it is specified by the runtime registry, and a
    reader that only reads data needs none of it.

A payload with other keys, or whose ``t`` names another type than the one the reference was
made with, is ``ProtoCorruptionError``.

**Required fields.** Every field listed for a built-in type in `Atom types and their persisted
fields`_ is REQUIRED unless it is marked optional. A record of a built-in type that lacks a
required field is ``ProtoCorruptionError`` when it is loaded (``proto_db`` checks it as soon as the
record is decoded, before any field is read). A writer MUST NOT add fields to a built-in type;
what a reader does with an unknown field of a built-in type is unspecified. Required fields bind
the built-in types only: an application type, including an application subclass of a built-in
collection, requires the fields of its built-in base and nothing more, so a field it adds in a
later release may be absent from its older records (it reads as nil).

**Attribute names.** A persisted attribute name is a non-empty string that does not start with
``_`` and is not ``oid``, ``atom_pointer`` or ``context``. The names of a type are shared by
every implementation of it, and a type's *behaviour names* are reserved: a persisted attribute
MUST NOT carry one.

- ``proto_db.core.dbobject.DBObject`` reserves ``with_``, ``has_defined_attr``, ``parents`` and
  ``with_parents``. The last two are reserved from format version 5 (an object's parents are
  the envelope key ``p``, never an attribute) for **new** fields only: a record written before
  may hold a field named ``parents`` or ``with_parents``, and a reader MUST still read it as an
  ordinary field (``proto_db`` keeps it readable and never refuses it, ruling R157). A writer
  MUST NOT create such a field, and keeps an existing one unchanged in a new version of the
  record.
- An application type reserves the names of the methods and properties its class defines in
  each implementation that defines it, **except accessors**. An accessor of an attribute is a
  method named like the attribute that only returns its stored value, or only stores a new
  value into it (``total`` and ``total:`` in Smalltalk). An accessor is not behaviour: a
  persisted ``total`` with a Smalltalk accessor ``total`` is valid. A method that does anything
  else is behaviour, a lazily initialising getter (``^total ifNil: [total := 0]``) included.
- Enforcement: a reader MAY refuse a record whose attribute names collide with the behaviour
  names of its own class for the type (``proto_db`` does); it need not check anything else, since
  it cannot know other implementations' classes. A writer MUST NOT write such a name.
- In ``proto_db`` a persisted attribute is read as a plain Python attribute: the field is its own
  accessor, and any attribute of the Python class with the same name, a property written as an
  accessor included, would hide it. ``proto_db`` therefore refuses a field named like any attribute
  of its Python class, when writing (``ProtoValidationError``) and when reading
  (``ProtoCorruptionError``), and both messages say so. The one exception is the two ``DBObject`` field names above,
  which a record written before format 5 may hold. A Python class for a shared type
  defines no accessors; it reads ``order.total`` directly.

An implementation cannot know the behaviour names of the others, so an application that shares
a type across languages MUST keep its attribute names disjoint from the non-accessor behaviour
names of every implementation of the type, and, for ``proto_db``, from every attribute of the
Python class: a protoBaseST writer that stores ``total`` on a type whose Python class has a
method or property ``total`` writes data ``proto_db`` refuses to open.

Type names (D57)
~~~~~~~~~~~~~~~~

A type name is language-neutral: two or more ASCII identifiers (``[A-Za-z_][A-Za-z0-9_]*``)
joined by dots, such as ``acme.sales.Order``. It names a type of the format, never a module of
an implementation; each implementation maps names to its own classes. Built-in types use the
prefix ``proto_db.``; applications SHOULD use a prefix of their own.

- In ``proto_db`` a class declares its name as ``__type_name__`` in its own body. A class that
  does not declare one is registered under its Python ``module.QualifiedName`` for reading
  data written before D57 only; saving an instance raises ``ProtoValidationError``.
- A class lists in ``__type_aliases__`` the names it was persisted under before. An alias reads
  as the class, and references spelled with either name reach the same atom. New data is
  always written under the declared name.
- The normative alias table of the format is the built-in table below. An application type's
  aliases belong to the type, not to an implementation: every implementation that defines the
  type MUST declare the same declared name and the same aliases.
- **The name a writer puts in a reference** (a ``@ref``, the ``t`` of a record) is the type's
  canonical name: the built-in alias for a built-in type, the declared name for a type it
  implements (never an alias), and for an opaque atom the name of the reference it read it
  through, as the owning implementation wrote it (usually the declared name; an older name when
  the owner wrote the reference before a rename). Readers resolve both, so copying the name read
  is always valid. Records and references written before a rename keep the old name, which stays
  a valid spelling: an application alias in a ``t`` or a ``@ref`` is never non-canonical.
- **How a reader resolves a reference.** The reference's name and the record's ``t`` must name
  one type: they are equal, or one is an alias of the other by the built-in table or by the
  reader's classes. When neither name has a class in the reader, the record's ``t`` decides: the
  record is an opaque atom of that type and the two names are taken as names of it (one may be
  an alias only the owning implementation knows). When the record's ``t`` has a class but the
  reference's name is unknown to the reader, the reference is ``ProtoCorruptionError``.
- A persisted name is never renamed without an alias, and built-in names are never renamed:
  code older than a rename cannot read the new name.

A reader MUST keep accepting every name it ever accepted. Data written before D57 by
``proto_db`` may contain Python-default names of application classes (``myapp.models.Person``);
an implementation that needs to read such data maps those names like any other.

Atoms of unknown types (D59)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Each implementation registers the classes of its own modules, so a database shared by several
languages holds records of types a given process has no class for. Such a record is valid data,
not corruption. A record whose type name ``t`` (or the type name of a ``@ref`` to it) is valid by
the grammar above, does not start with ``proto_db.`` and has no class in the reading process is an
**opaque atom**.

A reader:

- MUST decode an opaque atom's record like any other (envelope, ``oid``, state map, every value
  in it, references included) and MUST refuse it on the same grounds (``ProtoCorruptionError``);
- MUST keep its type name, its ``oid``, its pointer and its decoded fields, and present the
  fields for reading;
- MUST raise an explicit error naming the type (``proto_db``: ``ProtoNotSupportedError``) for every
  operation that needs the type's behaviour: a method, an attribute the record does not hold,
  a change, a construction, or its use where a built-in type is expected (a collection, a root
  atom);
- MUST refuse an unknown name that starts with ``proto_db.``, or one outside the grammar, with
  ``ProtoCorruptionError``: every built-in type of a format version is known to every reader of
  that version.

A writer:

- MUST NOT write a new record for an opaque atom: not a changed version, not a copy with its
  ``oid``, not a re-encoding of the record read. Its record stays byte for byte as the other
  implementation wrote it;
- MAY store references to it: a ``@ref`` with its pointer and the name of the reference it was
  read through (an application name, spelled in full; see the name a writer puts in a reference
  under `Type names (D57)`_), in any value it writes;
- MAY use it as a key or an element: its order key (rank 14, its ``oid``), its hash and its exact
  token (the atom placeholder) need no class, so it is filed exactly where the implementation
  that owns the type files it.

``proto_db`` reads an opaque atom as an instance of a per-name subclass of
``proto_db.core.atom.OpaqueAtom``; ``type(atom).__type_name__`` is the name it was referenced
by. A class registered later under that name takes over for the loads that follow. A Python
application that forgets to import the module of one of its own types meets the same fallback:
its records read as opaque atoms (``isinstance(x, Invoice)`` is false) instead of failing; the
verifier lists them under ``opaque_types``. The vector space
``polyglot`` holds one (`Conformance vectors`_).

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
   * - 22
     - ``proto_db.registry.ModuleList``
     - yes (added in format 5, feature ``registry``)
   * - 23
     - ``proto_db.registry.Catalog``
     - yes (added in format 5, feature ``registry``)

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
     - ``text`` is the signed lowercase hexadecimal form: ``-`` for a negative number, then the
       digits of ``|n|`` in ``0-9a-f`` with no leading zero, no ``0x`` and no ``+`` (``-1a``).
       ``@int`` is only for integers outside the native range; any other text, or ``@int`` of
       a native integer, is refused
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
     - the *to-scientific-string* of the decimal (`Decimal text`_), sign, exponent and trailing
       zeros kept: ``1.20``, ``-1.5E+30``, ``-0``, ``1E-7``, ``NaN``, ``-NaN123``, ``sNaN``,
       ``-Infinity``. Any other spelling (``1.0E+1``, ``1e3``, ``+1``, ``nan``) is refused
   * - date
     - ``{"@date": [year, month, day]}``
     - proleptic Gregorian, year 1 to 9999
   * - time
     - ``{"@time": [hour, minute, second, microsecond, fold, tz]}``
     - the wall clock; ``tz`` is ``nil``, ``["fixed", ...]`` or ``["zone", key]``
   * - datetime
     - ``{"@dt": [year, month, day, hour, minute, second, microsecond, fold, tz]}``
     - ``fold`` (0 or 1) tells the first from the second of a repeated local time (PEP 495);
       for a zone datetime the fields are its UTC instant (``tz`` below)
   * - timedelta
     - ``{"@td": [days, seconds, microseconds]}``
     - normalized: ``0 <= seconds < 86400``, ``0 <= microseconds < 10**6``, days may be negative
       (``-999999999 <= days <= 999999999``); an unnormalized body is refused
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
     - insertion order is kept; keys follow `Map keys`_; a msgpack map is never used for a
       value
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

``tz`` is one of:

``nil``
    A naive value.
``["fixed", offset, name]``
    A fixed offset: ``offset`` in microseconds east of UTC (``-86400 * 10**6 < offset < 86400 *
    10**6``) and ``name`` the zone's name. A reader rebuilds a fixed offset with that name.
    ``proto_db`` writes the name its ``timezone`` object reports, which for an unnamed offset is
    ``"UTC"`` (offset 0) or ``"UTC+05:30"`` / ``"UTC-03:00"`` (``UTC``, the sign, two-digit hours
    and minutes, then ``:SS`` and ``.ffffff`` only when not zero). The bytes depend only on the
    offset and the name, never on how the object was built.
``["instant", key]`` (``@dt`` only)
    An IANA time zone (``key`` such as ``"America/Argentina/Buenos_Aires"``). The fields of the
    ``@dt`` are the value's **UTC instant**, and ``fold`` MUST be 0. A reader presents the
    instant in the zone ``key`` with its own time-zone database; the instant itself, and so the
    value's order key, hash and token, never depend on that database (decision D58). A writer
    computes the instant at write time, with its own database, from the wall clock and
    ``fold``. Both the instant and the wall clock in the zone lie in years 1 to 9999: a writer
    refuses a value whose instant falls outside (``ProtoValidationError``), and a reader refuses
    an instant whose wall clock in ``key`` falls outside (``ProtoCorruptionError``; such bytes
    are never written for a value that exists in the zone).
``["zone", key]``
    In a ``@time``: an IANA zone; the fields are the wall clock (a time has no date, so no
    offset). In a ``@dt``: the **format-3 form**, whose fields were the wall clock and
    ``fold``; readers MUST still accept it, resolving the wall clock with their database, and
    writers MUST NOT write it (a ``@dt`` in a zone is always written in the instant form).

Any other kind of time zone cannot be stored (``ProtoValidationError`` when writing). A
reader whose database does not know ``key`` cannot build the value; ``proto_db`` refuses it with
``ProtoCorruptionError``.

**The class of a built-in subclass is not persisted.** A value of a subclass of a scalar type
(in Python: an ``Enum`` with a ``str`` or ``int`` mixin, a ``StrEnum``, an ``IntEnum``, any
subclass of ``str``, ``int``, ``float``, ``Decimal``, ``bytes``, ``bytearray``, ``date``,
``time``, ``datetime``, ``timedelta`` or ``UUID``) is stored as the plain value of its base
type: its data read with the base type's own methods, never with the subclass's conversions
or accessors (``__str__``, ``__float__``, ``__buffer__``, a ``year`` or ``days`` property, ...;
``proto_db`` reads a date or time type's stored fields through the base type's pickling state).
``Color.RED`` of ``class Color(str, Enum): RED = "red"`` is stored, ordered, hashed and
tokenized as the string ``"red"``, and is read back as ``"red"``. ``proto_db`` converts it when the
value enters a structure (``canonical``), and also converts a lookup's argument (``has``,
``get_at``, ``count_of``) and a raw dict given to the encoder, so the order key, the hash, the
exact token and the bytes always come from one value. A dict whose keys become one base value
(two subclass keys a Python dict tells apart) is refused (``ProtoValidationError``).

**Everything beyond the base value is dropped.** The rule drops the class *and* any state the
subclass adds: extra attributes (a ``str`` subclass with a ``__dict__``), extra fields (the
nanoseconds of a ``datetime`` subclass such as pandas' ``Timestamp``). It covers containers too: a
tuple subclass (a ``namedtuple``) is stored as a tuple, a list subclass as a list, and a dict
subclass (``OrderedDict``, ``defaultdict``) as a map; each is read back as the plain container.
An application that needs the class or the state back stores it explicitly (an atom, or the
value's name).

**Host-language subclasses.** The rule concerns a host-language class that derives from a
class the implementation maps to a format kind without being mapped to one itself. A class that
a format kind names is that kind, not a subclass: in Smalltalk ``Symbol`` derives from
``String`` but is the reserved ``@sym`` kind (see the reserved tags below). Until ``@sym`` is
implemented, an implementation MUST refuse to store a symbol; it MUST NOT store it as its string
(when ``@sym`` lands the two are different keys, and a collection keyed by symbols stored as
strings would stop answering lookups by symbol without an error) and MUST NOT write a tag of its
own. This rule concerns a language's own symbol type; the deferred symbol-reference
optimization of the runtime registry design (spec section 3a, a shared copy of a repeated string,
`Symbol references`_) is unrelated to it.

**One spelling per value.** Every canonical value has exactly one encoding, and readers refuse
the other spellings of the text forms (``@int``, ``@dec``, ``@td``), so decoding and
re-encoding gives back the bytes read. The only accepted non-canonical encodings are msgpack's
own (a wider integer, ``float 32``) and the format-2 and format-3 legacy forms named on this
page, which re-encode canonically.

Every tagged form is a msgpack map with **exactly one** key. A reader refuses with
``ProtoCorruptionError``:

- an unknown tag (``@sym`` among them, see below) or a map with zero or several keys;
- a body of the wrong shape or type: an ``@int`` or ``@dec`` text that is not canonical, a
  ``@td`` that is not normalized, a date or time field out of range, an ``["instant", key]`` with
  ``fold`` 1, a ``@ref`` segment that is not 16 bytes or a string of 32 hexadecimal digits, an
  alias outside the table, a ``@missing`` body other than 0, an empty ``@mut`` name, an unknown
  time zone key;
- a ``@map`` that breaks `Map keys`_;
- a msgpack ``ext`` value (the timestamp extension included) where a value is expected.

The vectors include one input of each kind of refusal (``invalid``).

Value nesting depth
~~~~~~~~~~~~~~~~~~~

The wire form of a canonical value nests at most ``MAX_VALUE_DEPTH = 1022`` MessagePack
containers.

**What is counted.** The *depth* of a wire form is the largest number of msgpack containers
(arrays and maps) on a path from its outermost item down to a scalar or an empty container,
the outermost item included when it is a container, and an empty container counted like any
other. The count starts at the canonical value itself: an attribute value (the value of one entry of a state map ``s``), or a value-layer vector file. The atom
envelope and the state map are not counted; in a record they add two. Each kind adds:

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Kind
     - Containers it adds
   * - nil, bool, native int, float, str, bytes
     - 0
   * - list
     - 1, plus the depth of its deepest item
   * - tuple (``{"@tup": [...]}``)
     - 2, plus the depth of its deepest item
   * - map (``{"@map": [[key, value], ...]}``)
     - 3 (the map, its array, a pair), plus the depth of its deepest key or value; 2 when empty
   * - ``@int``, ``@dec``, ``@uuid``, ``@missing``, ``@mut``
     - 1
   * - ``@date``, ``@td``, ``@ref``; ``@time`` and ``@dt`` whose ``tz`` is nil
     - 2
   * - ``@time`` and ``@dt`` whose ``tz`` is an array (``fixed``, ``instant``, ``zone``)
     - 3

So 1022 lists around ``1`` are exactly at the limit, and so are 1021 lists around an empty
list (``[]`` counts 1, ``{"@tup": []}`` and ``{"@map": []}`` count 2), and a ``@dt`` with a fixed
offset inside 1019 lists. The keys of a map are on its paths too: a map whose key is a tuple
nests that tuple 3 containers below the map's own depth. The count is a property of the wire
form alone: it does not depend on the host language, its stack or its recursion limit.

**Writers and readers.** A writer MUST refuse to store a value deeper than ``MAX_VALUE_DEPTH``,
and a reader MUST refuse data deeper than ``MAX_VALUE_DEPTH``, both at that same number: a value
of depth 1022 round trips, and depth 1023 is refused on both sides. So a reader MUST read a
record payload nested 1024 containers deep: an implementation whose MessagePack layer or whose
walks stop earlier (at 509 levels, say, the figure of msgpack-python before 1.2.0) is not
conforming from ``v2.0.0rc2`` on. ``proto_db`` refuses such a value with ``ProtoValidationError``
naming where it was going (the field of a ``DBObject``, the root, the mutable cell, the element,
key or value of a collection) when the value is validated, and again when a record is encoded,
and refuses such data with ``ProtoCorruptionError``. A record whose payload nests deeper than
1024 containers (one of its values deeper than the limit) is refused before any value is
decoded, because MessagePack's unpacker refuses it.

**Every attribute value, internal nodes included.** The limit applies to every attribute value
of every record, not only to the values an application writes: a node of a collection or of an
index stores a value inside the shapes `Atom types and their persisted fields`_ and
`Index maintenance contract`_ give, and those shapes add containers. The following is
informative, a consequence of
those normative shapes (a writer that applied the limit to the application's value alone would
write records a reader must refuse). The deepest *element* each structure can store:

.. list-table::
   :header-rows: 1
   :widths: 40 30 30

   * - Where the value is stored
     - Containers added
     - Deepest value accepted
   * - a DBObject field, a root, a mutable cell, a List element, a Dictionary key or value, a
       RepeatedKeysDictionary key
     - 0
     - 1022
   * - a Set element (in its bucket tuple)
     - 2
     - 1020
   * - a CountedSet element, a RepeatedKeysDictionary record (``(element, count)`` in a bucket
       tuple); a HashDictionary key or value (``(key, value)`` in a bucket tuple)
     - 4
     - 1018
   * - an element of an indexed collection, orderable (member key ``((0, element), token)``)
     - 4 (6 in a vector index's ``store``, whose key is ``(list number, member key)``)
     - 1018 (1016)
   * - an element of an indexed collection, unorderable (a list, a map, NaN, or a tuple holding
       one): member key ``((1, surrogate), token)``
     - 4, plus the surrogate's: 4 per list level (where the list costs 1), 4 per tuple level
       holding an unorderable value (where it costs 2), 6 per map level (where it costs 3), 2
       around each orderable leaf
     - 254 nested lists, or 169 nested maps, around a native scalar (253 and 169 in a vector
       index's ``store``)

The member key is what binds an indexed collection: an element is a member of every index of
its collection, so a List with an index over its elements stores an element made of nested
lists at most 254 deep, and a List of maps with an index on a field of them stores the maps,
unorderable, through their surrogates. ``proto_db`` refuses an element past these limits when it
is added (``add``, ``set_at``, ``append_last``, ``extend``, ``from_iterable``, and an index added
to a collection that already holds one), with ``ProtoValidationError`` naming the collection's
element, key or value, or the index, and the effective limit; nothing is refused later at
commit. The surrogate's cost per level is the price of the frozen member-key shape
(`Index maintenance contract`_); a flatter key (a non-nesting ``bin`` equality encoding) is
deferred to the next ``FORMAT_VERSION`` (D60).

**Why 1022.** A record payload ``{"t", "oid", "s": {name: value}}`` puts a value two containers
deep, so a value at the limit makes a record exactly 1024 containers deep. 1024 is what the
MessagePack layer supports on both sides, measured with msgpack-python, the MessagePack library
``proto_db`` writes and reads every record with: its unpacker refuses data nested deeper than 1024
containers in every release measured (1.0.8, 1.1.0, 1.1.1, 1.1.2 and 1.2.4: 1024 read, 1025
refused), and its packer writes 1024 since release 1.2.0 (releases up to 1.1.2 stop at 511).
``proto_db`` therefore requires msgpack-python 1.2.0 or later, and refuses to import with an older
one (``ProtoNotSupportedError``). The often-quoted figure of 509 levels is 511 minus the
envelope's 2, the packer limit of msgpack-python before 1.2.0; it is not a limit of the format.
msgpack-python's pure-Python fallback (used where its C extension is
not available) recurses once per container: under Python's default recursion limit it cannot
write or read records near 1024 containers, and fails with ``RecursionError``. ``proto_db`` runs
with the C extension.

**No version change.** The limit adds no ``FORMAT_VERSION``: no conforming writer could produce
deeper data before it. ``proto_db`` has always packed records with msgpack-python, whose packer
refuses more than 1024 containers in every release (more than 511 before 1.2.0), and the
envelope takes two, so no ``proto_db`` record holds a value deeper than 1022. The limit states a
bound every record already met, enforces it before the packer with ``proto_db``'s own errors, and
makes a reader refuse only data no conforming writer wrote. Before this rule ``proto_db`` walked
values by recursion, so how deep it could write or read depended on the Python stack in use
(about 330 nested lists read and 497 written under the default recursion limit, with a bare
``RecursionError`` beyond): values written then are read now at any depth up to the limit.
The vectors hold values at the limit (``values_depth``) and must-reject inputs one level deeper
(``invalid``, ``value-depth-over-*``).

Decimal text
~~~~~~~~~~~~

The ``@dec`` text is the *to-scientific-string* conversion of the General Decimal Arithmetic
specification (Python ``str(Decimal)``). A decimal is a sign ``s`` (0 or 1), a coefficient
``c`` (a string of decimal digits, without leading zeros unless it is ``0``) and an exponent
``e``, or a special value. The text is ``-`` when ``s`` is 1, then:

- **finite**: let ``n`` be the number of digits of ``c`` and ``a = e + n - 1`` (the adjusted
  exponent). If ``e <= 0`` and ``a >= -6``, the plain form: when ``e = 0`` the digits of ``c``;
  otherwise the digits with a decimal point inserted ``-e`` places from the right, padded on
  the left with zeros so that at least one digit precedes the point (``c = 5, e = -3`` gives
  ``0.005``). Otherwise the exponent form: the first digit of ``c``, then ``.`` and the
  remaining digits if ``n > 1``, then ``E``, then ``+`` or ``-``, then ``|a|`` in decimal
  (``c = 15, e = 29`` gives ``1.5E+30``; ``c = 0, e = 2`` gives ``0E+2``; ``c = 1, e = -7`` gives
  ``1E-7``);
- **infinity**: ``Infinity``;
- **quiet NaN**: ``NaN`` followed by its payload digits when the payload is not zero
  (``NaN123``); **signaling NaN**: ``sNaN`` and its payload the same way.

So ``1.20`` (``c = 120, e = -2``), ``-0`` (``s = 1, c = 0, e = 0``) and ``100`` versus ``1E+2``
are different decimals with different texts.

Map keys
~~~~~~~~

The keys of one ``@map`` are pairwise distinct canonical values, and two keys MUST NOT
**conflict**. Two keys conflict when:

- their order keys (`Order keys`_) are equal once every bool is read as the number 0 (false)
  or 1 (true): ``1``, ``1.0``, ``Decimal("1.00")`` and ``true`` conflict pairwise; so do ``0.0``
  and ``-0.0``, two aware datetimes of one instant, and a zone time and the naive time of the
  same wall clock;
- both are NaNs (float or decimal) with the same encoding;
- they are tuples of one length whose items conflict pairwise.

These keys cannot be map keys at all: a list, a map, a signaling decimal NaN, a datetime in
an IANA zone (``["instant", ...]`` or ``["zone", ...]``), and a tuple holding one of them. (A
decoded map is a Python ``dict``, whose equality ignores ``fold`` between two datetimes of one
zone, so whether two zone datetimes are one key would depend on the tz database; their UTC or
fixed-offset form is a valid key.)

A writer refuses such a map (``ProtoValidationError``) and a reader refuses it
(``ProtoCorruptionError``), except the format-3 key forms below: a map is never collapsed. Distinct NaN payloads, and bools in a map
without the numbers 0 and 1, are valid keys.

**Format-3 key forms.** Format 3 had no map-key rule, and Python could write two kinds of keys
the rule forbids: a datetime in an IANA zone (always in the format-3 ``["zone", key]`` wall-clock
form, alone or inside a tuple) and NaN keys with one encoding (a Python dict holds two NaN objects
as two keys). A reader MUST accept both: a key whose wire form holds a ``["zone", key]`` datetime
may be a zone datetime, and NaN keys with one encoding are kept as distinct keys, in the order
read; every other rule applies unchanged (an ``["instant", key]`` key is still refused, since only
format 4 writes that form).

A format-3 zone-datetime key conflicts with another key by the rule Python's dict applied when
format 3 wrote it (PEP 495), resolved with the reader's tz database:

- when its wall clock occurs exactly once in its zone (the offset does not depend on ``fold``),
  it is identified by its instant: it conflicts with every aware datetime key of that instant,
  fixed-offset or zone;
- when its wall clock is ambiguous or skipped in its zone (the offset depends on ``fold``), it
  is identified by its zone and its wall-clock fields without ``fold``: it conflicts only with
  a format-3 key of the same zone and wall clock (whatever its ``fold``).

A reader refuses a map whose keys conflict under this rule (``ProtoCorruptionError``); format 3
never wrote one. A writer MUST NOT write either form: writing such a map again is
refused with ``ProtoValidationError`` naming the key, never normalized or shortened silently.
An application migrates such a map explicitly before it changes the object that holds it,
typically by converting each zone-datetime key to UTC (``key.astimezone(timezone.utc)``, a
valid key of the same instant) and by giving each NaN key a value of its own or dropping it.
The conversion can itself make two keys conflict: an ambiguous zone key and an existing key of
the same instant (which format 3 held as two keys) become one UTC key, and the application
chooses which value to keep. Atoms that are never changed keep their format-3 maps as written.

Reserved tags
    ``@sym`` is reserved for symbols (the maintainer's runtime decision of 2026-11-08, option
    (c2): a symbol is an immutable value identified by its name, ``{"@sym": name}``, compared,
    hashed and ordered by name). **It is not implemented by this revision**: a ``proto_db`` reader
    refuses ``@sym`` as an unknown tag, so an implementation MUST NOT write it until ``proto_db``
    reads it. When it is added it will be an additive change marked by ``FORMAT_VERSION`` 5: a
    new tag, and a new rank after the mutable rank in the order below (`Format stability`_).
    Tags starting with ``@`` are reserved to the format.

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
    zone datetime      81 a3"@dt" 99 cd07ea 0b 01 06 1e 00 00 00 92 a7"instant" b0"America/New_York"
                       (2026-11-01 01:30 in New York, fold 1: the instant 06:30 UTC)
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
     - naive time, and a time in an IANA zone
     - microseconds since midnight of the wall clock. A time has no date, so an IANA zone gives
       it no offset: a ``["zone", key]`` time orders, compares and hashes exactly as the naive
       time of its wall clock (as Python compares it); its zone is kept in the value and in its
       token (D58)
   * - 8
     - fixed-offset time
     - microseconds since midnight minus the offset in microseconds (may be negative)
   * - 9
     - naive datetime
     - the date and time
   * - 10
     - aware datetime
     - the UTC instant in microseconds since 0001-01-01T00:00 (may be negative). For a fixed
       offset: the fields' microseconds minus the offset. For ``["instant", key]``: the fields'
       microseconds, read from the bytes, never from a tz database. For the format-3
       ``["zone", key]`` form: the wall clock minus the offset the reader's database gives it
       with ``fold`` (legacy data only)
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
     ``00`` and 255 is ``ff 00``), preceded by that length as 8 bytes little-endian. Here
     ``bit_length(v)`` is the number of bits of ``|v|`` without its leading zeros (0 for 0), as
     Python's ``int.bit_length``: so -1 is one byte ``ff``, -127 is ``81``, -128 is ``80 ff``.
     (Smalltalk's ``highBit`` of a negative integer differs; use the bit length of the
     absolute value.)

All residues are non-negative: a float or decimal residue is reduced into ``0 .. P - 1``
like an integer's, and the residue of a negative float ``-n/d`` is ``P - (n * d**(P-2) mod P)``
(0 stays 0), as ``(-n * d**(P-2)) mod P`` with a non-negative ``mod``.

Equal keys have equal hashes. The vectors list the hashes of values of every rank (``hashes``)
and of atoms (``atom_hashes``).

Exact tokens
~~~~~~~~~~~~

Key-group and vector indexes keep, beside an element's order key, its **exact token**, which
tells apart values that are equal keys but not identical (``1``, ``1.0`` and
``Decimal("1.00")``; aware datetimes of one instant in different zones). The token is a
function of the canonical encoding alone (decision D58):

    The exact token of a value is the msgpack byte string of its canonical encoding
    (`Canonical values`_: the bytes the value has inside a state map), in which every atom,
    at any depth, is encoded as the placeholder ``{"@ref": nil}`` instead of its reference.
    The token of an atom is therefore the 7 bytes ``81 a4 40 72 65 66 c0``.

It is stored as a msgpack ``bin`` inside the member key. Because the token is computed from the
canonical value, which holds only data of the format (time zone objects reduced to their
form, built-in subclasses converted to their base value as stated in `Canonical values`_), a
value decoded from the wire and the value it was written from have one token. The placeholder
is not the
encoding of any canonical value, so an atom is never confused with a value; atoms themselves
are told apart by the equality part of the member key (their oid).

Examples (bytes in hexadecimal): ``1`` is ``01``; ``1.0`` is ``cb 3ff0000000000000``;
``Decimal("1.00")`` is ``81 a4 40 64 65 63 a4 31 2e 30 30``; ``(atom, 1)`` is
``81 a4 40 74 75 70 92 81 a4 40 72 65 66 c0 01``. The vectors list the tokens of every kind of
value (``exact_tokens``), including float and decimal edge cases, aware times and datetimes.

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

``proto_db.avl.CountedKeyNode``: a KeyNode whose ``value`` is exactly a 2-item tuple (or list)
``(payload, count)`` with an integer ``count >= 0``, plus ``weight``, the sum of ``value[1]`` over
its subtree. Any other value is refused when the node is built (``ProtoValidationError``).

Invariants a writer MUST keep: ``left`` and ``right`` are refs to nodes of the same kind as
the parent, or nil; ``height = 1 + max(height(left), height(right))`` with nil counting 0;
``count = 1 + count(left) + count(right)``; ``|height(left) - height(right)| <= 1`` (AVL); in a
keyed tree every key of ``left`` orders strictly below ``key`` and every key of ``right``
strictly above, by order key; in a positional tree the in-order sequence is the element order
(``count`` gives the positions); ``weight = value[1] + weight(left) + weight(right)``.

Node classes: every node of one tree has the class of its root. A node added to a non-empty
tree takes the class of the tree's root, whatever class the operation asked for; the class an
operation names applies only when it starts an empty tree. So a tree of plain KeyNodes
written before format 3 stays a KeyNode tree when it grows, and the members tree of a
VectorIndex's ``invalid`` group is a KeyNode tree when the index was bulk-built and a
CountedKeyNode tree when it grew from empty; readers MUST accept both.

``proto_db.avl.ValueBox``: ``value``. A node value that is a string of at least 256 code points
(not UTF-8 bytes) or a byte string of at least 256 bytes is stored through a box (D51): the node's ``value`` is then a ref to a
ValueBox whose ``value`` is the string. A reader unboxes every node value that is a ref to a
ValueBox; boxes appear only as node values, never as keys and never elsewhere. Writers SHOULD
box from 256 up (the threshold is a writer's choice that does not change the meaning, but it
changes the bytes).

Collections
~~~~~~~~~~~

Every collection below persists ``count`` and ``indexes`` (a ref to an IndexSet, empty by
default) plus the fields listed for it; all of them are required.

``proto_db.collections.IndexSet``
    Not a collection: it persists only ``root``, a KeyNode tree from index name (str) to a ref
    to the index.

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

An index is part of its collection's state: a writer that changes an indexed collection MUST
update every index of it as below, in the same new version, or Python's index plans will miss
or invent rows (a wrong answer, which ``validate()`` reports as corruption).

Index maintenance contract
^^^^^^^^^^^^^^^^^^^^^^^^^^

**What a collection feeds to its indexes.** Each collection kind feeds *elements* to its
indexes, with a multiplicity:

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Collection
     - Elements fed to its indexes
   * - List
     - every element, once per occurrence (an element held twice is fed twice)
   * - Set
     - every element, once
   * - CountedSet
     - every distinct element, **once** whatever its count: it is fed when its count goes
       from 0 to 1 and removed when it goes back to 0; changing a count above 0 changes nothing
   * - Dictionary, HashDictionary
     - every mapped *value*, once per key that maps to it (never the keys); replacing the value
       of a key removes the old value and adds the new one
   * - RepeatedKeysDictionary
     - every distinct record under each key, once per key it is filed under

**Key extraction.** The key of an element is ``key_spec`` applied to it:

- ``element``: the element itself;
- ``path``: the path is split on ``.``, and each step is applied to the previous result,
  starting from the element; once a step gives MISSING the key is MISSING. One step named ``n``
  applied to a value ``v``:

  - ``v`` a map: the value under the **string** key ``n``, MISSING when there is none (an
    integer key ``1`` is not found by the step ``1``);
  - ``v`` a DBObject (or an application record): its attribute ``n`` when the record *defines*
    it, even as nil (so an attribute set to nil gives nil), MISSING when it does not;
  - ``v`` any other atom: its persisted attribute ``n`` (for example ``count`` of a nested
    List), MISSING when it has none; names starting with ``_`` and ``oid``, ``atom_pointer``
    and ``context`` give MISSING;
  - anything else (a scalar, a list, a tuple): MISSING;
- ``extractor``: the canonical value the registered function returns.

A key is *orderable* when `Order keys`_ accepts it. Lookups (``==``, ``in``, ranges, ``?N``) use
only the order key of the extracted key; the exact token never takes part in a lookup.

**Member keys.** Every stored element is a *member* under its **member key**
``(equality part, exact token)``. The equality part is ``(0, element)`` for an orderable element
and ``(1, surrogate)`` for an unorderable one. The surrogate is ``("l", items)`` for a list,
``("d", pairs)`` for a map, ``("t", items)`` for a tuple holding an unorderable value, ``("n",)``
for a NaN and ``("v", value)`` for any other value, items encoded the same way, and the
``pairs`` of a map are ``(surrogate of key, surrogate of value)`` sorted by the order key of the
surrogate of the key. The exact token is `Exact tokens`_ (a ``bin``). A member's entry holds the
element as written and its count of occurrences.

**Adding** an occurrence of an element: compute its key and member key; when the member exists
its count grows by one, otherwise a member with count 1 is created (holding the added element).

**Removing** an occurrence of an element: compute its key and member key (the element as the
collection holds it, so as it was decoded). The member with exactly that member key loses one
occurrence, and is deleted at 0. When no member has that member key the index does not hold
the element, which is an error (``ProtoValidationError``): a writer MUST NOT remove another
member instead, not even an equal one. Because the element is stored as its canonical value
and the token is that value's encoding, an element read from the database has the member key
it was added under.

``validate()`` checks exactly this contract: every member is stored under its own complete
member key (equality part and token) and under its element's own key, counts are positive,
totals add up, and the multiset of ``(key, element, token)`` equals that of the collection's
elements. On a format-3 index (below) it checks the same, except that the token of a member key
is only checked to be a tuple: such an index is stale, never corrupt.

``member_tokens`` (int). A format-4 writer MUST write it, with the value 1, on every key-group
and vector index: its tokens are canonical encodings. Its **absence** marks an index written by
format 3, whose tokens were Python-text tuples (``("i", 1)``, ``("f", "0x1.8p+0")``,
``("T", (...))``, ...): readers answer queries from it (lookups do not use tokens), but a
writer MUST rebuild it from the collection's elements (``empty`` index of the same definition,
then every element added) before changing it. ``proto_db`` does so at the collection's first
change, inside that change's transaction: a full rebuild, which for an ivf_flat index includes
a training run and for an hnsw index the graph's construction. Any **other value** is
reserved for a later format: a reader that does not know it MUST treat the index as one it
cannot maintain (it may answer queries from it; it MUST NOT change it, and ``proto_db`` rebuilds
it like a format-3 index).

Key-group indexes
^^^^^^^^^^^^^^^^^

``proto_db.indexing.EqualityIndex`` and ``proto_db.indexing.RangeIndex``
    ``groups``: a CountedKeyNode tree ordered by the extracted key, whose values are *groups*
    ``(members, total)``; ``side``: the group of the elements whose key is unorderable (a
    tuple, ``(nil, 0)`` when empty); ``total_count``: every stored occurrence; ``member_tokens``
    (written by format 4, absent from format-3 indexes, see above). ``members`` is a CountedKeyNode tree mapping a member key to
    ``(element, count)``, and ``total`` the occurrences in the group. An element whose key is
    MISSING is filed under the key MISSING. A key with no member left is removed from
    ``groups``. The two classes differ only in the plans they offer.

    Indexes written before format 3's CountedKeyNode (plain KeyNode trees, no ``weight``) are
    still read and answer every query; they offer no rank access until rebuilt (D56).

Vector indexes
^^^^^^^^^^^^^^

``proto_db.indexing.VectorIndex``
    ``dim`` (int >= 1), ``metric`` (``"cosine"``, ``"l2"`` or ``"ip"``), ``backend``
    (``"exact"``, ``"ivf_flat"`` or ``"hnsw"``), ``params`` (the complete backend parameters as
    a tuple of ``(name, value)`` pairs sorted by name: exact ``numpy``; ivf_flat ``nlist``,
    ``nprobe``, ``retrain_growth``, ``seed``, ``train_iters``; hnsw ``M``, ``ef_construction``,
    ``ef_search``, ``seed``), ``store`` (a KeyNode tree mapping ``(list number, member key)`` to
    ``(element, count, vector)``), ``invalid`` (the group of the elements without a valid
    vector, as a key-group index's group), ``total_count``, ``centroids`` (a tuple of vectors,
    empty unless trained), ``trained_size`` (the number of members at the last training, 0
    when untrained) and ``member_tokens`` (written by format 4, absent from format-3 indexes,
    see above). ``numpy`` and the hnsw graph are
    Python implementation choices (the graph is rebuilt in each process and never persisted);
    another implementation MUST preserve ``params`` as written.

    **Vectors.** The key of an element is a valid vector when it is a tuple or list of exactly
    ``dim`` numbers (int, float or decimal; a bool is not a number here), each converted to the
    nearest float 64 (an int or decimal too large for a float is invalid), every one finite,
    and the sum of their squares, computed as a correctly rounded sum, finite. The stored
    ``vector`` is that tuple of floats. An element whose key is not a valid vector (MISSING and
    nil included) goes to ``invalid``.

    **Scores.** ``sum`` below is the correctly rounded sum of the exact products (Python
    ``math.fsum``, Shewchuk's algorithm: independent of the order of the terms), and ``sqrt`` is
    IEEE 754's correctly rounded square root. ``ip``: ``sum(q_i * v_i)``. ``l2``: ``-sqrt(sum((q_i -
    v_i)**2))`` (the sum taken as +infinity when it overflows). ``cosine``: 0.0 when ``|q|`` or
    ``|v|`` is 0.0, else ``(sum(q_i * v_i) / |q|) / |v|`` with ``|x| = sqrt(sum(x_i * x_i))``, divided
    in that order. Each product and difference is the IEEE float 64 operation.

    **List numbers.** The backends ``exact`` and ``hnsw`` put every member in list 0. An
    ``ivf_flat`` index puts every member in list -1 while it is untrained (``centroids`` empty);
    when it is trained, a member's list is the index ``j`` of the centroid with the highest
    score for the member's vector, where the score is the ``cosine`` score for a cosine index
    and the ``l2`` score for an ``l2`` or ``ip`` index, and a tie goes to the lowest ``j`` (the
    first maximum). A writer MUST file every member under that list, and MUST recompute it
    whenever ``centroids`` change: removal and ``validate()`` look a member up only in the list
    its vector belongs to.

    **Training** (writer's choice, not needed to read). ``proto_db`` trains when the index first
    holds at least ``nlist`` members, and again when the number of members reaches
    ``retrain_growth`` times ``trained_size``; training computes ``nlist`` centroids (k-means from
    ``seed``, normalized for cosine), sets ``trained_size`` to the number of members and moves
    every member to its list. Another writer MAY train at other times or compute other
    centroids, provided ``centroids`` holds exactly ``nlist`` valid vectors of ``dim`` floats when
    it is not empty, ``trained_size`` is not 0 exactly when ``centroids`` is not empty, and every
    member is in its list. Python's own training may give slightly different centroids with
    and without numpy; that changes no answer's correctness, only which list holds a member.

Records
~~~~~~~

``proto_db.core.dbobject.DBObject``
    An immutable record: its state map is the record's attributes, under the attribute-name
    rules of `Atom records`_ (``with_`` and ``has_defined_attr`` are reserved). No field is
    required. Reading an absent attribute gives nil; an index path step tells an absent
    attribute (MISSING) from one set to nil.

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

Registry roots
~~~~~~~~~~~~~~

The runtime registry's state (the per-database class registry of `Format stability`_) is two
roots of a database's ``objects``. Their records are of the built-in types below, which use the
format-5 feature ``registry``: a record whose type tag is alias 22 or 23 (or either name) is
version 5, so it lands in a segment of version 5 or later. Both types carry no parents (no
``p``).

``proto_db.registry.ModuleList``, the value of the root ``__currentModules``
    ``modules``: a ``@tup`` of 2-item ``@tup`` values ``(name, depends)``, one per installed
    module, in installation order. ``name`` is the module name (a non-empty string, language
    neutral); ``depends`` a ``@tup`` of the module names it declared as dependencies when it was
    installed (non-empty strings; a record of the install: at load time a runtime follows its
    code's declaration). A name appears once. Anything else is ``ProtoCorruptionError``.

    **The module** ``base``. ``base`` is the module of the default mechanisms, which every
    runtime provides (its definitions are each language's own); it has no dependencies. A writer
    **SHOULD** put ``base`` first in a non-empty ``modules`` (``proto_db`` always does, adding it
    with an empty ``depends`` before the first module it installs). A reader **MUST** treat
    ``base`` as implicitly first when a non-empty ``modules`` does not start with it: as if
    ``(base, ())`` preceded the list when ``base`` is absent, or as if its entry were moved first
    when it is listed later. An empty ``modules`` lists nothing, ``base`` included.

    **The load order.** The order of ``modules`` is the install history only: a runtime **MUST
    NOT** let it decide how the registry is composed. A runtime **MUST** load the listed modules
    in this order, a function of the set of listed names and of the dependencies their code
    declares at load time: ``base`` first (whatever its name compares to), then, repeatedly,
    among the modules not yet placed whose dependencies are all placed, the one whose name is
    smallest in **code-point order** (the names compared as sequences of Unicode code points,
    the first differing code point deciding and a proper prefix coming first; no locale, case
    folding or normalization; UTF-8 byte order agrees with it, UTF-16 code-unit order does not).
    A dependency cycle, or a dependency that is not listed, is an error; the dependencies are
    the ones the module's code declares when it is loaded. Everything the load order decides follows it: a registry
    name's extensions are chained newest first (the module loaded last first), and its parents
    are the union of the ``parents=`` its extensions declare, in load order. Two databases that
    list the same modules in different orders therefore compose the same registry and resolve
    every name the same way. A writer **MUST NOT** reorder ``modules`` into the load order (the
    list is only appended to, see below).

``proto_db.registry.Catalog``, the value of the root ``__catalog``
    ``entries``: a ref to a Dictionary from registry name (a non-empty string) to a ref to its
    **catalog object**: a ``proto_db.core.dbobject.DBObject`` record whose parents include that
    registry name (``proto_db`` writes exactly the one-name tuple). An entry of another type, or
    whose parents lack its name, is ``ProtoCorruptionError``.

A writer changes ``modules`` only by **appending** a module that is absent, and never changes
nor removes a listed one, with one exception: a writer that appends to a list that does not start
with ``base`` writes it with ``base`` first, moved there or added (``proto_db`` repairs such a
list on its next install). An install changes ``entries`` only by adding a registry name that is
absent. An existing entry is **never removed**, and it is replaced only by a new version of that
catalog object: a record of the same type whose parents still include the name (the catalog
object's data is application data; a version is a new record, with a new ``oid``).
``proto_db`` merges installs against the root current at commit (add-if-absent), so concurrent
installs of different modules all land, in commit order, and replaces an entry only when it is
still the record the replacing transaction read (a concurrent replacement conflicts). Those
rules are behaviour, specified by the runtime registry, and leave no trace in the bytes.

To read a database: decode ``space.root``, load its SpaceRoot, look the database's name up in
``databases``, then read ``objects`` and ``mutables`` of that DatabaseRoot. History is the chain
of ``previous`` links; every version stays readable (nothing is ever deleted).

To commit a new version a writer appends the new atoms (children before the atoms that refer
to them), then a new DatabaseRoot (``version`` + 1, ``previous`` the version it was built on,
``origin`` carried, ``committed_at`` now in UTC), a new ``databases`` Dictionary and a new
SpaceRoot, and publishes the SpaceRoot's pointer as the new root (`Storage layouts`_). Conflict
detection between concurrent writers (read sets, merges of concurrently written collections)
is the library's job and leaves no trace in the format, except the epoch check of the publish.


Semantic catalog (format 6)
---------------------------

**Not yet written by any release.** This chapter is normative and specifies the semantic model of
decisions D63 to D72. Its status is *development branch; no release yet*: the repository's
development branch is the first writer, and no tagged release has written format 6. A reader
that implements it **MUST** read what this chapter specifies, and a writer **MUST** write it
exactly. Data that uses none of it stays version 4 or 5, byte for byte (S5).

What is normative is what lives in the database: the catalog and its canonical form, the
expression language and the intent log. The declaration syntax a language offers (Python's
``Field``, ``Relation`` and ``@intent``, Smalltalk's own) is not part of this chapter: each
language declares the same thing its own way and **MUST** produce the same catalog. A feature
that cannot be described as neutral data does not enter the catalog; it stays a local
convenience of one language and is catalogued as such (``languages``, below).

The feature and the types
~~~~~~~~~~~~~~~~~~~~~~~~~

The format-6 feature is ``catalog``. A record whose type tag is one of the built-in types below
(alias or name) uses it, is version 6, and so lands in a segment of version 6 or later; the
lowest-sufficient-version rule of `Format stability`_ applies. The types are appended to the
alias table in this order, from alias 24 (the table is append-only): 24
``proto_db.catalog.SemanticCatalog``, 25 ``Module``, 26 ``Extension``, 27 ``Entity``, 28
``Relationship``, 29 ``Attribute``, 30 ``Role``, 31 ``Intent``, 32 ``Constraint``, 33
``Expression`` and 34 ``IntentRecord`` (each in the package ``proto_db.catalog``). None carries
parents (no ``p``).

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Type
     - Fields
   * - ``proto_db.catalog.SemanticCatalog``
     - The value of the root ``__semanticCatalog``. ``modules``: a ref to a Dictionary from module
       name to Module. ``module_set``: a ``@tup`` of the module names in code-point order
       (`Registry roots`_).
       ``entities``: a ref to a Dictionary from type name to Entity or Relationship, the
       composition for ``module_set``.
   * - ``proto_db.catalog.Module``
     - ``name``; ``version``, a string (the first writer writes ``"1"``); ``depends``, a ``@tup``
       of module names; ``extensions``, a ``@tup`` of refs to Extension records in declaration
       order.
   * - ``proto_db.catalog.Extension``
     - ``module``; ``extends``, a type name; ``entity``, a map of the entity-level elements it
       contributes; ``elements``, a ``@tup`` of refs to the Attribute, Role, Intent and
       Constraint records it declares, with the fields it states.
   * - ``proto_db.catalog.Entity``
     - ``type_name``; ``supertypes``; ``meaning``; ``label``; ``help``; ``synonyms``; ``key``;
       ``attributes`` (refs to Attribute and Role records); ``intents``; ``constraints``;
       ``certification``; ``provenance``.
   * - ``proto_db.catalog.Relationship``
     - The Entity fields plus ``roles``: refs to its Role records, two or more.
   * - ``proto_db.catalog.Attribute``
     - ``name``; ``inherited_from``; ``kind`` (``field``, ``computed`` or ``computed_stored``);
       ``value_type``; ``values``; ``required``; ``default``; ``unit``; ``indexed``;
       ``expression`` (a ref to an Expression); ``languages``; ``meaning``; ``label``; ``help``;
       ``group``; ``order``; ``synonyms``; ``examples``; ``caveats``; ``sensitivity``;
       ``certification``; ``deprecated``; ``provenance``.
   * - ``proto_db.catalog.Role``
     - The Attribute fields plus ``target``, ``inverse``, ``cardinality``,
       ``inverse_cardinality``, ``ordered``, ``index_by``, ``binding`` and ``on_delete``.
   * - ``proto_db.catalog.Intent``
     - ``name``; ``meaning``; ``parameters``, a ``@tup`` of ``(name, value_type)`` pairs;
       ``requires`` (a ref to an Expression); ``commutes_with``; ``effects``; ``languages``;
       ``provenance``.
   * - ``proto_db.catalog.Constraint``
     - ``name``; ``scope`` (``attribute``, ``entity``, ``relation`` or ``aggregate``);
       ``expression`` (a ref to an Expression); ``enforced``; ``meaning``; ``message``;
       ``provenance``.
   * - ``proto_db.catalog.Expression``
     - ``ast``: the tree of `The expression language`_.
   * - ``proto_db.catalog.IntentRecord``
     - ``intent``, the pair ``(type name, intent name)``; ``target``, a handle; ``arguments``, a
       map; ``transaction``, the version number of the database version the commit produced.

Field rules:

- ``values`` (an enumeration of the permitted values of a ``str`` attribute), ``languages``,
  ``inverse_cardinality`` and ``inherited_from`` are part of the format. ``inherited_from``
  appears only on the elements of a composed subtype entity and names the type that declared the
  element. ``values`` is catalogued and not enforced by the first writer; the catalog is how a
  reader learns that.
- ``label`` and ``help`` are maps from a language tag to text. A declaration that gives a plain
  string is stored as ``{"und": text}`` (BCP 47 *undetermined*); a declaration that gives a map
  states its languages. Translations are not specified further.
- ``value_type`` names a canonical value kind: ``bool``, ``int``, ``decimal``, ``str``, ``bytes``,
  ``date``, ``time``, ``datetime``, ``timedelta``, ``uuid``, ``float``, ``list``, ``tuple`` or
  ``map``. A language maps its own types onto them (Smalltalk's ``#integer`` and ``#decimal`` are
  ``int`` and ``decimal``).
- **``ext``.** Every record **MAY** carry ``ext``, a map from string to value. A reader keeps it
  byte for byte, re-writes it unchanged, and never interprets it.
- **Single-language elements.** An intent or a computed attribute implemented in one language
  only carries ``languages``, a ``@tup`` of language names. Invoking or evaluating it from another
  language **MUST** raise ``ProtoNotSupportedError``; describing it is allowed.
- **Roots by name and type.** ``__semanticCatalog`` and ``__intentLog`` are reserved root names
  (`Space and database roots`_). ``__semanticCatalog`` is identified by name and by type: a value
  that is not a SemanticCatalog is ``ProtoCorruptionError``.

**Field encodings** (the same in the design spec, section 8). A field is **required** where listed under "Required fields" below;
every other listed field is **optional**, and absent means its default (below) or, where none is
given, "not declared". The value kinds are those of the format (``str``, ``bool``, ``int``, ``@tup``, ``@map``,
refs):

- Text fields (``meaning``, ``message``, ``group``, ``unit``, ``certification``, ``caveats``, ``on_delete``,
  ``sensitivity``, ``scope``, ``kind``, ``binding``, ``value_type``, ``type_name``, ``name``, ``module``, ``extends``,
  ``version``, ``inverse``, ``inherited_from``, ``target``) are ``str``.

- Lists of names are a ``@tup`` of ``str``, in declaration order: ``supertypes`` (the direct supertypes, in the
  language's base order), ``key`` (attribute names), ``synonyms``, ``depends``, ``commutes_with`` (intent names),
  ``index_by`` (attribute paths, dotted), ``values`` (the enumeration), ``languages`` (language names, non-empty
  when present), ``effects`` (free text, informative) and ``provenance`` (module names: the module that defined
  the element first, then those that modified it, in load order). ``module_set`` is a ``@tup`` of ``str`` in
  code-point order.

- Lists of elements are a ``@tup`` of refs: ``attributes`` (Attribute and Role records), ``intents``,
  ``constraints``, ``roles``, ``extensions``, ``elements``. A composed list keeps the position of an element's first
  definition, and an element added by a later module goes after the existing ones, in load order.
  ``modules`` and ``entities`` are refs to Dictionaries from ``str`` to a ref (module name to Module, type name to
  Entity or Relationship).

- ``label`` and ``help`` are a ``@map`` from a language tag to ``str`` (a plain string is ``{"und": text}``).

- ``examples`` is a ``@tup`` of canonical values. ``default`` is a canonical value; its absence means no default
  is declared (a declared default of nil is present as nil). ``order`` is an ``int``. ``parameters`` is a ``@tup``
  of ``(name, value_type)`` pairs, each a ``@tup`` of two ``str``.

- ``required``, ``indexed``, ``ordered``, ``enforced`` and ``deprecated`` are ``bool``.

- ``cardinality`` and ``inverse_cardinality`` are the two ends of the relation as ``str``, one of ``one``, ``many``,
  ``0..1`` and ``1..*``: ``cardinality`` is the end at the role's own entity, ``inverse_cardinality`` the end at its
  ``target``. A declaration written as a pair (``many-to-one``, ``one-to-many``, ``one-to-one``, ``many-to-many``)
  is stored as these two. ``binding`` is ``identity`` or ``version``. ``kind`` is ``field``, ``computed`` or
  ``computed_stored``. ``scope`` is ``attribute``, ``entity``, ``relation`` or ``aggregate``.

- ``expression`` and ``requires`` are refs to an Expression record; ``ast`` is the tree of the expression
  language, nested ``list`` values whose operator and names are ``str``.

- ``Extension.entity`` is a ``@map`` whose keys are among ``meaning``, ``label``, ``help``, ``synonyms``, ``key``,
  ``supertypes`` and ``certification``, each with the encoding of the Entity field of that name.

- ``IntentRecord.intent`` is a ``@tup`` of two ``str``; ``target`` is the ``str`` name of the target's cell
  (``<type name>#<32 hex digits>``); ``arguments`` is a ``@map`` from parameter name to canonical value (an entity
  argument is carried as its cell name); ``transaction`` is an ``int``.

- **Required fields.** ``SemanticCatalog``: ``modules``, ``module_set``, ``entities``. ``Module``: ``name``, ``version``.
  ``Extension``: ``module``, ``extends``. ``Entity`` and ``Relationship``: ``type_name`` (and ``roles`` for a
  Relationship). ``Attribute``: ``name``, ``kind``. ``Role``: ``name``, ``target``, ``cardinality``. ``Intent``: ``name``.
  ``Constraint``: ``name``, ``scope``, ``expression``. ``Expression``: ``ast``. ``IntentRecord``: all four.

- A Role carries the Attribute fields except ``kind``, ``value_type``, ``values``, ``default``, ``unit`` and
  ``expression``: a role is stored and holds entities, not a value of a kind.

- **Omission.** Defaults are omitted, in the records and in the canonical form: absent text, an empty ``@tup``
  or ``@map``, ``required`` false, ``indexed`` false, ``enforced`` true, ``sensitivity`` ``internal``, ``binding``
  ``identity``, ``ordered`` false and ``deprecated`` false. A writer writes no field at its default; a reader
  that meets one accepts it and its canonical form omits it. Every other optional field is written when it is
  declared.

Composition and the canonical form
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The composed ``entities`` are a cache. A reader **MAY** use them when ``module_set`` equals the
set of module names of ``__currentModules`` (`Registry roots`_) and **MUST** otherwise compose
from the modules' contributions. A runtime that has the modules' code recomposes from it in the
registry's load order and verifies that the result equals the stored catalog; a difference is an
explicit error, never corrected silently. Contributions fold in the load order: the last module
that defines an element wins. Every composed element lists, in ``provenance``, the module that
defined it and then the modules that modified it, in load order. Nothing is deleted: a module
never removes another's element, it **MAY** mark it ``deprecated``, and a module is never
uninstalled.

A module **MAY** freely redefine ``label``, ``help``, ``group``, ``meaning``, ``synonyms``,
``caveats``, ``examples``, ``default``, ``sensitivity``, ``deprecated``, the expression of a
computed attribute, the ``enforced`` of a constraint and the implementation of an intent. It
**MAY** add attributes, roles, intents and constraints. It **MUST NOT** change, without a
declared migration (which this chapter does not specify, so always), the ``kind`` of an
attribute, the ``value_type`` of a stored attribute, or the ``cardinality``, ``target`` or
``binding`` of a role. A redefinition that restates a structural field **MUST** equal the
composed one. A subtype entity (``supertypes`` is not empty) is composed as the fold of its
registry linearization from the last to the first; its inherited elements carry
``inherited_from``, and it **MUST NOT** change what a module may not change (the same list),
because a role, query, aggregate or invariant typed by the supertype reads and writes subtype
instances through the supertype's declaration.

**The canonical form** of a catalog is the composed catalog expanded into one value: every ref
replaced by the fields of the record it names, oids and pointers dropped, every Dictionary a
map sorted by key, the whole encoded canonically (`Canonical values`_). Two implementations
conform when their canonical forms are equal byte for byte. Record bytes are **not** compared:
oids and placement differ by writer.

The expression language
~~~~~~~~~~~~~~~~~~~~~~~

An expression is a nested list ``[operator, argument...]``, stored as data in an Expression
record's ``ast`` and evaluated the same way in every language. Its operators:

``["const", v]``
    A canonical value.

``["attr", name, ...]``
    A path from the current element. Each step but the last is a ``one`` role or relation; an
    absent step gives MISSING. The last step reads the element's stored own attribute, then the
    field's catalogued ``default`` when it has one, else MISSING. It never yields a language's
    own ``None`` for an absent value.

Arithmetic ``["+", a, b]``, ``["-", a, b]``, ``["*", a, b]``
    ``int`` with ``int`` gives ``int``; when either operand is a ``decimal`` the result is a
    ``decimal`` (exact). MISSING in gives MISSING. A ``bool``, a ``float``, a string or any other
    kind raises ``ProtoValidationError``. Division is not specified.

Comparison ``["==", a, b]`` and ``"!="``, ``"<"``, ``"<="``, ``">"``, ``">="``
    The semantics of `Order, equality and hashing`_ (equality, type ranks, order keys). A
    comparison that involves MISSING is false.

Logic ``["and", ...]``, ``["or", ...]``, ``["not", a]``
    Operands are booleans, evaluated left to right, ``and`` stopping at the first false and ``or`` at the first
    true. A MISSING operand counts as false (as in a comparison), so ``not`` of MISSING is true. Any other
    non-boolean operand that is reached raises ``ProtoValidationError``.

Aggregates ``["sum", over, term, where?]`` and ``["count", over, where?]``
    ``over`` is a role name of the current element or a ``via`` expression. ``term`` and
    ``where`` are evaluated with each member as the current element. ``sum`` of no members is the int 0 (and ``count`` of none the int 0); otherwise the terms are added as ``+`` does.
    A member whose ``term`` is MISSING contributes nothing, and a member for which ``where`` is not true is not a
    member of the aggregate.

Traversal ``["via", origin, role, role...]``
    The set reached through each end's own collection. The origin ``["self"]`` is the current
    element; ``["root", name]`` and ``["cell", name]`` are valid in queries only.

Reserved, not specified by this revision: ``in``, ``is_missing``, ``min``, ``max``, ``exists``,
``all``, ``reach`` and ``valid_at``. A reader that meets an operator it does not implement
**MUST** raise ``ProtoNotSupportedError``. A text form (``"committed <= credit_limit"``) is a
convenience of one language that is parsed into this tree; it is never stored.

The intent log
~~~~~~~~~~~~~~

``__intentLog`` is a root of a database's ``objects`` whose value is a ``List`` of IntentRecord
records: those of the commit that wrote this version of the database, in the order the operations
ran, and nothing older (history is the chain of versions, `Space and database roots`_). A value
that is not a ``List``, or an element that is not an IntentRecord, is ``ProtoCorruptionError``
when read. ``IntentRecord.transaction`` is the ``version`` of the DatabaseRoot the commit
produced. A database that never used intents has no ``__intentLog``.


Framed log files
----------------

Every append-only log of a node (``roots.log``, ``ranges.log``, ``segments.log``, a shared
storage's ``cluster/<node_id>.roots``) has this layout.

Header (14 bytes): magic ``PDB2LOG\0`` (8 bytes), the log format version (``u16``, 2 or 3), and
the kind (4 ASCII bytes: ``ROOT``, ``RANG`` or ``SEGS``). A reader checks all three: a header that
is only a prefix of the expected one (possibly followed by zero bytes, and nothing else) is a new
log; any other header is ``ProtoCorruptionError``, a version it does not read
``ProtoNotSupportedError``. Version 3 differs from 2 only in that an acceptor log may hold
versioned entries (kind 7); a writer writes 2 for every other log, and raises the header of an
acceptor log to 3 in place, ``fsync``-ed, before it appends the log's first versioned entry (a
compaction writes 3 when it keeps one). A reader of version 2 therefore refuses such a log at
its header.

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
    start (u64) | end (u64)``: the byte ranges of the records shipped with that commit. When two
    entries name the same root (segment and offset), the later one replaces the earlier. A
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
       bytes each, as in ``ranges.log``, each with ``start <= end``), then the record items as in
       kind 5. Flag bit 0 (``ACCEPTED``) marks an accepted root; without it the entry only makes
       the records durable on a data replica. Other flag bits, and a range with ``start >
       end``, are ``ProtoCorruptionError``.
   * - 7 versioned
     - 3 + entry
     - ``kind (u8) | storage format version (u16) |`` the payload of an entry of kind 1, 2, 5 or
       6: that entry, whose roots (root and ``pred``) are of the given storage format version
       (above 4). An entry whose root, ``pred`` or records need a storage format above 4 is
       written in this form, every other entry in its plain form; a committer gives its root at
       least the version of every record of its commit (`Format stability`_). A version of 4 or
       less, a wrapped kind other than 1, 2, 5 or 6, or an entry of 3 bytes or fewer is
       ``ProtoCorruptionError``; a version the reader does not read is
       ``ProtoNotSupportedError``. A reader that predates kind 7 refuses it as an unknown kind,
       so it never reads a format-5 root or record as format 4.

``seq`` is the write permission the root was committed under (0 under a plain lease); ``pred``
is the root it was built on, all zero when unknown (``pred`` label 0 and epoch 0). Kinds 1 to 4
are exactly 41 bytes; another size, an unknown kind, a kind-5 or kind-6 entry cut inside an
item or with trailing bytes is ``ProtoCorruptionError``. Every promise, accepted root and
rejoined marker is ``fsync``-ed before the message it justifies is sent, whatever the
storage's durability mode. A reader takes the highest accepted and committed roots in the
order ``(label, epoch, segment bytes, offset)``, the highest promise, and ``top``, the highest
label or term of any entry other than kind 4. Kinds 1 and 5, and kind 6 with ``ACCEPTED``,
count as accepted roots; kind 6 without it counts as no root (only its records are redone).
Two entries whose roots differ but share ``(label, epoch)`` are ``ProtoCorruptionError`` (the
protocol makes that impossible, so it is reported, never broken as a tie). A log without a
kind-4 entry is *empty*: the node must rejoin before acting as an acceptor. Kind-5 and kind-6
records are *redone* (written at their offsets) when the storage opens; a compaction
summarizes entries a checkpoint covered (implementation-specific timing).


Cluster wire protocol
---------------------

Nodes exchange **frames** over TCP: a 4-byte **big-endian** length, then that many bytes of
msgpack. A node never stores a record larger than 16 MiB (``MAX_RECORD_BYTES``), and a
replication splits its records into chunks of at most 16 MiB (``CHUNK_BYTES``). There is no
handshake: a connection carries frames from its first byte.

**Connections are one-way.** Every node listens on its address and opens one outbound
connection to each peer's listening address. A node *sends* only on its own outbound
connections and *reads* only from connections it accepted: it never reads from a socket it
opened and never writes to a socket it accepted. A reply therefore travels on the replier's
own outbound connection to the asker's listening address; an implementation that answers on
the inbound socket is never heard. A sender that cannot connect drops the message (the
protocol tolerates loss) and retries the connection after a back-off.

**Errors drop the connection.** The receiver of a connection drops the whole connection, and
every message of it not yet delivered, when it reads a malformed frame: a length over 64 MiB
(``MAX_FRAME_BYTES``), a body that is not one msgpack map, or **any invalid message**: a map
without ``v``, ``type`` or ``from``, another version, an unknown type, a missing or ill-typed
field, a map key that is not a string. The frames are decoded per read of the socket (up to 64
KiB), and the messages already decoded from the same read as the bad frame are lost with it,
not only the bad one. The sender sees a closed socket and reconnects on its next message.
Extra fields are ignored.

Every message is a msgpack map with ``v`` (the protocol version, 13), ``type`` and ``from``
(the sender's node id, a string), then the fields of its type, written in the order of the
table.

Field types:

- *int*: a non-negative integer; *number*: a non-negative integer or float; *str*, *bool*;
- *uuid*: 16 bytes; *optional bytes* / *optional str*: the type or nil;
- *root*: nil (unknown), ``[0, nil, nil, label]`` (the empty space, known) or ``[epoch, segment
  16 bytes, offset, label]``; a 3-item form without the label is read with label 0; a root of a
  storage format above 4 adds the version as a fifth item, ``[epoch, segment, offset, label,
  version]`` (a version of 4 or less there, or one the reader does not read, is refused as a
  malformed message);
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
     - rid, inc; segment (uuid); offset; data (optional bytes: a framed record); optional
       ``unknown`` (bool, absent reads as false)
   * - ``commit_query``
     - rid, inc; root
   * - ``commit_info``
     - rid, inc; persisted (bool); ranges; optional ``unknown`` (bool, absent reads as false)

An optional field may be absent; it is checked when present, and a receiver ignores a key it
does not know. ``unknown`` is the only optional field of protocol 14. A member answers
``fetched`` or ``commit_info`` with ``unknown`` true when it cannot tell whether it holds what
was asked: it has no storage attached, its space closed or detached while it served the
request, or (``commit_info`` only) it is still attaching its storage. The asker counts such a
member as heard, but never as one that "does not have it": a record is reported absent only
when every member answered that it lacks it, so an answer with ``unknown`` makes the record
unavailable, not corrupt. A member that sends ``data: None`` without ``unknown`` says it
lacks the record.

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

The meaning of each message, and the rules that make the protocol safe, are those of
:doc:`coordinator-protocol` and of decisions D44 to D55; the field list above is complete for
protocol 14. The vectors hold one frame of every type.


A minimal external writer (file storage)
----------------------------------------

A writer that implements neither the merge of concurrent writers nor the cluster protocol can
still share a file-storage directory with ``proto_db`` processes. It commits by optimistic
retry: when the published root moved since it read it, it **re-reads the root and redoes its
commit**; it never publishes over a root it did not build on.

Before taking the lock, the writer:

1. writes its records into a segment it created itself (``O_CREAT | O_EXCL``, a fresh UUID4 name,
   a format-4 header), never into another process's segment, children before the atoms that
   refer to them;
2. flushes and ``fsync``-s that segment, and ``fsync``-s the directory once after creating the
   segment, so that every record the new root references, and the segment's directory entry,
   are on disk before the root is (POSIX does not order a new file's directory entry before a
   later rename).

Then, to publish, it MUST, in this order:

3. take the exclusive ``flock`` on ``space.lock`` (``msvcrt.locking`` of the first byte on
   Windows), retrying until a timeout; on timeout it gives up without publishing;
4. read ``space.root`` and check its CRC, its fields and its ``version`` (a version it does not
   know is an error, never a reason to overwrite);
5. compare the root's ``epoch`` with the epoch of the root its transaction started from; when they
   differ, go to step 9;
6. check that the directory has no ``cluster/`` subdirectory and that the SpaceRoot the root
   points to has no ``cluster_term`` (absent or nil): a space shared by a cluster, even one that
   has not persisted a root yet, is written only through the cluster, so the writer stops with
   an error;
7. write the new root record (``epoch`` + 1, its SpaceRoot's segment and offset, ``version`` 4) to
   ``space.root.tmp``, ``fsync`` it, rename it over ``space.root``, and ``fsync`` the directory;
8. release the lock. The commit is durable once step 7 returned.

9. On an epoch mismatch it releases the lock and redoes the commit on the root it just read:
   it loads the new SpaceRoot and its database's current DatabaseRoot, checks that every root
   name and mutable cell its transaction read still holds the same value (the same pointer, or
   the same canonical bytes for a plain value) and that every name it found **absent** is still
   absent, and either aborts (something it read changed) or writes again: its changed root
   values; new ``objects`` and ``mutables`` Dictionaries built from the current DatabaseRoot's,
   with only its own entries replaced, added or removed; a new DatabaseRoot whose ``previous``
   is the current one and whose ``version`` is that one's plus one (``origin`` carried); a new
   ``databases`` Dictionary built from the current one with only its own database's entry
   replaced; and a new SpaceRoot. Then it publishes from step 3 with the new epoch. Records of
   the abandoned attempt stay in its segment, unreferenced; they are harmless.

When there is no ``space.root`` yet, the expected epoch is "none": the writer creates the first
SpaceRoot, publishes it with ``epoch`` 1 in step 7, and treats a ``space.root`` that appeared
meanwhile as an epoch mismatch (step 9, with nothing read).

A minimal writer MUST NOT change a collection that has indexes unless it maintains every one of
them exactly as `Indexes`_ specifies (member keys, exact tokens, ``member_tokens``); in
particular it MUST NOT change a collection holding a format-3 index (no ``member_tokens``) unless
it can rebuild that index kind from the elements in the same commit (an ivf_flat index retrains,
an hnsw graph is rebuilt). A writer that cannot leaves such collections unchanged; reading
them needs none of this. It MUST NOT write a new version of an opaque atom
(`Atoms of unknown types (D59)`_).

What is normative and what is not
---------------------------------

Normative: every byte layout on this page; the type names and the alias table; the value
encoding with its one spelling per value, the map-key rule and the nesting limit; the order, hash and exact-token
functions (for writers of keyed structures); the structure invariants of the AVL trees and
collections; the index maintenance contract and the IVF list assignment; the semantic catalog chapter (types, fields, canonical form, expression
language and intent log); the field names,
their optionality, the meaning of absence and the reserved attribute names (accessors
excepted); the root names reserved by ``__``; what readers and writers do with opaque atoms and
the name a reference carries; the format stability rules (what each version number covers, the
lowest-sufficient-version rule) and the pinned vectors; the root-publication protocol of a shared
directory (lock, epoch, rename) and the minimal writer's checks; the log reading rules (torn tail versus corruption); the cluster frame and message
layouts, the one-way connections and the drop rule, and the values members compute (ballots,
fingerprint).

Implementation-specific (nothing on disk depends on them, and an implementation MAY do
otherwise): lazy loading and stubs (portable as a technique, R83a); the decoded-atom cache, the
record and page caches, residency and unloading; the query result cache and query plans; the
per-version operation logs of collections (never persisted) and the merge of concurrent
writers; group commit; durability modes (``fsync``, ``flush``, ``none``) other than the acceptor
log's unconditional sync; segment size, sealing, shipping and checkpoint timing; the
``ValueBox`` threshold, tree shapes, bucket order and IVF training (they change bytes, not
meaning); pager tokens (a client-side msgpack
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
   * - Format 3 to 4 (D58, ruling R129)
     - Exact tokens are canonical encodings (a ``bin``) instead of Python-text tuples, and
       key-group and vector indexes persist ``member_tokens``; a datetime in an IANA zone is
       written as its UTC instant with ``["instant", key]``; one spelling per value (canonical
       ``@int``, ``@dec``, ``@td``); the map-key rule; required fields checked at load
     - Format-3 segments and roots are read. A format-3 zone datetime (``["zone", key]`` in a
       ``@dt``) is read as a wall clock with the reader's database and rewritten as an instant.
       A format-3 index (no ``member_tokens``) answers queries and is rebuilt from its
       collection at its first change, a full rebuild (an ivf_flat retraining, an hnsw graph
       construction) inside the user's first write transaction on it; ``validate()`` reports
       it stale, never corrupt. A format-3 ``@map`` with a zone-datetime key or NaN keys of one
       encoding is read as written and refused when written again (`Map keys`_). **Values of
       built-in subclasses were misfiled by format 3**: it wrote them through the subclass's
       own conversion (``str()``, ``float()``, ``bytes()``, ``str()`` of a ``Decimal``) but
       ordered and hashed them by their base value. The idioms that trigger it are a
       ``(str, Enum)`` member on every Python version (``str(Color.RED)`` is ``"Color.RED"`` on
       3.11 to 3.14, checked on 3.11 and 3.14) and any subclass of ``str``, ``float``,
       ``bytes`` or ``Decimal`` that overrides that conversion. The value reads back as the
       written text (``"Color.RED"``), stored where ``"red"`` belongs. Symptom: a Dictionary,
       HashDictionary, Set, CountedSet or RepeatedKeysDictionary holding it, or an index over
       it, cannot find it (``has`` is false, ``remove`` does nothing), and in a key-ordered
       Dictionary the out-of-order key also hides **other** keys (with ``dog``, ``zebra`` and
       ``Color.RED``, ``has("dog")`` is false); later writes go into the misordered tree.
       Detection: ``collection.validate()`` raises ``ProtoMisfiledError`` (a
       ``ProtoCorruptionError``) naming the misfiled key, for keyed trees out of order, hash
       buckets holding another hash's element and index groups holding another key's
       element. Repair: ``tx.set_root_object(name, collection.rebuilt())``, which refiles the
       entries as they read now and rebuilds every index; it refuses (``ProtoValidationError``)
       when two entries become one key with two different values, so it never drops a value.
       A collection held by another atom is repaired by storing the rebuilt collection in
       that atom. A non-canonical text form is refused;
       ``proto_db`` never wrote one. A format-3 reader refuses format-4 data
       (``ProtoNotSupportedError``)

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
- Datetimes in an IANA zone are stored as their UTC instant, not as a wall clock (D58).
- There is no ``proto_db.tools.inspect`` module in this revision.


Known gaps
----------

None of these breaks a round trip between conforming implementations.

- **Symbols** (``@sym``) are decided but not implemented: an implementation cannot store
  symbols yet without breaking the round trip. Symbol *references* (a storage encoding of strings)
  are deferred and not part of format 5 (`Symbol references`_). The class registry's persisted state is
  implemented, an object's parents (`Atom records`_) and the registry roots (`Registry roots`_),
  as format-5 features (`Format stability`_).
- **Format-3 zone datetimes need a tz database** (what remains of B3). Format-4 data never does
  for its keys, hashes or tokens (an ``["instant", key]`` value holds its UTC instant), only to
  show a wall clock. A format-3 ``["zone", key]`` value holds a wall clock, and its instant, its
  order key, its hash and, as a map key, its conflicts with other keys (`Map keys`_) all depend
  on the offset the reader's database gives that wall clock. A reader without a tz database,
  or whose database lacks the zone, MUST refuse such a value with an explicit error; it MUST NOT
  assume an offset. (``proto_db`` refuses a ``@dt`` whose zone its database lacks, in either form,
  with ``ProtoCorruptionError`` "Malformed @dt value"; for the format-4 form another reader MAY
  instead present the instant in UTC, since nothing stored depends on the zone's rules.) A reader whose database differs from the writer's may file such a value
  elsewhere than ``proto_db`` did; rebuilding the collection (``rebuilt()``) after the first read
  refiles it as an instant. Nothing else remains: no conversion tool is planned.
- **Format-3 indexes are rebuilt only at the first change** (what remains of B4). A format-3
  key-group or vector index (no ``member_tokens``) answers every query, and ``validate()``
  reports it stale, never corrupt. ``proto_db`` rebuilds it inside the first write transaction that
  changes its collection; nothing rebuilds it ahead of time, and a writer that cannot rebuild
  that index kind MUST NOT change the collection (`A minimal external writer (file storage)`_).
- **Extractor and vector backends** are per-process functions: an index on an extractor, or an
  hnsw index, is only fully usable by an implementation that provides the same function or
  backend.
- **The ``numpy`` parameter** of an exact vector index is a Python knob persisted in the format
  (``params``): it changes no answer, only how ``proto_db`` computes it. Another implementation
  preserves it and may ignore it.
- **Format-3 collections misfiled by built-in subclasses** stay misfiled until rebuilt: see
  the format history (detection with ``validate()``, repair with ``rebuilt()``). Nothing
  repairs them automatically: a reader cannot tell which value an application meant.
- **Cost of exact tokens.** A token is a msgpack packing of the value. Measured on the
  reference platform (free-threaded CPython 3.14 with msgpack's C extension, where
  ``msgpack.packb(1)`` alone takes about 4.6 µs, nearly all of it building a ``Packer``: packing
  ``1`` with a reused one takes 0.14 µs), a token costs 5 to 7 µs for a scalar or a small tuple,
  against 0.5 to 3 µs for the format-3 Python-text tuples. In an indexed write workload (40
  commits of 200 appends and 20 removals each on two lists with three indexes, FileStorage),
  tokens took 0.34 % of commit time, so no fast path was added (a reused packer would give the
  same bytes).
- **Zone datetimes after a tz-database change.** A stored zone datetime keeps its instant; the
  wall clock a reader shows is computed with the reader's database, so a future appointment
  written as "09:00 in São Paulo" shows another wall clock if that zone's rules change before
  it. This is the price of keys that never depend on a tz database (D58); applications that
  need the wall clock as written store it as a naive datetime plus the zone name.

Conformance vectors
-------------------

``tests/conformance/`` holds the vectors (``vectors/``), their manifest (``manifest.json``, a
JSON description of what every file means, in a language-neutral notation), the generator
(``generate.py``) and the test that checks the committed files byte for byte
(``test_conformance.py``). They cover:

- every value kind, the legacy forms readers accept (format-2 references, a built-in type by
  name, a format-3 zone datetime, a type alias, format-3 maps with zone-datetime and repeated
  NaN keys, which a writer refuses to write again) and one input of every kind of refusal (value,
  map key, atom, record, root, segment and log layers, invalid cluster messages);
- the order, hash and exact-token functions, including float and decimal edge cases, aware
  times and datetimes, the atom hash and nested atoms;
- record, segment and root framing (current, format 3 and format 2);
- a complete format-4 database written through the public API: every persistable built-in
  type and one application type, three versions of one database, a mutable cell, trained
  ivf_flat (with its list assignment) and hnsw vector indexes;
- a format-3 database: a DatabaseRoot without ``origin``, a key-group index (over ints,
  strs, floats, decimals, tuples, bytes, dates, datetimes, nil and a list) and a vector index
  written with the format-3 member tokens in plain KeyNode trees (valid before, rebuilt at
  their first change), zone datetimes in the wall-clock form, maps with format-3 key forms,
  and an application record and reference spelled by an alias;
- a cluster commit stamped with its lease, the three log kinds with every acceptor-log entry
  kind decoded field by field, and a torn tail versus mid-log corruption;
- one frame of every cluster message type;
- a ``polyglot`` space holding a record of the type ``polyglot.st.Order``, which no Python class
  implements (an opaque atom referencing a ``proto_db`` record), referenced from a root record,
  and an older record of that type whose ``t`` is its previous name ``polyglot.st.SalesOrder``,
  referenced under the declared name.
  The test changes the referencing record and checks that the opaque record is kept byte for
  byte and still reachable at its pointer;
- a ``parents`` space using the format-5 feature ``parents``: a format-4 segment, then the
  format-5 segment opened by the first record with parents, holding a record whose parents are
  a saved record, a record saved in the same commit and a registry name, and a record whose
  only parent is a registry name; its root record is version 5;
- a ``registry`` space using the format-5 feature ``registry``: a format-4 segment, then the
  format-5 segment opened by an install of a module and its dependency, holding the catalog
  objects, the ``ModuleList`` of ``__currentModules`` and the ``Catalog`` of ``__catalog``
  (`Registry roots`_); its root record is version 5.

``tests/conformance/vectors.sha256`` pins the vector files (`Format stability`_).

**The verifier.** ``python -m proto_db.tools.verify <directory> [--out FILE]`` checks a
file-storage space written by any implementation, the reverse direction of the vectors. It
never writes to the space. It reads the root record, decodes every atom reachable from the
SpaceRoot (every database, every version through ``previous``, every tree node, index and
record; each pointer once), checks that every record of a segment of the current write version
is in its canonical encoding (re-encoding the decoded record, in the writer's attribute order,
gives the stored bytes; a non-minimal integer, string, binary or container header, a ``float
32``, a built-in type spelled by name instead of its alias, or any other form writers MUST NOT
produce is a ``non-canonical`` failure; type names are compared as stored, so an application
alias left by a supported rename is not one; older segments hold legacy forms and are not
checked), checks that the root record's body is the canonical map ``{version, segment, offset,
epoch}`` (minimal encodings, that key order, no other key), validates
every collection with its indexes (``IndexedCollection.validate()``), and prints the current content of every database as JSON
in the manifest's notation (``proto_db.tools.notation``: the ``contents`` of a database vector,
without the generator's own ``ivf`` list assignment; an opaque atom is described as
``{"opaque": type name, "attrs": [...]}``), plus ``root``, ``atoms``, ``collections`` and
``opaque_types``. Every failure is printed on standard error as ``FAILED <where>: <error class>:
<message>``, and the walk goes on. Exit status: 0 when everything decoded, was canonical and
validated; 1 when anything failed, a missing or damaged segment included; 2 only when there is
no readable root record (no ``space.root``, a bad CRC, an unknown version). The format-3 ``legacy``
vector fails it on purpose, naming each misfiled collection.

``tests/conformance/README.md`` explains the notation and how another implementation uses them.
