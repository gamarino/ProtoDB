# Conformance vectors of the ProtoDB storage format

These files are the contract between ProtoBase and any other implementation of its storage
format, protoBaseST first (rulings R83, R83a; decision D57). The specification is
`docs/source/format-spec.rst`; the vectors show its bytes. Every byte here was produced by
ProtoBase's own encoders (`generate.py`) and is checked byte for byte against the code by
`test_conformance.py`, so a vector never disagrees with what ProtoBase reads and writes.

## Files

| Path | Content |
| --- | --- |
| `manifest.json` | What every vector file means, in a language-neutral JSON notation (below) |
| `vectors/values/*.msgpack` | One canonical value each, as an attribute value is encoded |
| `vectors/values/depth-*.msgpack` | Values exactly `MAX_VALUE_DEPTH` (1022) MessagePack containers deep (`values_depth`) |
| `vectors/invalid/*.bin` | Inputs a reader must refuse, at every layer (value, map key, atom envelope and state, record, segment, root, log, acceptor log, cluster message) |
| `vectors/framing/` | Segment headers (formats 4, 3 and 2), an ATOM and a BLOB record, root records (file, formats 3 and 2, cloud, tiered) |
| `vectors/database/` | A complete format-4 file-storage space: `space.root` and one segment, written through the public API (trained ivf_flat and hnsw vector indexes included) |
| `vectors/legacy/` | A format-3 space: a DatabaseRoot without `origin`, a key-group index (tokens of every kind) and a vector index written with format-3 member tokens and plain KeyNode trees, zone datetimes in the wall-clock form, maps with format-3 key forms (zone datetimes, NaNs twice), collections that format 3 misfiled by writing a `(str, Enum)` member through `str()`, an application record and reference spelled by an alias |
| `vectors/polyglot/` | A format-4 space holding a record of `polyglot.st.Order`, a type no Python class implements (an opaque atom, D59), which references a ProtoBase record and is referenced from a root, plus an older record of that type written as `polyglot.st.SalesOrder` (its previous name) and referenced under the declared name |
| `vectors.sha256` | The SHA-256 of every vector file and of `manifest.json`, sorted by path; `format-spec.rst` states this file's own digest |
| `vectors/cluster/<uuid>.seg` | The segment of one cluster commit: a `SpaceRoot` with its lease term, epoch and predecessor |
| `vectors/cluster/roots.log` | An acceptor log holding every entry kind (1 to 6), each listed field by field |
| `vectors/cluster/roots-torn.log` | The same log with a torn tail (its last frame cut) |
| `vectors/cluster/ranges.log`, `segments.log` | The other two framed logs of a cluster node |
| `vectors/cluster/messages/*.frame` | One wire frame (4-byte big-endian length, then msgpack) of every message type |
| `generate.py` | The script that wrote all of the above |
| `typed.py` | The notation, in Python (build a value from its description); describing values and databases is `proto_db/tools/notation.py`, which `python -m proto_db.tools.verify` prints |
| `model.py` | The application type of the vectors, `conformance.Invoice` |

## The notation

Every description is a JSON object with one key.

**Values** (canonical values, what the codec decodes):
`{"nil": null}`, `{"bool": true}`, `{"int": "-5"}` (decimal text: JSON numbers lose precision),
`{"float": "0x1.8000000000000p+0"}` (C99 hexadecimal; `"nan"`, `"inf"`, `"-inf"`),
`{"str": "..."}`, `{"bytes": "00ff"}` (hexadecimal), `{"decimal": "1.20"}`,
`{"date": [y, m, d]}`, `{"time": [h, mi, s, us, fold, tz]}`,
`{"datetime": [y, m, d, h, mi, s, us, fold, tz]}` with `tz` = `null`,
`["fixed", offset_us, name]`, `["instant", key]` (the fields are the UTC instant, fold 0) or
`["zone", key]` (a time's zone; in a datetime only the format-3 wall-clock form), `{"timedelta": [days, seconds, us]}`, `{"uuid": "<32 hex>"}`,
`{"list": [...]}`, `{"tuple": [...]}`, `{"map": [[key, value], ...]}` (in order),
`{"missing": true}`, `{"mutable": "name"}`,
`{"ref": {"segment": "<32 hex>", "offset": n, "type": "type name"}}`.

**Stored objects** (database contents): `{"object": type, "attrs": [[name, ...], ...]}` (a
record, attributes sorted by name), `{"opaque": type, "attrs": [...]}` (a record of a type the
reader has no class for), `{"collection": type, "elements": [...]}` (List, Set) or
`{"collection": type, "items": [[key or element, value or count], ...]}` (the dictionaries,
CountedSet), in the collection's iteration order.

**Raw msgpack data** (atom record envelopes, root records, cluster messages):
the same scalars, plus `{"array": [...]}` and `{"mpmap": [[key, value], ...]}` for msgpack
arrays and maps.

## Using the vectors in another implementation

A reader passes when, for every entry of `manifest.json`:

1. `values`: decoding the file gives the described value; encoding that value gives the
   file's bytes again (a writer's check).
2. `values_decode_only`: legacy forms (format 2 references, a built-in type spelled by name,
   an alias, a format-3 zone datetime, msgpack's wider encodings).
   Decoding gives the described value; re-encoding gives the bytes of the `canonical` file,
   never the legacy form. `values_read_not_rewritten`: format-3 maps whose keys the map-key
   rule forbids (a zone datetime, NaNs with one encoding); decoding gives the described value,
   and encoding it again is refused (ProtoValidationError).
3. `invalid`: reading the file at its `layer` is refused with the listed `error`:
   `corruption` (ProtoCorruptionError), `not_supported` (ProtoNotSupportedError) or `protocol`
   (a cluster receiver drops the connection). Layers: `value` (one canonical value), `atom`
   (an atom record payload, loaded as a reference to `type`), `record`, `segment` (a segment
   header), `root` (a root record), `log` (a framed log of `kind`), `roots_log` (an acceptor log,
   decoded entry by entry), `message` (bytes received on a cluster connection).
   The `value-depth-over-*` inputs are one MessagePack container deeper than `MAX_VALUE_DEPTH`
   (their bytes come from a raw msgpack packer: no conforming writer produces them); a reader
   refuses them as `corruption`, whatever the depth its own stack would allow.
4. `order`: the values of `ascending` are in strictly ascending key order; each group of
   `equal` is one key; the `unorderable` values cannot be keys.
5. `hashes`, `atom_hashes`, `exact_tokens` and `atom_tokens`: your hash function and exact
   tokens give these results (a token is given as hexadecimal bytes: it is the value's canonical
   encoding, with every atom written as `{"@ref": nil}`). They matter to any writer of a set, hash
   map or index.
6. `framing`: each header, record and root decodes to the listed fields and re-encodes to the
   same bytes.
7. `database`: walk the segment from offset 26; each record is at the listed `offset`, has the
   listed `kind`, `size`, `tag`, `type`, `oid` and decoded `state` (attributes in the written
   order), and re-encodes to the same bytes. Then open the space as a database: following
   `space.root` -> `SpaceRoot` -> `databases["db"]` -> `DatabaseRoot`, the content you read must
   be `contents` (`roots` lists each root object with collections expanded in their iteration
   order, `object` entries list a record's attributes sorted by name; `indexes` gives each
   index's type and whether it has format-4 member tokens; `ivf` gives the trained centroids
   and the list number of every stored vector, which your list assignment must reproduce).
   Records whose `canonical` is true re-encode byte for byte; the others are legacy forms
   (`legacy`: an alias, a format-3 zone datetime) that re-encode canonically.
   Do the same for `legacy_database`, a format-3 space: its indexes validate before any change
   (stale, not corrupt), and after a change of a collection its index must have been rebuilt
   with format-4 tokens. Records whose `rewritable` is false hold a format-3 map key form:
   they decode to the listed state, and encoding that state again is refused.
   `misfiled` lists the roots format 3 misfiled (a `(str, Enum)` member written as
   `"LegacyColor.RED"` but filed as `"red"`): your integrity check must report them, and a
   rebuild of each must hold every key listed.
8. `cluster`: the segment as in step 7; each log decodes to the listed entries (`roots_log`
   gives every entry's size and `fields`, from which your encoder must rebuild the entry's
   bytes -- `records` items are `[segment, offset, size]` of records in the cluster segment --,
   the commit's root and base, and what a reader of the log concludes in `loaded`);
   `roots_log_torn` is read with its torn tail dropped (`valid_end`, `entries`); each message
   frame decodes to the described message and re-encodes to the same bytes.

9. `polyglot_database`: as `database`; the record of `opaque_type` must be read without a class
   for it, its fields readable, and kept byte for byte when the record that references it is
   changed and committed again.

10. `values_depth`: `max_value_depth` is `MAX_VALUE_DEPTH` (1022), the deepest nesting of
    MessagePack containers a value may have (format spec, "Value nesting depth": a list adds 1, a
    `@tup` 2, a `@map` 3, a tag 1, 2 or 3 by the arrays in its body). Each entry describes its
    value as `levels`, `[kind, count]` pairs from the outside in (`list`, `tuple`, or `map`: a map
    of one pair whose key is the string `"k"`), around `leaf` (a value description); `containers`
    is the file's depth. Decoding the file and encoding the value give each other, and one more
    list around the value is refused by a writer (ProtoValidationError). Readers and writers
    accept these depths and refuse one container more; the vectors nest deeper than recursion in
    many languages allows by default.

For the cross-implementation round trip (R83a), also run the reverse: write a database with
your implementation, open it with ProtoBase (`ObjectSpace(FileStorage(path))`), and compare; `python -m proto_db.tools.verify <path>`
does the ProtoBase side in one command (exit 0, and the content dumped in this notation).

## Regenerating

```
python -m tests.conformance.generate
```

Only when the format changes on purpose: the test checks the committed files and never
regenerates them. The generator makes oids, segment UUIDs, the database origin and commit
times deterministic, so a regeneration with an unchanged format rewrites identical bytes and
`git diff` shows exactly what a format change changed. Update `format-spec.rst` in the same
commit, including the pinned digest of `vectors.sha256` in its "Format stability" section
(`sha256sum tests/conformance/vectors.sha256`).
