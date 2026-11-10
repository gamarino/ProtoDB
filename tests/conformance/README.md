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
| `vectors/invalid/*.bin` | Inputs a reader must refuse (a value, a record or a root record) |
| `vectors/framing/` | Segment headers (format 3 and 2), an ATOM and a BLOB record, root records (file, format 2, cloud, tiered) |
| `vectors/database/` | A complete file-storage space: `space.root` and one segment, written through the public API |
| `vectors/cluster/<uuid>.seg` | The segment of one cluster commit: a `SpaceRoot` with its lease term, epoch and predecessor |
| `vectors/cluster/roots.log` | An acceptor log holding every entry kind (1 to 6) |
| `vectors/cluster/ranges.log`, `segments.log` | The other two framed logs of a cluster node |
| `vectors/cluster/messages/*.frame` | One wire frame (4-byte big-endian length, then msgpack) of every message type |
| `generate.py` | The script that wrote all of the above |
| `typed.py` | The notation, in Python (build a value from its description, describe a value) |
| `model.py` | The application type of the vectors, `conformance.Invoice` |

## The notation

Every description is a JSON object with one key.

**Values** (canonical values, what the codec decodes):
`{"nil": null}`, `{"bool": true}`, `{"int": "-5"}` (decimal text: JSON numbers lose precision),
`{"float": "0x1.8000000000000p+0"}` (C99 hexadecimal; `"nan"`, `"inf"`, `"-inf"`),
`{"str": "..."}`, `{"bytes": "00ff"}` (hexadecimal), `{"decimal": "1.20"}`,
`{"date": [y, m, d]}`, `{"time": [h, mi, s, us, fold, tz]}`,
`{"datetime": [y, m, d, h, mi, s, us, fold, tz]}` with `tz` = `null`, `["zone", key]` or
`["fixed", offset_us, name]`, `{"timedelta": [days, seconds, us]}`, `{"uuid": "<32 hex>"}`,
`{"list": [...]}`, `{"tuple": [...]}`, `{"map": [[key, value], ...]}` (in order),
`{"missing": true}`, `{"mutable": "name"}`,
`{"ref": {"segment": "<32 hex>", "offset": n, "type": "type name"}}`.

**Raw msgpack data** (atom record envelopes, root records, cluster messages):
the same scalars, plus `{"array": [...]}` and `{"mpmap": [[key, value], ...]}` for msgpack
arrays and maps.

## Using the vectors in another implementation

A reader passes when, for every entry of `manifest.json`:

1. `values`: decoding the file gives the described value; encoding that value gives the
   file's bytes again (a writer's check).
2. `values_decode_only`: legacy forms (format 2 references, a built-in type spelled by name).
   Decoding gives the described value; re-encoding gives the bytes of the `canonical` file,
   never the legacy form.
3. `invalid`: decoding the file at its `layer` (a value, a record, a root record) is refused.
4. `order`: the values of `ascending` are in strictly ascending key order; each group of
   `equal` is one key; the `unorderable` values cannot be keys.
5. `hashes` and `exact_tokens`: your hash function and exact tokens give these results.
   They matter to any writer of a set, hash map or index.
6. `framing`: each header, record and root decodes to the listed fields and re-encodes to the
   same bytes.
7. `database`: walk the segment from offset 26; each record is at the listed `offset`, has the
   listed `kind`, `size`, `tag`, `type`, `oid` and decoded `state` (attributes in the written
   order), and re-encodes to the same bytes. Then open the space as a database: following
   `space.root` -> `SpaceRoot` -> `databases["db"]` -> `DatabaseRoot`, the content you read must
   be `contents` (`roots` lists each root object with collections expanded in their iteration
   order, `object` entries list a record's attributes sorted by name).
8. `cluster`: the segment as in step 7; each log decodes to the listed entries (`roots_log`
   gives the size of every entry, the commit's root and base, and what a reader of the log
   concludes in `loaded`); each message frame decodes to the described message and
   re-encodes to the same bytes.

For the cross-implementation round trip (R83a), also run the reverse: write a database with
your implementation, open it with ProtoBase (`ObjectSpace(FileStorage(path))`), and compare.

## Regenerating

```
python -m tests.conformance.generate
```

Only when the format changes on purpose: the test checks the committed files and never
regenerates them. The generator makes oids, segment UUIDs, the database origin and commit
times deterministic, so a regeneration with an unchanged format rewrites identical bytes and
`git diff` shows exactly what a format change changed. Update `format-spec.rst` in the same
commit.
