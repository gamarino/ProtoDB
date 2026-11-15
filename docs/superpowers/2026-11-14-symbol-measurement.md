# Symbol references: measurement and verdict (Task 10, R153)

Spec section 3a proposes symbols as a storage encoding of strings: a string already interned in the database is
written as a short content id instead of its bytes. Ruling R153 makes the feature conditional on a measurement:
references must save a material share of bytes that segment compression does not. This document is that
measurement.

## Criterion (R213)

Symbols **pay** only if, for some minimum length `L`, variant (d) (references plus compression) is at least
**10 % smaller in total bytes** than variant (c) (compression alone), measured on the real segment bytes the
storage writes. Otherwise the verdict is **deferral**: Task 11 documents the deferral and implements nothing.

The criterion is applied to the per-block compression figures, the realistic case (see Method). The whole-segment
figures are reported too; they do not change the verdict (below).

## Data

`benchmarks/symbol_bytes.py` builds a sales data set with field names after common ERP practice (Odoo-like),
seeded (`--seed 20261114`), so every run writes the same data:

| Model | Objects | Fields (strings in bold) |
|---|---:|---|
| partner | 1,500 | **ref, name, street, city, state_name, country_name, country_code, zip, email, phone, lang, currency_code, payment_term**, is_company, customer_rank, supplier_rank, active |
| product | 600 | **default_code, name, categ_name, uom_name, type, barcode, tracking, invoice_policy**, list_price, standard_price (decimals), sale_ok, purchase_ok, active |
| sale order | 2,000 | **name, state, currency_code, payment_term, user_name, team_name, client_order_ref** (40 % of orders, else nil), **note, invoice_status**, partner (reference), date_order, amount_untaxed, amount_tax, amount_total, lines (a list of references) |
| order line | 9,864 | **name** (`[PRD-00012] Product name`), **product_uom, tax_name, state**, product (reference), sequence, product_uom_qty, price_unit, discount, price_subtotal, qty_delivered, qty_invoiced |
| **total** | **13,964** | |

Places are 9 countries with 2 to 5 states each and 1 to 5 cities per state (accented names included); 8 product
categories written as paths (`All / Saleable / Hardware / Fasteners`); order states `draft`, `sent`,
`confirmed`, `done`, `cancelled`; currency codes follow the partner's country. Every object is a `DBObject`; the
three collections are `Dictionary` roots keyed by `ref`, `default_code` and the order `name`.

It is written the way an ERP writes it, through a real `ObjectSpace` over `FileStorage` (`durability="flush"`) in
a fresh directory: partners and products imported 100 per transaction, then one transaction per sales order (the
order and its lines). The space then holds one segment of 11,393,442 bytes with 54,360 records:

| Records | Type | Bytes (header included) |
|---:|---|---:|
| 13,964 | `DBObject` (alias 19) | 5,044,010 |
| 30,280 | `avl.KeyNode` (alias 0) | 4,942,713 |
| 6,066 | `Dictionary` (alias 6) | 755,737 |
| 2,022 | `DatabaseRoot` (alias 18) | 478,790 |
| 2,022 | `SpaceRoot` (alias 17) | 171,866 |
| 6 | `IndexSet` (alias 3) | 300 |

The tree nodes and roots are the path copies of the 2,000 order commits and 21 import commits; they are bytes the
storage really wrote, so they are measured with the rest.

What repeats, in the state maps of all records (inline msgpack bytes):

| Strings | Occurrences | Distinct | Inline bytes | Share of (a) |
|---|---:|---:|---:|---:|
| field names, `DBObject` records | 181,668 | 50 | 1,875,528 | 16.5 % |
| field names, structure records | 214,038 | 15 | 1,274,212 | 11.2 % |
| values, all records | 110,820 | 13,942 | 1,292,836 | 11.3 % |
| values of at least 8 bytes | 44,077 | 8,869 | 878,185 | 7.7 % |
| values of at least 16 bytes | 18,752 | 4,723 | 593,883 | 5.2 % |
| values of at least 24 bytes | 14,692 | 2,788 | 510,713 | 4.5 % |

Field names of at least 8 bytes: 133,664 occurrences of 37 distinct names; of at least 16 bytes: 2,000 of one
name (`client_order_ref`, 16 bytes); none of 24 or more.

## Method

- **(a) inline**: the bytes of the segment files as written.
- **(b) simulated references.** Every ATOM record is decoded and re-encoded with each string of the state map
  (attribute names and string values) of at least `L` UTF-8 bytes replaced by a reference, **only where the
  reference is shorter than the string's inline encoding** (the writer policy of spec 3a). Record headers are
  rebuilt with the new lengths and CRCs. To (b) is added a symbol table holding each distinct replaced string
  once, costed as one record per string (15-byte header plus the payload `[<bin id>, <str>]`) in a segment of
  its own; the table's tree structure is not costed, which favours the references.
  - Not candidates: the envelope keys (`t`, `oid`, `s`), the tag keys of canonical values (`@ref`, `@dec`, ...),
    and the strings inside scalar encodings (decimal text, time zones, a reference's type tag): they are not
    `str` values. The type tag of every record here is an integer alias.
  - The packer is checked: every payload re-encodes to its exact bytes, and a policy that replaces nothing
    rebuilds the written segment byte for byte (both asserted).
- **Reference size (R214).** The primary assumption is **17 bytes**: the byte `0xC1` (the one byte msgpack never
  uses as a type byte, already the record magic) followed by the 16-byte content hash of spec 3a's proposal (a).
  Any distinct wire form needs at least one tag byte, so 17 bytes is the smallest a 16-byte content id can take;
  the real hash bytes (BLAKE2b) are written, so the compressor sees what it would see. Three sensitivity sizes are
  measured: 9 bytes (an 8-byte hash, if collisions were left to the commit-time retry), 24 bytes (the form this
  format uses for tagged values, `{"@sym": <bin 16>}`), and 3 bytes (`0xC1` and a 2-byte sequential id, spec 3a's
  option (b), measured only to show what the id size changes).
- **(c) compression**: Python 3.14's `compression.zstd` (zstd 1.5.7, level 3), applied to each segment file as
  a whole and per 64 KiB block (each block compressed independently, sizes summed).
- **(d)**: the (b) segments and the table segment, compressed the same two ways.

**Which compression is realistic (R217).** Per block. The storage reads one record by its pointer (segment,
offset) at random; whole-segment compression would decompress up to 256 MiB (the default `segment_max_bytes`) to
read one atom, and an open segment is appended to for its whole life, so it could not be compressed until
sealed. A block-compressed segment reads one block per record, the usual design of log-structured stores. The
whole-segment figure is the best case of compression and is reported for completeness.

## Results

Command (from a clean export of commit `80a71c7`, `python -m benchmarks.export 80a71c7 <dir>`, run in that
directory), once with `-X gil=0` and once with `-X gil=1`:

```bash
systemd-run --user --scope -p MemoryMax=12G -p MemorySwapMax=0 \
  /home/gamarino/Documentos/proyectos/ProtoBase/.venv314t/bin/python -X gil=0 \
  benchmarks/symbol_bytes.py <fresh dir> --json <out.json>
```

Both runs printed byte-identical output: the same SHA-256 of the segment
(`5fc2f756aa2bb8d2c08302e67862423d023942d7e1c7ef14c60ade4a2be9d77e`) and the same figures for every variant
(`uuid.uuid4` and the commit clock are pinned to seeded values in the benchmark, so the bytes are reproducible).
Raw results: `benchmarks/results/2026-11-15-80a71c7-symbol-bytes-gil-{off,on}.{json,log}`. Each run took about
one minute.

Baseline: **(a) 11,393,442 B**; **(c) 2,799,544 B per 64 KiB block** (−75.4 %), 2,420,928 B whole segment
(−78.8 %).

Primary assumption, 17-byte reference, names and values as candidates (percentages: (b) against (a), (d) against
(c) of the same kind):

| L | Distinct replaced | Inline bytes saved | Table | (b) | (d) per block | (d) whole segment |
|---:|---:|---:|---:|---:|---:|---:|
| 8 | 4,355 | 275,099 | 268,168 | 11,386,511 (−0.06 %) | 2,999,701 (**+7.15 %**) | 2,607,665 (+7.71 %) |
| 16 | 4,355 | 275,099 | 268,168 | 11,386,511 (−0.06 %) | 2,999,701 (**+7.15 %**) | 2,607,665 (+7.71 %) |
| 24 | 2,788 | 260,949 | 181,066 | 11,313,559 (−0.70 %) | 2,929,171 (**+4.63 %**) | 2,534,385 (+4.69 %) |

`L` = 8 and 16 replace the same strings: a 17-byte reference is shorter than the inline encoding only for strings
of at least 17 UTF-8 bytes. **Field names save nothing** at this size: no name is longer than 16 bytes, so no
name reference is ever shorter than the name; every byte saved above comes from values (the names-only variant
equals (a) and (c) exactly, the values-only variant equals the rows above).

Sensitivity (names and values as candidates unless stated; the best row of each size):

| Reference | Best (b) | (d) per block, best L | (d) whole segment, same row |
|---|---:|---:|---:|
| 9 B (8-byte hash) | 10,910,061 at L=8 (−4.24 %) | 2,789,997 at L=8, **names only** (−0.34 %) | 2,417,490 (−0.14 %) |
| 24 B (`{"@sym": bin 16}`) | 11,416,403 at any L (+0.20 %) | 2,936,715 at any L (+4.90 %) | 2,538,383 (+4.85 %) |
| 3 B (sequential 2-byte id) | 9,791,054 at L=8 (−14.06 %) | 2,631,776 at L=8 (**−5.99 %**) | 2,475,489 (+2.25 %) |

With 3-byte ids at L=8, names alone save 10.67 % of (a) uncompressed (1,216,722 bytes) but only 2.85 % against
(c) per block; values alone save 3.40 % and 3.31 %. Every row of every size is in the raw results.

## Verdict: deferral

No variant meets the criterion. Under the proposed design (17-byte content ids) references plus compression are
**larger** than compression alone at every `L` (+4.6 % to +7.2 % per block): a hash is incompressible, and it
replaces text that compression already reduces to a few bytes per repetition. Even uncompressed, references save at
most 0.70 % of (a). The best figure of any reference size, the 3-byte sequential id the spec rejected for its
cluster cost, is −5.99 % per block, short of 10 %, and it turns into +2.25 % with whole-segment compression.
Compression alone removes 75.4 % of the bytes, with no format change.

Task 11 therefore documents the deferral in the spec and the format spec and implements nothing (R153).

## What would change the verdict

- **Data whose long strings repeat beyond a compression block.** References win only where a long string recurs
  far apart in the file (in different 64 KiB blocks) and is not otherwise compressible: long, identical
  descriptions or texts copied across many records written over a long time. The likely reason they do not win
  here (an explanation, not a measurement): the long values (product descriptions on order lines, category
  paths, notes) recur within a block often enough to compress anyway.
- **A short id.** Only a few-byte id brings (d) below (c) at all, and spec 3a explains its cost: a read-modify-write
  of the table on every commit that creates a symbol, and tentative ids in merged commits.
- **No compression.** If the storage never compresses, the 17-byte references still save at most 0.70 % here;
  the comparison would then be (b) against (a), and only a short id (−14.06 %) would be material.
- **A different compressor setting** (a trained zstd dictionary, a higher level, other block sizes) was not
  measured. The expectation, unmeasured, is that a dictionary helps (c) most on small blocks.

## Rulings made by this task

- **R214** The reference is costed at 17 bytes (`0xC1` and a 16-byte content hash, the smallest wire form of
  spec 3a's proposal), with 9, 24 and 3 bytes as sensitivity; the verdict uses 17.
- **R215** The writer policy of spec 3a is applied: a string is replaced only where the reference is shorter than
  its inline encoding, so `L` is a lower bound on top of that rule.
- **R216** The symbol table is costed as one record per distinct string (header, id, string) and nothing for its
  structure, a cost below any real implementation's; it favours the references.
- **R217** The criterion is applied to per-64 KiB-block compression (random reads by pointer); whole-segment
  compression is reported and does not change the verdict.
- **R218** The benchmark pins `uuid.uuid4` and the commit clock to seeded values, a benchmark-only patch, so the
  written bytes are reproducible and both GIL modes can be compared byte for byte.
- **R219** Candidate strings are attribute names and `str` values of state maps; envelope keys, tag keys and
  scalar encodings are excluded (they are not strings of the data model).
- **R220** The ERP data set is defined in the benchmark (Task 12's tutorial does not exist yet): 13,964 objects,
  imports 100 per transaction, one transaction per sales order.
