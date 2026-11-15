"""Bytes of string data with and without symbol references and compression (Task 10, R153/R213).

Builds a small ERP data set (partners, products, sales orders and order lines; seeded, so the
same data every run), writes it through a real ``ObjectSpace`` over ``FileStorage`` into a fresh
directory, and measures the segment files the storage actually wrote, four ways:

(a) inline strings: the segment bytes as written;
(b) a **simulated** symbol reference: every record is re-encoded with each string of at least
    ``L`` UTF-8 bytes (attribute names of state maps and string values) replaced by a fixed-size
    content-id reference, where the reference is shorter than the string's inline encoding (the
    writer policy of spec section 3a), plus a table holding each distinct replaced string once;
(c) compression of the real segment files, whole and per 64 KiB block;
(d) (b) then (c).

    python benchmarks/symbol_bytes.py WORKDIR [--json OUT] [--partners N] [--products N]
                                      [--orders N] [--seed S]

run from the root of the tree to measure (``proto_db`` is imported from the working directory).
``WORKDIR`` must not exist; the space is created in it and left there.

The reference is ``0xC1`` (the one byte msgpack never uses as a type byte) followed by the first
``h`` bytes of BLAKE2b of the string's UTF-8 bytes: 9 bytes for an 8-byte hash, 17 bytes for a
16-byte hash (the primary assumption).  A third size, 24 bytes, is the reference written as a
canonical tagged value of this format, ``{"@sym": <bin 16>}``.  A fourth, 3 bytes, is spec 3a's
rejected option (b), a sequential 2-byte id, measured only to show what the id size changes.
The hash bytes are real, so the
compressor sees what it would see.  Each table entry is costed as one record: the 15-byte record
header and the payload ``[<bin id>, <str>]``; the table's structure (its tree nodes) is not
costed, which favours the references.

Self-reporting: prints the objects per type, the records per type tag, the distinct strings and
every variant's bytes; verifies that every ATOM payload re-encodes to its exact bytes with the
packer used for the simulation, that the counts read back from the reopened space equal the
counts generated, and that a sample of the objects reads back equal to the generated model.
``uuid.uuid4`` and the commit clock are pinned to seeded values so the written bytes are
reproducible: the SHA-256 of the segments is printed, and the same command under ``-X gil=0`` and
``-X gil=1`` must print the same digest.  Exit status 1 on any mismatch.
"""
import argparse
import datetime
import decimal
import hashlib
import json
import os
import random
import struct
import sys
import uuid
import zlib

sys.path.insert(0, os.getcwd())

try:
    from compression import zstd as _zstd
    COMPRESSOR = "zstd %s (level %d)" % (".".join(map(str, _zstd.zstd_version_info)), _zstd.COMPRESSION_LEVEL_DEFAULT)

    def compress(data: bytes) -> bytes:
        return _zstd.compress(data)
except ImportError:                                  # Python < 3.14
    COMPRESSOR = "zlib %s (level 6)" % zlib.ZLIB_VERSION

    def compress(data: bytes) -> bytes:
        return zlib.compress(data, 6)

import msgpack

D = decimal.Decimal
BLOCK = 64 * 1024
LENGTHS = (8, 16, 24)
SEGMENT_HEADER = 26
RECORD_HEADER = 15
RECORD_MAGIC = b"\xc1\xd2"

# ---------------------------------------------------------------------------------------------
# The ERP data (deterministic).  Field names follow common ERP practice (Odoo-like); names of
# places, units and states are plain strings, as an application that does not normalise them
# into records writes them.
# ---------------------------------------------------------------------------------------------

COUNTRIES = [
    # name, code, currency, language, weight, {state: [cities]}
    ("Argentina", "AR", "ARS", "es_AR", 30, {
        "Buenos Aires": ["La Plata", "Mar del Plata", "Bahía Blanca", "Tandil", "Pilar"],
        "Ciudad Autónoma de Buenos Aires": ["Buenos Aires"],
        "Córdoba": ["Córdoba", "Río Cuarto", "Villa María"],
        "Santa Fe": ["Rosario", "Santa Fe", "Rafaela"],
        "Mendoza": ["Mendoza", "San Rafael", "Godoy Cruz"]}),
    ("United States", "US", "USD", "en_US", 20, {
        "California": ["Los Angeles", "San Francisco", "San Diego", "Sacramento"],
        "New York": ["New York", "Buffalo", "Rochester"],
        "Texas": ["Houston", "Austin", "Dallas", "San Antonio"],
        "Illinois": ["Chicago", "Springfield"]}),
    ("Spain", "ES", "EUR", "es_ES", 10, {
        "Comunidad de Madrid": ["Madrid", "Alcalá de Henares", "Getafe"],
        "Cataluña": ["Barcelona", "Girona", "Tarragona"],
        "Andalucía": ["Sevilla", "Málaga", "Granada"]}),
    ("Brazil", "BR", "BRL", "pt_BR", 12, {
        "São Paulo": ["São Paulo", "Campinas", "Santos"],
        "Rio de Janeiro": ["Rio de Janeiro", "Niterói"],
        "Minas Gerais": ["Belo Horizonte", "Uberlândia"]}),
    ("Mexico", "MX", "MXN", "es_MX", 8, {
        "Ciudad de México": ["Ciudad de México"],
        "Jalisco": ["Guadalajara", "Zapopan"],
        "Nuevo León": ["Monterrey", "San Pedro Garza García"]}),
    ("Germany", "DE", "EUR", "de_DE", 7, {
        "Bayern": ["München", "Nürnberg", "Augsburg"],
        "Berlin": ["Berlin"],
        "Nordrhein-Westfalen": ["Köln", "Düsseldorf", "Dortmund"]}),
    ("Chile", "CL", "CLP", "es_CL", 5, {
        "Región Metropolitana de Santiago": ["Santiago", "Puente Alto"],
        "Valparaíso": ["Valparaíso", "Viña del Mar"]}),
    ("Uruguay", "UY", "UYU", "es_UY", 4, {
        "Montevideo": ["Montevideo"],
        "Canelones": ["Canelones", "Ciudad de la Costa"]}),
    ("France", "FR", "EUR", "fr_FR", 4, {
        "Île-de-France": ["Paris", "Versailles", "Boulogne-Billancourt"],
        "Auvergne-Rhône-Alpes": ["Lyon", "Grenoble"]}),
]
STREETS = ["Avenida Corrientes", "Calle Mayor", "Main Street", "Rua Augusta", "Hauptstraße", "Avenida Rivadavia",
           "Rue de la Paix", "Avenida Paulista", "Broadway", "Paseo de la Reforma", "Calle San Martín",
           "Avenida Libertador", "Elm Street", "Gran Vía", "Avenida 18 de Julio"]
NAME_A = ["Andes", "Pampa", "Atlas", "Nova", "Delta", "Sur", "Norte", "Pacific", "Atlantic", "Global", "Prime",
          "Vértice", "Horizonte", "Cumbre", "Litoral", "Patagonia", "Alpha", "Omega", "Orion", "Cóndor"]
NAME_B = ["Logística", "Distribuciones", "Comercial", "Industrial", "Tecnología", "Servicios", "Trading",
          "Supply", "Foods", "Agro", "Metalúrgica", "Textil", "Consulting", "Retail", "Construcciones"]
SUFFIX = {"AR": ["S.A.", "S.R.L.", "S.A.S."], "US": ["Inc.", "LLC", "Corp."], "ES": ["S.L.", "S.A."],
          "BR": ["Ltda.", "S.A."], "MX": ["S.A. de C.V.", "S. de R.L."], "DE": ["GmbH", "AG"],
          "CL": ["SpA", "Ltda."], "UY": ["S.A.", "S.R.L."], "FR": ["SARL", "SAS"]}
FIRST = ["María", "Juan", "Lucía", "Carlos", "Ana", "Pedro", "Sofía", "Diego", "Laura", "Martín", "John", "Emma",
         "Hans", "Claire", "João", "Camila", "Pablo", "Valentina", "Michael", "Julia"]
LAST = ["García", "Fernández", "López", "Martínez", "Smith", "Johnson", "Müller", "Silva", "Rodríguez", "Pérez",
        "Gómez", "Dubois", "Santos", "Schmidt", "Romero", "Álvarez", "Brown", "Torres"]
PAYMENT_TERMS = ["Immediate Payment", "15 Days", "30 Days", "45 Days", "End of Following Month",
                 "30% Now, Balance 60 Days"]
CATEGORIES = {  # category path -> (unit names, nouns)
    "All / Saleable / Office Furniture": (["Units"], ["Desk", "Chair", "Cabinet", "Shelf", "Table"]),
    "All / Saleable / Hardware / Fasteners": (["Units", "Dozens", "Box of 100"], ["Screw", "Bolt", "Nut", "Washer", "Rivet"]),
    "All / Saleable / Hardware / Cables": (["m", "Units"], ["Power Cable", "Network Cable", "Coaxial Cable"]),
    "All / Saleable / Food / Dry Goods": (["kg", "g", "Units"], ["Flour", "Rice", "Sugar", "Yerba Mate", "Coffee"]),
    "All / Saleable / Food / Beverages": (["Liters", "Units"], ["Mineral Water", "Orange Juice", "Red Wine", "Beer"]),
    "All / Saleable / Chemicals": (["Liters", "kg"], ["Solvent", "Detergent", "Lubricant", "Paint"]),
    "All / Services / Consulting": (["Hours", "Days"], ["Implementation", "Training", "Support Plan"]),
    "All / Saleable / Packaging": (["Units", "Dozens"], ["Carton Box", "Pallet", "Stretch Film"]),
}
ADJ = ["Standard", "Premium", "Heavy Duty", "Compact", "Ergonomic", "Industrial", "Eco", "Large", "Small",
       "Stainless", "Galvanized", "Organic", "Professional"]
SPEC = ["M6", "M8", "1 kg", "5 kg", "500 ml", "1 L", "Cat6", "120x60", "Grade A", "Type B", "Blue", "Black", "White"]
TAXES = ["VAT 21%", "VAT 10.5%", "Sales Tax 8.25%", "IVA 16%", "ICMS 18%", "MwSt 19%", "TVA 20%", "Exempt"]
ORDER_STATES = [("draft", 15), ("sent", 10), ("confirmed", 45), ("done", 25), ("cancelled", 5)]
INVOICE_STATUS = {"draft": "no", "sent": "no", "cancelled": "no", "confirmed": "to invoice", "done": "invoiced"}
SALESPEOPLE = [f"{f} {l}" for f, l in zip(FIRST, LAST)][:16]
TEAMS = ["Direct Sales", "Website", "Distributors", "Key Accounts", "Export"]
NOTES = ["Deliver in the morning, between 8 and 12.", "Customer requests a single shipment.",
         "Payment confirmed by bank transfer.", "Call before delivery.", "Use the rear loading dock.", ""]


def build_data(seed: int, n_partners: int, n_products: int, n_orders: int) -> dict:
    """Return plain dicts (the model the read-back is compared with)."""
    rnd = random.Random(seed)
    weights = [c[4] for c in COUNTRIES]
    partners = []
    for i in range(n_partners):
        country, code, currency, lang, _, states = rnd.choices(COUNTRIES, weights)[0]
        state = rnd.choice(sorted(states))
        city = rnd.choice(states[state])
        company = rnd.random() < 0.7
        if company:
            name = f"{rnd.choice(NAME_A)} {rnd.choice(NAME_B)} {rnd.choice(SUFFIX[code])}"
        else:
            name = f"{rnd.choice(FIRST)} {rnd.choice(LAST)}"
        slug = "".join(ch for ch in name.lower() if ch.isascii() and ch.isalnum())[:16]
        partners.append({
            "ref": f"P{i:05d}", "name": name, "is_company": company,
            "street": f"{rnd.choice(STREETS)} {rnd.randint(1, 9999)}", "city": city, "state_name": state,
            "country_name": country, "country_code": code, "zip": f"{rnd.randint(1000, 99999)}",
            "email": f"{slug}{i}@example.com", "phone": f"+{rnd.randint(1, 99)} {rnd.randint(10, 999)} "
                                                       f"{rnd.randint(1000000, 9999999)}",
            "lang": lang, "currency_code": currency, "customer_rank": rnd.randint(0, 50),
            "supplier_rank": rnd.choice([0, 0, 0, 1, 2]), "payment_term": rnd.choice(PAYMENT_TERMS),
            "active": True})
    products = []
    cats = sorted(CATEGORIES)
    for i in range(n_products):
        categ = rnd.choice(cats)
        uoms, nouns = CATEGORIES[categ]
        price = D(rnd.randint(50, 500000)) / 100
        service = categ.startswith("All / Services")
        products.append({
            "default_code": f"PRD-{i:05d}", "name": f"{rnd.choice(ADJ)} {rnd.choice(nouns)} {rnd.choice(SPEC)}",
            "categ_name": categ, "uom_name": rnd.choice(uoms), "type": "service" if service else rnd.choice(["consu", "product"]),
            "list_price": price, "standard_price": (price * D("0.6")).quantize(D("0.01")),
            "barcode": "".join(str(rnd.randint(0, 9)) for _ in range(13)), "sale_ok": True,
            "purchase_ok": not service, "tracking": "none" if service else rnd.choice(["none", "none", "lot", "serial"]),
            "invoice_policy": rnd.choice(["order", "delivery"]), "active": True})
    states, sw = zip(*ORDER_STATES)
    orders = []
    base = datetime.datetime(2026, 1, 1, 8, 0, 0)
    for i in range(n_orders):
        p = rnd.randrange(n_partners)
        partner = partners[p]
        state = rnd.choices(states, sw)[0]
        lines = []
        for seq in range(rnd.randint(1, 9)):
            prod = rnd.randrange(n_products)
            pr = products[prod]
            qty = D(rnd.randint(1, 200))
            disc = rnd.choice([D(0), D(0), D(0), D(5), D(10)])
            subtotal = (qty * pr["list_price"] * (1 - disc / 100)).quantize(D("0.01"))
            lines.append({
                "sequence": 10 * (seq + 1), "product": prod, "name": f"[{pr['default_code']}] {pr['name']}",
                "product_uom_qty": qty, "product_uom": pr["uom_name"], "price_unit": pr["list_price"],
                "discount": disc, "tax_name": rnd.choice(TAXES), "price_subtotal": subtotal, "state": state,
                "qty_delivered": qty if state == "done" else D(0), "qty_invoiced": qty if state == "done" else D(0)})
        untaxed = sum((ln["price_subtotal"] for ln in lines), D(0))
        tax = (untaxed * D("0.21")).quantize(D("0.01"))
        orders.append({
            "name": f"S{i:05d}", "partner": p, "date_order": base + datetime.timedelta(minutes=17 * i),
            "state": state, "currency_code": partner["currency_code"], "payment_term": partner["payment_term"],
            "user_name": rnd.choice(SALESPEOPLE), "team_name": rnd.choice(TEAMS),
            "client_order_ref": f"PO-{rnd.randint(100000, 999999)}" if rnd.random() < 0.4 else None,
            "note": rnd.choice(NOTES), "invoice_status": INVOICE_STATUS[state], "amount_untaxed": untaxed,
            "amount_tax": tax, "amount_total": untaxed + tax, "lines": lines})
    return {"partners": partners, "products": products, "orders": orders}


def write_space(path: str, data: dict) -> None:
    """Write the data set the way an ERP would: partners and products imported in batches of 100
    per transaction, then one transaction per sales order (the order and its lines)."""
    from proto_db import DBObject, FileStorage, ObjectSpace
    space = ObjectSpace(FileStorage(path, durability="flush"))
    db = space.new_database("erp")
    for kind, key in (("partners", "ref"), ("products", "default_code")):
        rows = data[kind]
        for start in range(0, len(rows), 100):
            def fn(tr, batch=rows[start:start + 100]):
                coll = tr.get_root_object(kind) or tr.new_dictionary()
                for row in batch:
                    coll = coll.set_at(row[key], DBObject(**row))
                tr.set_root_object(kind, coll)
            db.run(fn)
    for order in data["orders"]:
        def fn(tr, order=order):
            partners = tr.get_root_object("partners")
            products = tr.get_root_object("products")
            lines = [DBObject(**{**ln, "product": products.get_at(data["products"][ln["product"]]["default_code"])})
                     for ln in order["lines"]]
            rec = DBObject(**{**order, "partner": partners.get_at(data["partners"][order["partner"]]["ref"]),
                              "lines": lines})
            orders = tr.get_root_object("orders") or tr.new_dictionary()
            tr.set_root_object("orders", orders.set_at(order["name"], rec))
        db.run(fn)
    space.close()


def verify_space(path: str, data: dict, rnd: random.Random) -> dict:
    """Reopen the space; check the counts and that a sample reads back equal to the model."""
    from proto_db import FileStorage, ObjectSpace
    space = ObjectSpace(FileStorage(path, durability="flush"))
    db = space.open_database("erp")
    tr = db.new_transaction()
    counts = {}
    failures = []
    for kind in ("partners", "products", "orders"):
        counts[kind] = tr.get_root_object(kind).count
    n_lines = 0
    orders = tr.get_root_object("orders")
    for _, rec in orders.items():
        n_lines += len(rec.lines)
    counts["order_lines"] = n_lines

    def same(rec, model, skip=()):
        for k, v in model.items():
            if k in skip:
                continue
            got = getattr(rec, k)
            if got != v or type(got) is not type(v) and not (v is None and got is None):
                failures.append((k, v, got))
    for kind, key in (("partners", "ref"), ("products", "default_code")):
        coll = tr.get_root_object(kind)
        for row in rnd.sample(data[kind], 100):
            same(coll.get_at(row[key]), row)
    for order in rnd.sample(data["orders"], 100):
        rec = orders.get_at(order["name"])
        same(rec, order, skip=("partner", "lines"))
        if rec.partner.ref != data["partners"][order["partner"]]["ref"]:
            failures.append(("partner", order["name"]))
        if len(rec.lines) != len(order["lines"]):
            failures.append(("lines", order["name"]))
        for got, ln in zip(rec.lines, order["lines"]):
            same(got, ln, skip=("product",))
            if got.product.default_code != data["products"][ln["product"]]["default_code"]:
                failures.append(("product", order["name"]))
    tr.abort()
    space.close()
    return {"counts": counts, "sample_failures": failures[:5], "sample_failure_count": len(failures)}


# ---------------------------------------------------------------------------------------------
# Segment parsing and the simulation's packer (msgpack, smallest form, as msgpack-python 1.x).
# ---------------------------------------------------------------------------------------------

def read_segments(path: str) -> list:
    """Return [(file name, header bytes, [(kind, payload bytes)])] for every segment."""
    out = []
    for name in sorted(n for n in os.listdir(path) if n.endswith(".seg")):
        with open(os.path.join(path, name), "rb") as f:
            raw = f.read()
        assert raw[:8] == b"PDB2SEG\0", name
        pos, records = SEGMENT_HEADER, []
        while pos < len(raw):
            hdr = raw[pos:pos + RECORD_HEADER]
            assert hdr[:2] == RECORD_MAGIC and zlib.crc32(hdr[:11]) == struct.unpack_from("<I", hdr, 11)[0], (name, pos)
            kind, length, crc = hdr[2], *struct.unpack_from("<II", hdr, 3)
            payload = raw[pos + RECORD_HEADER:pos + RECORD_HEADER + length]
            assert len(payload) == length and zlib.crc32(payload) == crc, (name, pos)
            records.append((kind, payload))
            pos += RECORD_HEADER + length
        out.append((name, raw[:SEGMENT_HEADER], records, raw))
    return out


def str_inline(b: bytes) -> bytes:
    n = len(b)
    if n < 32:
        return bytes([0xA0 | n]) + b
    if n < 256:
        return b"\xd9" + bytes([n]) + b
    if n < 65536:
        return b"\xda" + struct.pack(">H", n) + b
    return b"\xdb" + struct.pack(">I", n) + b


def pack_plain(v, out: bytearray) -> None:
    if v is None:
        out.append(0xC0)
    elif v is True:
        out.append(0xC3)
    elif v is False:
        out.append(0xC2)
    elif type(v) is int:
        if 0 <= v < 128:
            out.append(v)
        elif v >= 0:
            for lim, code, fmt in ((1 << 8, 0xCC, ">B"), (1 << 16, 0xCD, ">H"), (1 << 32, 0xCE, ">I"), (1 << 64, 0xCF, ">Q")):
                if v < lim:
                    out.append(code)
                    out += struct.pack(fmt, v)
                    break
        elif v >= -32:
            out += struct.pack(">b", v)
        else:
            for lim, code, fmt in ((1 << 7, 0xD0, ">b"), (1 << 15, 0xD1, ">h"), (1 << 31, 0xD2, ">i"), (1 << 63, 0xD3, ">q")):
                if v >= -lim:
                    out.append(code)
                    out += struct.pack(fmt, v)
                    break
    elif type(v) is float:
        out.append(0xCB)
        out += struct.pack(">d", v)
    elif type(v) is str:
        out += str_inline(v.encode("utf-8"))
    elif type(v) is bytes:
        n = len(v)
        out += (b"\xc4" + bytes([n])) if n < 256 else (b"\xc5" + struct.pack(">H", n)) if n < 65536 else (b"\xc6" + struct.pack(">I", n))
        out += v
    elif type(v) is list:
        array_header(len(v), out)
        for x in v:
            pack_plain(x, out)
    elif type(v) is dict:
        map_header(len(v), out)
        for k, x in v.items():
            pack_plain(k, out)
            pack_plain(x, out)
    else:
        raise TypeError(type(v))


def array_header(n: int, out: bytearray) -> None:
    out += bytes([0x90 | n]) if n < 16 else (b"\xdc" + struct.pack(">H", n)) if n < 65536 else (b"\xdd" + struct.pack(">I", n))


def map_header(n: int, out: bytearray) -> None:
    out += bytes([0x80 | n]) if n < 16 else (b"\xde" + struct.pack(">H", n)) if n < 65536 else (b"\xdf" + struct.pack(">I", n))


class Policy:
    """Which strings become references, and the bookkeeping of what was replaced.

    ``min_len`` is L (UTF-8 bytes); ``ref_hash`` the id length h; ``form`` "c1" (``0xC1`` + h
    hash bytes), "tagged" (``{"@sym": <bin h>}``) or "seq" (``0xC1`` + an h-byte sequential id,
    spec 3a option (b)); ``kinds`` the candidate kinds ("name", "value")."""

    def __init__(self, min_len: int, ref_hash: int, form: str, kinds: tuple):
        self.min_len, self.ref_hash, self.form, self.kinds = min_len, ref_hash, form, kinds
        self.ref_size = 1 + 5 + 2 + ref_hash if form == "tagged" else 1 + ref_hash
        self.distinct: dict = {}                     # utf-8 bytes -> id
        self.saved = {"name": 0, "value": 0}
        self.replaced = {"name": 0, "value": 0}

    def ref(self, b: bytes) -> bytes:
        rid = self.distinct.get(b)
        if rid is None:
            if self.form == "seq":                   # spec 3a option (b): a sequential small id
                assert len(self.distinct) < 1 << (8 * self.ref_hash)
                rid = len(self.distinct).to_bytes(self.ref_hash, "big")
            else:
                rid = hashlib.blake2b(b, digest_size=self.ref_hash).digest()
            self.distinct[b] = rid
        if self.form in ("c1", "seq"):
            return b"\xc1" + rid
        return b"\x81\xa4@sym\xc4" + bytes([self.ref_hash]) + rid

    def string(self, s: str, kind: str, out: bytearray) -> None:
        b = s.encode("utf-8")
        inline = str_inline(b)
        if kind in self.kinds and len(b) >= self.min_len and self.ref_size < len(inline):
            out += self.ref(b)
            self.saved[kind] += len(inline) - self.ref_size
            self.replaced[kind] += 1
        else:
            out += inline

    def table_records(self) -> bytes:
        """The symbol table as records: one ``[<bin id>, <str>]`` record per distinct string."""
        out = bytearray()
        for b, rid in self.distinct.items():
            payload = bytearray()
            pack_plain([rid, b.decode("utf-8")], payload)
            out += frame(1, bytes(payload))
        return bytes(out)


#: Tagged canonical values whose bodies hold values (walked); every other tag's body is a scalar
#: encoding (decimal text, a date, a reference ...) whose strings are not ``str`` values.
_CONTAINER_TAGS = ("@tup", "@map")


def pack_value(v, pol: Policy, out: bytearray) -> None:
    if type(v) is str:
        pol.string(v, "value", out)
    elif type(v) is list:
        array_header(len(v), out)
        for x in v:
            pack_value(x, pol, out)
    elif type(v) is dict:
        map_header(len(v), out)
        for tag, body in v.items():
            pack_plain(tag, out)
            if tag in _CONTAINER_TAGS:
                pack_value(body, pol, out)
            else:
                pack_plain(body, out)
    else:
        pack_plain(v, out)


def pack_atom(env: dict, pol: Policy | None, out: bytearray) -> None:
    """An atom envelope; strings of the state map ``s`` (names and values) are candidates."""
    if pol is None:
        pack_plain(env, out)
        return
    map_header(len(env), out)
    for k, v in env.items():
        pack_plain(k, out)
        if k == "s":
            map_header(len(v), out)
            for name, value in v.items():
                pol.string(name, "name", out)
                pack_value(value, pol, out)
        else:
            pack_plain(v, out)


def frame(kind: int, payload: bytes) -> bytes:
    head = RECORD_MAGIC + bytes([kind]) + struct.pack("<II", len(payload), zlib.crc32(payload))
    return head + struct.pack("<I", zlib.crc32(head)) + payload


def compressed(segments: list) -> tuple:
    """(bytes compressed per whole segment, bytes compressed per 64 KiB block)."""
    whole = sum(len(compress(s)) for s in segments)
    blocks = sum(len(compress(s[i:i + BLOCK])) for s in segments for i in range(0, len(s), BLOCK))
    return whole, blocks


def string_census(decoded: list) -> dict:
    """Distinct and total strings of the state maps, by kind and by minimum length."""
    names, values = [], []

    def walk(v):
        if type(v) is str:
            values.append(v)
        elif type(v) is list:
            for x in v:
                walk(x)
        elif type(v) is dict:
            for tag, body in v.items():
                if tag in _CONTAINER_TAGS:
                    walk(body)
    app_names, app_values = [], []                  # of DBObject records (alias 19) only
    for env in decoded:
        if env is None:
            continue
        app = env["t"] == 19
        before = len(values)
        for name, value in env["s"].items():
            names.append(name)
            if app:
                app_names.append(name)
            walk(value)
        if app:
            app_values.extend(values[before:])
    out = {}
    for kind, items in (("name", names), ("value", values), ("dbobject_name", app_names),
                        ("dbobject_value", app_values)):
        enc = [s.encode("utf-8") for s in items]
        out[kind] = {"total": len(enc), "distinct": len(set(enc)), "inline_bytes": sum(len(str_inline(b)) for b in enc)}
        for L in LENGTHS:
            long_ = [b for b in enc if len(b) >= L]
            out[kind][f"len>={L}"] = {"total": len(long_), "distinct": len(set(long_)),
                                      "inline_bytes": sum(len(str_inline(b)) for b in long_)}
    return out


def measure(path: str) -> dict:
    segs = read_segments(path)
    decoded = []                                     # per record, segment order
    type_counts: dict = {}
    for _, _, records, _ in segs:
        for kind, payload in records:
            if kind != 1:
                decoded.append(None)
                continue
            env = msgpack.unpackb(payload, raw=False, strict_map_key=False)
            again = bytearray()
            pack_atom(env, None, again)
            assert bytes(again) == payload, "the simulation's packer does not reproduce a payload"
            decoded.append(env)
            t = env["t"]
            row = type_counts.setdefault(str(t), {"records": 0, "bytes": 0})
            row["records"] += 1
            row["bytes"] += RECORD_HEADER + len(payload)
    real = [raw for _, _, _, raw in segs]
    a_bytes = sum(len(r) for r in real)
    c_whole, c_block = compressed(real)
    result = {"segments": len(segs), "records": sum(len(r) for _, _, r, _ in segs), "record_types": type_counts,
              "a": a_bytes, "c_whole": c_whole, "c_block": c_block, "strings": string_census(decoded),
              "sha256": hashlib.sha256(b"".join(real)).hexdigest(), "variants": []}
    configs = []
    for ref_hash, form in ((16, "c1"), (8, "c1"), (16, "tagged"), (2, "seq")):
        for L in LENGTHS:
            for kinds in (("name", "value"), ("name",), ("value",)):
                configs.append((L, ref_hash, form, kinds))
    def simulate(pol: Policy) -> list:
        sim, it = [], iter(decoded)
        for _, header, records, _ in segs:
            buf = bytearray(header)
            for kind, payload in records:
                env = next(it)
                if env is None:
                    buf += frame(kind, payload)
                    continue
                p = bytearray()
                pack_atom(env, pol, p)
                buf += frame(kind, bytes(p))
            sim.append(bytes(buf))
        return sim
    # A policy that replaces nothing must rebuild the written segments byte for byte.
    assert simulate(Policy(1 << 30, 16, "c1", ("name", "value"))) == real, "the simulation does not rebuild the segments"
    for L, ref_hash, form, kinds in configs:
        pol = Policy(L, ref_hash, form, kinds)
        sim = simulate(pol)
        table = pol.table_records()
        table_seg = (bytes(SEGMENT_HEADER) + table) if table else b""
        b_records = sum(len(s) for s in sim)
        d_whole, d_block = compressed(sim + ([table_seg] if table_seg else []))
        result["variants"].append({
            "L": L, "ref_size": pol.ref_size, "ref_form": f"{form}/{ref_hash}", "kinds": "+".join(kinds),
            "distinct_replaced": len(pol.distinct), "replaced": pol.replaced, "saved_inline": pol.saved,
            "b_records": b_records, "b_table": len(table_seg), "b": b_records + len(table_seg),
            "d_whole": d_whole, "d_block": d_block})
    return result


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("workdir")
    ap.add_argument("--json")
    ap.add_argument("--seed", type=int, default=20261114)
    ap.add_argument("--partners", type=int, default=1500)
    ap.add_argument("--products", type=int, default=600)
    ap.add_argument("--orders", type=int, default=2000)
    args = ap.parse_args()
    if os.path.exists(args.workdir):
        sys.exit(f"{args.workdir} exists; give a fresh directory")
    # Reproducible bytes: seeded UUIDs (oids, segment names, origins) and a commit clock that
    # advances one second per call.
    uid_rnd = random.Random(args.seed ^ 0x5EED)
    uuid.uuid4 = lambda: uuid.UUID(int=uid_rnd.getrandbits(128), version=4)
    import proto_db.transactions as transactions
    clock = [datetime.datetime(2026, 11, 14, 12, 0, 0, tzinfo=datetime.timezone.utc)]

    def fixed_now():
        clock[0] += datetime.timedelta(seconds=1)
        return clock[0]
    transactions._utc_now = fixed_now

    data = build_data(args.seed, args.partners, args.products, args.orders)
    n_lines = sum(len(o["lines"]) for o in data["orders"])
    objects = {"partners": len(data["partners"]), "products": len(data["products"]),
               "orders": len(data["orders"]), "order_lines": n_lines}
    path = os.path.join(args.workdir, "space")
    os.makedirs(args.workdir)
    write_space(path, data)
    check = verify_space(path, data, random.Random(args.seed + 1))
    m = measure(path)
    ok = check["counts"] == objects and check["sample_failure_count"] == 0
    report = {"ok": ok, "gil": sys._is_gil_enabled(), "python": sys.version.split()[0], "compressor": COMPRESSOR,
              "block_bytes": BLOCK, "seed": args.seed, "objects": objects, "objects_total": sum(objects.values()),
              "read_back": check, **m}
    print(f"objects written: {objects} (total {report['objects_total']}); read back: {check['counts']}; "
          f"sample mismatches: {check['sample_failure_count']}")
    print(f"segments: {m['segments']}, records: {m['records']}, sha256 of the segments: {m['sha256']}")
    print("records per type tag:", json.dumps(m["record_types"]))
    for kind in ("name", "value", "dbobject_name", "dbobject_value"):
        s = m["strings"][kind]
        print(f"strings ({kind}s): {s['total']} total, {s['distinct']} distinct, {s['inline_bytes']} inline bytes; "
              + "; ".join(f"len>={L}: {s[f'len>={L}']['total']} total, {s[f'len>={L}']['distinct']} distinct, "
                          f"{s[f'len>={L}']['inline_bytes']} B" for L in LENGTHS))
    print(f"compressor: {COMPRESSOR}")
    print(f"(a) inline: {m['a']} B   (c) whole segments: {m['c_whole']} B   (c) 64 KiB blocks: {m['c_block']} B")
    print(f"{'ref':>10} {'kinds':>10} {'L':>3} {'distinct':>8} {'saved_names':>11} {'saved_vals':>10} "
          f"{'(b)':>9} {'table':>7} {'(d) whole':>9} {'(d) block':>9} {'d/c block':>9}")
    for v in m["variants"]:
        print(f"{v['ref_form']:>10} {v['kinds']:>10} {v['L']:>3} {v['distinct_replaced']:>8} "
              f"{v['saved_inline']['name']:>11} {v['saved_inline']['value']:>10} {v['b']:>9} {v['b_table']:>7} "
              f"{v['d_whole']:>9} {v['d_block']:>9} {v['d_block'] / m['c_block']:>9.4f}")
    if args.json:
        with open(args.json, "w") as f:
            json.dump(report, f, indent=1, default=str)
    print("OK" if ok else "FAILED")
    if not ok:
        print(json.dumps(check, default=str))
        sys.exit(1)


if __name__ == "__main__":
    main()
