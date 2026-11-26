"""M6: the catalog install paths, per commit, on FileStorage.

Arm "catalog": a database with the demo catalog (acme_sales + acme_delivery) installs plain modules
(no semantic declarations): each install commit recomposes the stored catalog under the root lock
although the catalog does not change.  Arm "plain": a database without a catalog installs the same
plain modules (no recomposition).  Also: trivial root writes, the demo modules' first installs and an
idempotent reinstall (planned as nothing to add: a commit with no registry change).

Historical: this probe ran at commit 6b0697a, before the precondition concurrency model (D73).  It uses
``set_root_object(..., replace=True)``, which was removed with that model, so it no longer runs on the
current code as it is; it is kept unchanged as the record of the measurement it produced.  On the current
code, drop ``replace=True`` (an object root is always replaced, compared against its base value)."""
import os, sys, time, json, statistics
REPO, WORK, N = sys.argv[1], sys.argv[2], int(sys.argv[3])
sys.path.insert(0, REPO); sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from proto_db import FileStorage, ObjectSpace
from docs.kit.semantic.queries import MAPPING

PLAIN = {f"m6_plain_{i}": f"m6plain.p{i}" for i in range(1, N + 1)}

def timed(db, fn, n=1):
    out = []
    for _ in range(n):
        t = time.perf_counter(); db.run(fn); out.append(time.perf_counter() - t)
    return out

def stats(xs):
    return {"n": len(xs), "median_ms": round(statistics.median(xs) * 1e3, 3),
            "min_ms": round(min(xs) * 1e3, 3), "max_ms": round(max(xs) * 1e3, 3)}

def write_fn():
    c = [0]
    def write(tr):
        c[0] += 1; tr.set_root_object("probe", c[0], replace=True)
    return write

def arm(name, demo):
    space = ObjectSpace(FileStorage(os.path.join(WORK, name)), modules_mapping={**MAPPING, **PLAIN})
    db = space.new_database("acme")
    r = {}
    w = write_fn(); timed(db, w, 3)
    r["trivial_write"] = stats(timed(db, w, N))
    if demo:
        r["first_install_acme_sales"] = stats(timed(db, lambda tr: tr.install_module("acme_sales")))
        r["first_install_acme_delivery"] = stats(timed(db, lambda tr: tr.install_module("acme_delivery")))
        r["idempotent_reinstall"] = stats(timed(db, lambda tr: tr.install_module("acme_delivery"), N))
    for m in PLAIN:                      # import outside the timing: the commit is what is measured
        __import__(PLAIN[m])
    r["install_plain_module"] = stats([timed(db, lambda tr, m=m: tr.install_module(m))[0] for m in PLAIN])
    r["trivial_write_after"] = stats(timed(db, w, N))
    tr = db.new_transaction(); r["modules"] = len(tr.installed_modules()); tr.abort()
    space.close()
    return r

print(json.dumps({"plain_then_catalog": {"plain": arm("plain", False), "catalog": arm("catalog", True)}}, indent=1))
