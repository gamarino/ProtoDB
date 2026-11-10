"""Per-edge cost of the candidate mechanisms, against a plain attribute read.

python -X gil=0 micro.py   (run from the repository)
- direct: node.left is a plain instance attribute (today);
- link (option a): node.left is absent from __dict__; __getattr__ resolves (pointer, class) held in
  _links through the space's BoundedCache (get = lock + OrderedDict.move_to_end), as 8.3l does;
- weak (option b): node.left resolves a weakref.ref held in _links (no cache lookup when alive);
- key_get on a real 100k Dictionary (17-18 levels), for scale.
Each in 1 thread and in 8 threads (aggregate ns per access, i.e. wall / total accesses).
"""
import os
import sys
import threading
import time
import weakref

sys.path.insert(0, os.getcwd())
from proto_db.storage.cache import BoundedCache  # noqa

N = 2_000_000


class Direct:
    def __init__(self, left):
        self.left = left


class Linked:
    cache = None

    def __init__(self, key):
        self._links = {"left": (key, Direct)}

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        links = self.__dict__.get("_links")
        if links is not None and name in links:
            ref = links[name]
            atom = Linked.cache.get(ref[0])
            if atom is not None and type(atom) is ref[1]:
                return atom
            return None
        raise AttributeError(name)


class Weak:
    def __init__(self, target):
        self._links = {"left": weakref.ref(target)}

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        links = self.__dict__.get("_links")
        if links is not None and name in links:
            atom = links[name]()
            if atom is not None:
                return atom
            return None
        raise AttributeError(name)


def bench(objs, threads):
    per = N // threads

    def run():
        o = objs
        n = len(o)
        for i in range(per):
            o[i % n].left

    ts = [threading.Thread(target=run) for _ in range(threads)]
    t0 = time.perf_counter()
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    return (time.perf_counter() - t0) * 1e9 / (per * threads)


target = Direct(None)
cache = BoundedCache(50_000, sizeof=lambda v: 1)
Linked.cache = cache
for k in range(1000):
    cache.put(("p", k), target)
direct = [Direct(target) for _ in range(1000)]
linked = [Linked(("p", k)) for k in range(1000)]
weak = [Weak(target) for _ in range(1000)]
res = {}
for name, objs in (("direct", direct), ("link_via_cache", linked), ("weakref", weak)):
    for th in (1, 8):
        bench(objs, th)
        res[f"{name}_{th}t_ns"] = round(min(bench(objs, th) for _ in range(3)), 1)
print(res)
