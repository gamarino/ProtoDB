"""Property tests: order_key is a total order consistent with equality and hashing."""
import uuid
from datetime import timedelta

from hypothesis import given, settings, strategies as st

from proto_db.values import MISSING, hash_key, order_key, values_equal


class _Atom:
    def __init__(self, oid):
        self.oid = oid


scalars = st.one_of(
    st.just(MISSING), st.none(), st.booleans(),
    st.integers(min_value=-2**70, max_value=2**70),
    st.floats(allow_nan=False),
    st.decimals(allow_nan=False),
    st.text(max_size=6), st.binary(max_size=6),
    st.dates(), st.times(), st.times(timezones=st.timezones()),
    st.datetimes(), st.datetimes(timezones=st.timezones()),
    st.timedeltas(min_value=timedelta(days=-1000), max_value=timedelta(days=1000)),
    st.uuids(), st.integers(0, 5).map(lambda i: _Atom(uuid.UUID(int=i))),
)
values = st.recursive(scalars, lambda c: st.lists(c, max_size=3).map(tuple), max_leaves=6)
CFG = settings(max_examples=300, deadline=None)


@CFG
@given(values, values)
def test_antisymmetric_and_total(a, b):
    ka, kb = order_key(a), order_key(b)
    assert (ka <= kb) or (kb <= ka)
    if ka <= kb and kb <= ka:
        assert ka == kb


@CFG
@given(values, values, values)
def test_transitive(a, b, c):
    ka, kb, kc = order_key(a), order_key(b), order_key(c)
    if ka <= kb and kb <= kc:
        assert ka <= kc


@CFG
@given(values, values)
def test_equal_implies_same_hash(a, b):
    if values_equal(a, b):
        assert hash_key(a) == hash_key(b)
    assert values_equal(a, b) == (order_key(a) == order_key(b))


@CFG
@given(st.lists(values, max_size=15))
def test_sorting_never_raises(xs):
    ordered = sorted(xs, key=order_key)
    keys = [order_key(x) for x in ordered]
    assert keys == sorted(keys)
