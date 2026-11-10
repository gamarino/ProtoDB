"""Property tests: codec round-trip is exact for arbitrary nested canonical values."""
import uuid
from datetime import timedelta, timezone

import msgpack
from hypothesis import given, settings, strategies as st

from proto_db.core import codec
from proto_db.core.atom import Atom, AtomPointer
from proto_db.values import MISSING
from tests.support.exact import assert_exact


class PropLeaf(Atom):
    """Atom used as a nested reference in generated values."""

    __type_name__ = "tests.codec_roundtrip.PropLeaf"


LEAVES = [PropLeaf() for _ in range(4)]

timezones = st.one_of(
    st.none(), st.just(timezone.utc), st.timezones(),
    st.integers(-86_399_999_999, 86_399_999_999).map(lambda us: timezone(timedelta(microseconds=us))),
    st.tuples(st.integers(-1439, 1439), st.text(min_size=1, max_size=5)).map(
        lambda t: timezone(timedelta(minutes=t[0]), t[1])),
)
scalars = st.one_of(
    st.just(MISSING), st.none(), st.booleans(),
    st.integers(), st.integers(min_value=-2**200, max_value=2**200),
    st.floats(allow_nan=True), st.decimals(allow_nan=True, allow_infinity=True),
    st.text(), st.binary(),
    st.dates(), st.times(timezones=timezones), st.datetimes(timezones=timezones, allow_imaginary=True),
    st.timedeltas(), st.uuids(),
    st.sampled_from(LEAVES),
)
keys = st.recursive(
    st.one_of(st.none(), st.booleans(), st.integers(), st.text(max_size=8), st.binary(max_size=8),
              st.dates(), st.uuids(), st.decimals(allow_nan=False)),
    lambda c: st.lists(c, max_size=3).map(tuple), max_leaves=4)
values = st.recursive(
    scalars,
    lambda c: st.one_of(
        st.lists(c, max_size=4),
        st.lists(c, max_size=4).map(tuple),
        st.dictionaries(keys, c, max_size=4),
    ),
    max_leaves=20,
)
state = st.dictionaries(st.text(min_size=1, max_size=10), values, max_size=5)


class _Refs:
    def __init__(self, segment=None):
        self.segment = segment or uuid.uuid4()
        self.by_ptr = {}

    def save_child(self, atom):
        ptr = AtomPointer(self.segment, len(self.by_ptr))
        self.by_ptr[ptr] = atom
        return ptr

    def load_ref(self, ptr, type_name):
        assert type_name == PropLeaf.__type_name__
        return self.by_ptr[ptr]


@settings(max_examples=400, deadline=None)
@given(state)
def test_state_roundtrip_exact_through_msgpack(s):
    refs = _Refs()
    encoded = codec.encode_state(s, refs.save_child)
    wire = msgpack.unpackb(msgpack.packb(encoded, use_bin_type=True), raw=False)
    assert_exact(codec.decode_state(wire, refs.load_ref), s)


@settings(max_examples=200, deadline=None)
@given(values)
def test_encode_is_deterministic(v):
    segment = uuid.uuid4()
    a = codec.encode_state({"v": v}, _Refs(segment).save_child)
    b = codec.encode_state({"v": v}, _Refs(segment).save_child)
    assert msgpack.packb(a, use_bin_type=True) == msgpack.packb(b, use_bin_type=True)
