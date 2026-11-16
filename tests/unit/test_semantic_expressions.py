"""Unit tests for proto_db.semantic.expressions (spec section 7, format spec "The expression language").

Catches: an operator evaluated differently from the normative chapter (arithmetic that rounds a
decimal or accepts a float or a bool, a comparison that disagrees with the planner's ``Term`` on
MISSING, None or mixed types, logic that does not count MISSING as false or accepts a non-boolean),
aggregates whose empty sum is not the int 0 or that count a MISSING term, ``via`` that does not
reach through two hops or dereference identity handles, ``root``/``cell`` accepted outside a query,
an operator outside the subset accepted, a reserved operator not refused as unsupported,
``to_planner`` translating a predicate the index cannot answer exactly (two attributes, a field with
a catalogued default), ``dependencies`` naming what an expression reads from its members, and a text
form that parses Python's grammar instead of the subset's.
"""
import datetime
from decimal import Decimal

import pytest

from proto_db.collections.lists import List
from proto_db.collections.sets import Set
from proto_db.core.dbobject import DBObject
from proto_db.errors import ProtoNotSupportedError, ProtoUserError, ProtoValidationError
from proto_db.query.expressions import And, Not, Or
from proto_db.query.terms import Term
from proto_db.semantic.expressions import MAX_EXPRESSION_DEPTH, Reader, check_ast, dependencies, evaluate, parse, \
    to_planner
from proto_db.values import MISSING, MutableObject


def attr(*names):
    return ["attr", *names]


def const(value):
    return ["const", value]


ELEMENT = DBObject(i=7, j=2, d=Decimal("1.50"), s="open", flag=True, nil=None, f=1.5, b=False)

#: ``(id, ast, expected)`` evaluated on :data:`ELEMENT` without a reader.
OPERATORS = [
    ("const", const(5), 5),
    ("attr", attr("s"), "open"),
    ("attr-absent", attr("nope"), MISSING),
    ("plus", ["+", attr("i"), attr("j")], 9),
    ("minus", ["-", attr("j"), attr("i")], -5),
    ("times", ["*", attr("i"), attr("j")], 14),
    ("eq", ["==", attr("s"), const("open")], True),
    ("ne", ["!=", attr("s"), const("open")], False),
    ("lt", ["<", attr("j"), attr("i")], True),
    ("le", ["<=", attr("i"), const(7)], True),
    ("gt", [">", attr("j"), attr("i")], False),
    ("ge", [">=", attr("d"), const(Decimal("1.5"))], True),
    ("and", ["and", attr("flag"), ["<", attr("j"), attr("i")]], True),
    ("or", ["or", attr("b"), ["==", attr("i"), const(8)]], False),
    ("not", ["not", attr("b")], True),
    ("eq-nil", ["==", attr("nil"), const(None)], True),
]


@pytest.mark.parametrize("ast, expected", [(a, e) for _, a, e in OPERATORS], ids=[i for i, _, _ in OPERATORS])
def test_each_operator(ast, expected):
    check_ast(ast)
    result = evaluate(ast, ELEMENT)
    assert result is expected if expected is MISSING else (type(result) is type(expected) and result == expected)


def test_int_and_decimal_arithmetic_is_exact():
    big = Decimal("12345678901234567890.123456789012345678901")
    result = evaluate(["+", const(big), const(1)], ELEMENT)
    assert type(result) is Decimal and str(result) == "12345678901234567891.123456789012345678901"
    product = evaluate(["*", const(Decimal("0.1")), const(3)], ELEMENT)
    assert type(product) is Decimal and str(product) == "0.3"
    # the default decimal context would round this product to 28 digits
    assert str(evaluate(["*", const(big), const(big)], ELEMENT)) == str(_exact_product(big, big))
    assert evaluate(["*", const(10 ** 30), const(10 ** 30)], ELEMENT) == 10 ** 60
    assert type(evaluate(["-", const(Decimal("2")), const(Decimal("2"))], ELEMENT)) is Decimal


def _exact_product(a: Decimal, b: Decimal) -> Decimal:
    sign_a, digits_a, exp_a = a.as_tuple()
    sign_b, digits_b, exp_b = b.as_tuple()
    product = int("".join(map(str, digits_a))) * int("".join(map(str, digits_b)))
    return Decimal((sign_a ^ sign_b, tuple(int(c) for c in str(product)), exp_a + exp_b))


def test_missing_propagates_and_comparisons_are_false():
    for op in ("+", "-", "*"):
        assert evaluate([op, attr("nope"), const(1)], ELEMENT) is MISSING
        assert evaluate([op, const(Decimal("1")), attr("nope")], ELEMENT) is MISSING
    for op in ("==", "!=", "<", "<=", ">", ">="):
        assert evaluate([op, attr("nope"), const(1)], ELEMENT) is False
        assert evaluate([op, const(1), attr("nope")], ELEMENT) is False
        assert evaluate([op, attr("nope"), attr("nope")], ELEMENT) is False
    assert evaluate(["not", attr("nope")], ELEMENT) is True
    assert evaluate(["and", attr("nope"), const(True)], ELEMENT) is False
    assert evaluate(["or", attr("nope"), const(True)], ELEMENT) is True
    with pytest.raises(ProtoValidationError, match="'and'"):
        evaluate(["and", const(True), attr("i")], ELEMENT)
    with pytest.raises(ProtoValidationError, match="'not'"):
        evaluate(["not", attr("s")], ELEMENT)
    # left to right: stopping at the first false (true for or) reaches no later operand
    assert evaluate(["and", attr("b"), attr("i")], ELEMENT) is False
    assert evaluate(["or", attr("flag"), attr("i")], ELEMENT) is True


def test_float_and_division_are_refused():
    for ast in (["+", attr("f"), const(1)], ["*", const(2), const(0.5)], ["-", attr("d"), attr("f")],
                ["+", attr("flag"), const(1)], ["+", attr("s"), const("x")], ["+", attr("nil"), const(1)]):
        with pytest.raises(ProtoValidationError, match=r"'[-+*]'"):
            evaluate(ast, ELEMENT)
    for bad in (["/", attr("i"), const(2)], ["//", attr("i"), const(2)], ["%", attr("i"), const(2)]):
        with pytest.raises(ProtoValidationError, match=repr(bad[0])):
            check_ast(bad)
        with pytest.raises(ProtoValidationError, match=repr(bad[0])):
            evaluate(bad, ELEMENT)


def test_root_and_cell_only_in_queries():
    for origin in (["root", "orders"], ["cell", "acme.Customer#" + "0" * 32]):
        ast = ["count", ["via", origin, "lines"]]
        with pytest.raises(ProtoValidationError, match=repr(origin[0])):
            check_ast(ast)
        check_ast(ast, in_query=True)
    check_ast(["count", ["via", ["self"], "lines"]])


def test_reserved_operators_are_not_supported_and_unknown_ones_invalid():
    for op in ("in", "is_missing", "min", "max", "exists", "all", "reach", "valid_at"):
        with pytest.raises(ProtoNotSupportedError, match=repr(op)):
            check_ast([op, attr("x")])
        with pytest.raises(ProtoNotSupportedError, match=repr(op)):
            evaluate([op, attr("x")], ELEMENT)
    for bad in (["frobnicate", 1], [], "attr", ["attr"], ["attr", ""], ["+", const(1)], ["not", 1, 2],
                ["sum", "lines"], ["count"], ["via", ["self"]], ["via", ["here"], "x"], ("const", 1),
                ["const", 1, 2], ["and"]):
        with pytest.raises(ProtoValidationError):
            check_ast(bad)


class _Cells(Reader):
    def __init__(self, cells, defaults=None):
        self.cells = cells
        self.defaults = defaults or {}

    def cell(self, name):
        return self.cells.get(name)

    def root(self, name):
        return self.cells.get(name)

    def default(self, element, name):
        return self.defaults.get((element.oid, name), MISSING)


def _domain():
    products = {name: DBObject(sku=name, price=Decimal(price)) for name, price in (("p1", "2.50"), ("p2", "4"))}
    cells = {f"acme.Product#{name}": product for name, product in products.items()}
    handle = {name: MutableObject(f"acme.Product#{name}") for name in products}
    lines_a = List.from_iterable([DBObject(quantity=2, product=handle["p1"]),
                                  DBObject(quantity=1, product=handle["p2"])])
    lines_b = List.from_iterable([DBObject(quantity=3, product=handle["p1"]), DBObject(product=handle["p2"])])
    orders = [DBObject(state="open", lines=lines_a), DBObject(state="cancelled", lines=lines_b)]
    cells["acme.Order#a"], cells["acme.Order#b"] = orders
    customer = DBObject(name="ACME", orders=Set.from_iterable([MutableObject("acme.Order#a"),
                                                               MutableObject("acme.Order#b")]))
    return customer, orders, products, _Cells(cells)


def test_via_two_hops_through_identity_handles():
    customer, orders, products, reader = _domain()
    lines = evaluate(["via", ["self"], "orders", "lines"], customer, reader=reader)
    assert isinstance(lines, tuple) and len(lines) == 4
    assert sorted(line["quantity"] or 0 for line in lines) == [0, 1, 2, 3]
    reached = evaluate(["via", ["self"], "orders", "lines", "product"], customer, reader=reader)
    assert {p["sku"] for p in reached} == {"p1", "p2"} and len(reached) == 2          # a set: once each
    assert evaluate(["attr", "product", "price"], orders[0]["lines"].get_at(0), reader=reader) == Decimal("2.50")
    with pytest.raises(ProtoUserError, match="reader"):
        evaluate(["via", ["self"], "orders", "lines"], customer)
    with pytest.raises(ProtoUserError, match="reader"):
        evaluate(["attr", "product", "price"], orders[0]["lines"].get_at(0))


def test_sum_and_count_with_where():
    customer, orders, _, reader = _domain()
    assert evaluate(["sum", "lines", attr("quantity")], orders[0]) == 3
    assert evaluate(["sum", "lines", attr("quantity")], orders[1]) == 3          # MISSING contributes nothing
    assert evaluate(["count", "lines"], orders[1]) == 2
    assert evaluate(["sum", "nothing", attr("quantity")], orders[0]) == 0
    assert type(evaluate(["count", "nothing"], orders[0])) is int
    amount = ["*", attr("quantity"), attr("product", "price")]
    total = evaluate(["sum", "lines", amount], orders[0], reader=reader)
    assert type(total) is Decimal and total == Decimal("9.00")
    open_lines = ["via", ["self"], "orders", "lines"]
    where = ["==", attr("quantity"), const(3)]
    assert evaluate(["sum", open_lines, attr("quantity"), where], customer, reader=reader) == 3
    assert evaluate(["count", open_lines, ["!=", attr("quantity"), const(3)]], customer, reader=reader) == 2
    with pytest.raises(ProtoValidationError, match="where"):
        evaluate(["count", "lines", attr("quantity")], orders[0])


def test_absent_value_reads_the_catalogued_default_never_none():
    element = DBObject(i=1)
    reader = _Cells({}, {(element.oid, "credit_limit"): Decimal("100")})
    assert evaluate(attr("credit_limit"), element, reader=reader) == Decimal("100")
    assert evaluate(attr("credit_limit"), element) is MISSING
    assert evaluate(attr("i"), element, reader=reader) == 1
    assert evaluate(attr("other"), element, reader=reader) is MISSING


@pytest.mark.parametrize("value", [None, True, 0, 1, 1.0, Decimal("1.0"), 2, "a", "b", b"a",
                                   datetime.date(2026, 6, 1), (1, 2), [1, 2], {"k": 1}, float("nan")])
def test_comparisons_agree_with_the_planner_term(value):
    for key in (MISSING, None, False, 1, Decimal("1"), 1.5, "a", datetime.date(2026, 6, 1), (1, 2), [1], {"k": 1}):
        element = DBObject(x=key) if key is not MISSING else DBObject()
        for op in ("==", "!=", "<", "<=", ">", ">="):
            ast = [op, attr("x"), const(value)]
            term = to_planner(ast)
            got = evaluate(ast, element)
            if term is not None:
                assert term.matches(element) is got, (key, op, value)
            mirrored = {"<": ">", "<=": ">=", ">": "<", ">=": "<="}.get(op, op)
            assert evaluate([mirrored, const(value), attr("x")], element) is got, (key, op, value)


def test_to_planner_translates_simple_predicates():
    d = datetime.date(2026, 6, 1)
    assert to_planner(["<", ["attr", "date"], ["const", d]]) == Term("date", "<", d)
    assert to_planner(["<", ["const", d], ["attr", "date"]]) == Term("date", ">", d)
    both = ["and", ["==", attr("state"), const("open")], ["not", [">=", attr("n"), const(3)]],
            ["or", ["==", attr("a"), const(1)], ["!=", attr("b"), const(2)]]]
    assert to_planner(both) == And(Term("state", "==", "open"), Not(Term("n", ">=", 3)),
                                   Or(Term("a", "==", 1), Term("b", "!=", 2)))
    assert to_planner(["and", ["==", attr("state"), const("open")]]) == Term("state", "==", "open")


def test_to_planner_returns_none_for_two_attributes():
    assert to_planner(["<=", attr("committed"), attr("credit_limit")]) is None
    assert to_planner(["and", ["==", attr("s"), const(1)], ["<", attr("a"), attr("b")]]) is None
    assert to_planner(["==", attr("order", "date"), const(1)]) is None              # a path: no Term step
    assert to_planner(["==", attr("x"), const([1])]) is None                        # no index answers it
    assert to_planner(["==", ["+", attr("x"), const(1)], const(2)]) is None
    assert to_planner(attr("flag")) is None
    # a field with a catalogued default: the index sees MISSING where evaluation sees the default
    assert to_planner(["==", attr("state"), const("open")], defaults={"state"}) is None
    assert to_planner(["==", attr("other"), const("open")], defaults={"state"}) == Term("other", "==", "open")


def test_dependencies_of_sum_over_a_role():
    assert dependencies(["sum", "lines", attr("amount")]) == frozenset({"lines"})
    assert dependencies(["<=", attr("committed"), attr("credit_limit")]) == frozenset({"committed", "credit_limit"})
    assert dependencies(["count", ["via", ["self"], "orders", "lines"], ["==", attr("x"), const(1)]]) == {"orders"}
    assert dependencies(["count", ["via", ["root", "orders"], "lines"]]) == frozenset()
    assert dependencies(["*", attr("quantity"), attr("product", "price")]) == {"quantity", "product"}


def test_parse_the_briefs_texts():
    assert parse("committed <= credit_limit") == ["<=", ["attr", "committed"], ["attr", "credit_limit"]]
    assert parse("state == 'open'") == ["==", ["attr", "state"], ["const", "open"]]
    assert parse("quantity * price") == ["*", ["attr", "quantity"], ["attr", "price"]]
    assert parse("state != 'cancelled'") == ["!=", ["attr", "state"], ["const", "cancelled"]]
    assert parse("order.date >= 2") == [">=", ["attr", "order", "date"], ["const", 2]]
    assert parse("a + b * c - 1.50") == ["-", ["+", ["attr", "a"], ["*", ["attr", "b"], ["attr", "c"]]],
                                         ["const", Decimal("1.50")]]
    assert parse("(a + b) * -2") == ["*", ["+", ["attr", "a"], ["attr", "b"]], ["const", -2]]
    assert parse("not a < 1 and b or c == \"x\"") == [
        "or", ["and", ["not", ["<", ["attr", "a"], ["const", 1]]], ["attr", "b"]], ["==", ["attr", "c"], ["const", "x"]]]
    assert parse("a and b and c") == ["and", ["attr", "a"], ["attr", "b"], ["attr", "c"]]
    assert parse("'it\\'s'") == ["const", "it's"]
    for text in ("committed <= credit_limit", "a + b * c - 1.50", "not a < 1 and b or c == 'x'"):
        check_ast(parse(text))


def test_parse_refuses_in_and_calls():
    cases = {"state in ('open','confirmed')": 7, "sum(lines)": 4, "a / b": 3, "a < b < c": 7, "1.5e3": 1,
             "a ** 2": 3, "x == None": 6, "a is b": 3, "a == ": 6, "'open": 1, "a.": 2, "1_000": 1, "a b": 3,
             "lambda: 1": 1, "a[0]": 2, "": 1, ".5": 1, "-a": 1}
    for text, column in cases.items():
        with pytest.raises(ProtoValidationError, match=f"column {column}\\b"):
            parse(text)


# -- fix round 1 (review I1, I2, M1-M7) -------------------------------------------------------------

def _str(ast, element=ELEMENT, reader=None):
    return str(evaluate(ast, element, reader=reader))


def test_decimal_exponents_follow_the_general_decimal_arithmetic():
    """+ and - take the smaller exponent (an int has exponent 0), * the sum of the exponents; a sum is a
    left fold from the int 0 (I1)."""
    assert _str(["+", const(1), const(Decimal("1.50"))]) == "2.50"
    assert _str(["*", const(Decimal("1.50")), const(7)]) == "10.50"
    assert _str(["+", const(Decimal("1E+1")), const(2)]) == "12"
    assert _str(["+", const(Decimal("1E+1")), const(Decimal("1E+2"))]) == "1.1E+2"
    assert _str(["*", const(Decimal("1E+1")), const(Decimal("1E+1"))]) == "1E+2"
    assert _str(["-", const(Decimal("0.10")), const(Decimal("0.10"))]) == "0.00"
    tens = DBObject(lines=List.from_iterable([DBObject(v=Decimal("1E+1")), DBObject(v=Decimal("2E+1"))]))
    assert _str(["sum", "lines", attr("v")], tens) == "30"                   # 0 + 1E+1 + 2E+1
    one = DBObject(lines=List.from_iterable([DBObject(v=Decimal("1E+1"))]))
    assert _str(["sum", "lines", attr("v")], one) == "10"


def test_wrong_kind_raises_before_missing_propagates():
    for ast in (["+", attr("nope"), const("x")], ["*", const(1.5), attr("nope")], ["-", attr("nope"), attr("flag")]):
        with pytest.raises(ProtoValidationError):
            evaluate(ast, ELEMENT)


def test_non_finite_decimals_are_refused_in_arithmetic():
    for value in (Decimal("NaN"), Decimal("Infinity"), Decimal("-Infinity")):
        with pytest.raises(ProtoValidationError, match="finite"):
            evaluate(["+", const(value), const(1)], ELEMENT)


def test_via_identity_order_and_absent_cells():
    record = DBObject(n=1)
    other = DBObject(n=2)
    h1, h2, gone = MutableObject("acme.P#1"), MutableObject("acme.P#2"), MutableObject("acme.P#gone")
    reader = _Cells({"acme.P#1": record, "acme.P#2": other})
    # a record and a handle reached twice count once; first-reached order; an absent cell contributes nothing
    element = DBObject(items=List.from_iterable([other, h1, gone, other, h1, h2]))
    reached = evaluate(["via", ["self"], "items"], element, reader=reader)
    assert [m["n"] for m in reached] == [2, 1, 2]                      # `other` once by oid, h2's record once
    # version-bound references are identified by (entity, version)
    v1, v2 = DBObject(n=10), DBObject(n=11)
    bound = DBObject(items=List.from_iterable([{"entity": h1, "version": v1}, {"entity": h1, "version": v1},
                                              {"entity": h1, "version": v2}]))
    assert [m["n"] for m in evaluate(["via", ["self"], "items"], bound, reader=reader)] == [10, 11]
    # anything else in a role is not a member
    with pytest.raises(ProtoValidationError, match="role"):
        evaluate(["via", ["self"], "items"], DBObject(items=List.from_iterable([1, 2])))
    with pytest.raises(ProtoValidationError, match="role"):
        evaluate(["count", "name"], DBObject(name="ACME"))


def test_root_origin_is_deduplicated_and_a_bare_root_is_an_expression():
    record = DBObject(n=1)
    h = MutableObject("acme.P#1")
    reader = _Cells({"orders": List.from_iterable([h, record, h, record]), "acme.P#1": DBObject(n=2)})
    assert [m["n"] for m in evaluate(["root", "orders"], ELEMENT, reader=reader)] == [2, 1]
    assert evaluate(["count", ["root", "orders"]], ELEMENT, reader=reader) == 2
    check_ast(["count", ["root", "orders"]], in_query=True)
    with pytest.raises(ProtoValidationError, match="'root'"):
        check_ast(["count", ["root", "orders"]])
    with pytest.raises(ProtoValidationError, match="'cell'"):
        check_ast(["cell", "x"], in_query=True)                         # a cell is an origin only


def test_parse_takes_ascii_digits_only():
    for text, column in (("x == ٣", 6), ("²", 1), ("1٣", 1)):
        with pytest.raises(ProtoValidationError, match=f"column {column}\\b"):
            parse(text)


def _nested(op: str, depth: int) -> list:
    tree = attr("flag")
    for _ in range(depth - 1):
        tree = [op, tree] if op == "not" else [op, tree, const(True)]
    return tree


@pytest.mark.parametrize("op", ["not", "and", "or"])
def test_expression_depth_limit(op):
    """check_ast enforces MAX_EXPRESSION_DEPTH; evaluate handles a tree at the limit and refuses one past it."""
    at_limit, past = _nested(op, MAX_EXPRESSION_DEPTH), _nested(op, MAX_EXPRESSION_DEPTH + 1)
    check_ast(at_limit)
    assert evaluate(at_limit, ELEMENT) in (True, False)
    for call in (check_ast, lambda tree: evaluate(tree, ELEMENT)):
        with pytest.raises(ProtoValidationError, match="deeper"):
            call(past)


def test_sum_of_terms_depth_limit():
    terms = ["sum", "lines", _nested("not", MAX_EXPRESSION_DEPTH - 1)]
    check_ast(terms)
    with pytest.raises(ProtoValidationError, match="deeper"):
        check_ast(["sum", "lines", _nested("not", MAX_EXPRESSION_DEPTH)])


def test_parse_depth_limit():
    n = MAX_EXPRESSION_DEPTH
    assert parse("(" * n + "a" + ")" * n) == ["attr", "a"]
    assert check_ast(parse("not " * (n - 1) + "a")) is None
    assert evaluate(parse(" + ".join(["i"] * n)), ELEMENT) == 7 * n              # depth n - 1
    for text in ("(" * (n + 1) + "a" + ")" * (n + 1), "not " * n + "a", " + ".join(["i"] * (n + 2))):
        with pytest.raises(ProtoValidationError, match="deeper"):
            parse(text)
