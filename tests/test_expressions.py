import pytest

from minietl.errors import ConfigError, RowError
from minietl.expressions import Expression


def test_arithmetic_follows_precedence_and_parentheses():
    row = {"price": 2.5, "qty": 4}
    assert Expression("price * qty + 1").evaluate(row) == 11.0   # 10.0 + 1
    assert Expression("1 + price * qty").evaluate(row) == 11.0   # multiplication binds first
    assert Expression("(1 + price) * qty").evaluate(row) == 14.0  # 3.5 * 4


def test_floor_division_and_modulo_use_python_semantics():
    # Floors toward negative infinity: -7 // 2 is -4, and -7 % 2 is 1.
    assert Expression("a // b").evaluate({"a": -7, "b": 2}) == -4
    assert Expression("a % b").evaluate({"a": -7, "b": 2}) == 1


@pytest.mark.parametrize("x, expected", [(0, False), (1, False), (3, True), (5, False), (6, False)])
def test_chained_comparison_is_exclusive_on_both_ends(x, expected):
    assert Expression("1 < x < 5").evaluate({"x": x}) is expected


def test_and_short_circuits_before_dividing_by_zero():
    expression = Expression("x != 0 and 10 / x > 1")
    assert expression.evaluate({"x": 0}) is False   # would raise if the right side ran
    assert expression.evaluate({"x": 5}) is True    # 10 / 5 = 2 > 1
    assert expression.evaluate({"x": 20}) is False  # 10 / 20 = 0.5


def test_or_returns_the_first_truthy_operand_like_python():
    expression = Expression("a or b")
    assert expression.evaluate({"a": 0, "b": "fallback"}) == "fallback"
    assert expression.evaluate({"a": "first", "b": "fallback"}) == "first"


def test_helper_functions():
    assert Expression("upper(strip(name))").evaluate({"name": "  ada "}) == "ADA"
    assert Expression("lower(name)").evaluate({"name": "ADA"}) == "ada"
    assert Expression("len(name)").evaluate({"name": "héllo"}) == 5
    assert Expression("abs(x)").evaluate({"x": -3}) == 3
    assert Expression("round(x, 1)").evaluate({"x": 2.46}) == 2.5


def test_coalesce_skips_none_but_not_falsy_values():
    expression = Expression("coalesce(a, b, 'z')")
    assert expression.evaluate({"a": None, "b": None}) == "z"
    assert expression.evaluate({"a": None, "b": 7}) == 7
    assert expression.evaluate({"a": 0, "b": 7}) == 0


def test_conditional_expression_picks_a_branch():
    expression = Expression("'adult' if age >= 18 else 'minor'")
    assert expression.evaluate({"age": 18}) == "adult"
    assert expression.evaluate({"age": 17}) == "minor"


def test_membership_and_none_tests():
    assert Expression("status in ('new', 'open')").evaluate({"status": "open"}) is True
    assert Expression("status in ('new', 'open')").evaluate({"status": "closed"}) is False
    assert Expression("status not in ('new', 'open')").evaluate({"status": "closed"}) is True
    assert Expression("x is None").evaluate({"x": None}) is True
    assert Expression("x is not None").evaluate({"x": 0}) is True


@pytest.mark.parametrize("text", ["x is 'abc'", "x is not 1000", "x is y", "1 < x is True"])
def test_identity_tests_are_only_allowed_against_none(text):
    # "is" on equal strings/numbers depends on interning, so it would give data-dependent surprises.
    with pytest.raises(ConfigError, match="None"):
        Expression(text)


def test_equality_works_where_identity_is_refused():
    built_at_runtime = "".join(["ab", "c"])
    assert Expression("x == 'abc'").evaluate({"x": built_at_runtime}) is True


def test_absurdly_deep_nesting_is_a_config_error_not_a_crash():
    for text in ("+".join(["a"] * 5000), "not " * 5000 + "a"):
        with pytest.raises(ConfigError):
            Expression(text)


def test_names_lists_columns_but_not_function_names():
    expression = Expression("a + b * len(c) if d else upper(e)")
    assert expression.names == {"a", "b", "c", "d", "e"}


@pytest.mark.parametrize(
    "text",
    [
        "__import__('os').system('x')",  # function call outside the whitelist
        "open('secrets.txt')",
        "a.b",                            # attribute access
        "a[0]",                           # subscript
        "a ** 2",                         # power could build enormous integers
        "~a",
        "a & b",
        "lambda: 1",
        "[x for x in y]",
        "{'a': 1}",
        "f'{a}'",
        "(y := 3)",
        "b'bytes'",
        "1j",
        "round(x=1)",                     # keyword arguments
        "len(*a)",
    ],
)
def test_constructs_outside_the_whitelist_are_refused_at_load_time(text):
    with pytest.raises(ConfigError):
        Expression(text)


@pytest.mark.parametrize("text", ["a +", "(", "1 2"])
def test_syntax_errors_become_config_errors(text):
    with pytest.raises(ConfigError, match="invalid expression"):
        Expression(text)


@pytest.mark.parametrize("text", ["", "   ", None, 5])
def test_empty_or_non_string_expression_is_refused(text):
    with pytest.raises(ConfigError, match="non-empty string"):
        Expression(text)


@pytest.mark.parametrize(
    "text, row",
    [
        ("a / b", {"a": 1, "b": 0}),          # division by zero
        ("a + 1", {"a": None}),               # arithmetic on NULL
        ("a < 3", {"a": None}),               # ordering NULL
        ("a + b", {"a": "x", "b": 1}),        # str + int
        ("upper(a)", {"a": None}),            # helper on NULL
    ],
)
def test_evaluation_problems_reject_the_row_not_the_run(text, row):
    with pytest.raises(RowError) as excinfo:
        Expression(text).evaluate(row)
    assert text in str(excinfo.value)  # the reason names the offending expression


def test_evaluation_does_not_modify_the_row():
    row = {"a": 1, "b": 2}
    Expression("a + b").evaluate(row)
    assert row == {"a": 1, "b": 2}
