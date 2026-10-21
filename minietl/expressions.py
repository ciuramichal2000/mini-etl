"""A tiny expression language for the filter and derive steps.

A config file is data, so running its expressions through eval() would let a
config execute arbitrary code. Instead the text is parsed with the ast module
and compiled into closures from a short whitelist of node types: literals,
column names, arithmetic, comparisons, and/or/not, conditional expressions and
a handful of helper functions. Everything else (attribute access, subscripts,
lambdas, comprehensions, ``**``...) is refused when the config is loaded, not
when the first row hits it.
"""

import ast
import operator
from collections.abc import Callable, Mapping
from typing import Any

from .errors import ConfigError, RowError

Evaluator = Callable[[Mapping[str, Any]], Any]


def _coalesce(*values: Any) -> Any:
    """First argument that is not None, or None when all are."""
    for value in values:
        if value is not None:
            return value
    return None


FUNCTIONS: dict[str, Callable[..., Any]] = {
    "upper": str.upper,
    "lower": str.lower,
    "strip": str.strip,
    "len": len,
    "abs": abs,
    "round": round,
    "coalesce": _coalesce,
}

_BINARY = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod,
}
_UNARY = {ast.USub: operator.neg, ast.UAdd: operator.pos, ast.Not: operator.not_}
_COMPARE = {
    ast.Eq: operator.eq,
    ast.NotEq: operator.ne,
    ast.Lt: operator.lt,
    ast.LtE: operator.le,
    ast.Gt: operator.gt,
    ast.GtE: operator.ge,
    ast.Is: operator.is_,
    ast.IsNot: operator.is_not,
    ast.In: lambda left, right: left in right,
    ast.NotIn: lambda left, right: left not in right,
}


class _Compiler:
    """Turns a parsed expression tree into nested closures, refusing unknown nodes."""

    def __init__(self, text: str) -> None:
        self.text = text
        self.names: set[str] = set()

    def refuse(self, what: str) -> ConfigError:
        return ConfigError(f"expression {self.text!r}: {what} is not supported")

    def compile(self, node: ast.AST) -> Evaluator:
        if isinstance(node, ast.Constant):
            return self._constant(node)
        if isinstance(node, ast.Name):
            return self._name(node)
        if isinstance(node, ast.BoolOp):
            return self._bool_op(node)
        if isinstance(node, ast.UnaryOp):
            return self._unary(node)
        if isinstance(node, ast.BinOp):
            return self._binary(node)
        if isinstance(node, ast.Compare):
            return self._compare(node)
        if isinstance(node, ast.IfExp):
            return self._conditional(node)
        if isinstance(node, ast.Call):
            return self._call(node)
        if isinstance(node, (ast.Tuple, ast.List)):
            return self._sequence(node)
        raise self.refuse(type(node).__name__)

    def _constant(self, node: ast.Constant) -> Evaluator:
        value = node.value
        if value is not None and not isinstance(value, (str, int, float)):  # bool is an int
            raise self.refuse(f"literal {value!r}")
        return lambda row: value

    def _name(self, node: ast.Name) -> Evaluator:
        name = node.id
        self.names.add(name)
        return lambda row: row[name]

    def _bool_op(self, node: ast.BoolOp) -> Evaluator:
        operands = [self.compile(value) for value in node.values]
        stop_on_truthy = isinstance(node.op, ast.Or)

        def run(row: Mapping[str, Any]) -> Any:
            # Same short-circuit semantics as Python, so "x != 0 and 10 / x > 1" is safe.
            result: Any = None
            for operand in operands:
                result = operand(row)
                if bool(result) == stop_on_truthy:
                    return result
            return result

        return run

    def _unary(self, node: ast.UnaryOp) -> Evaluator:
        function = _UNARY.get(type(node.op))
        if function is None:
            raise self.refuse(f"operator {type(node.op).__name__}")
        operand = self.compile(node.operand)
        return lambda row: function(operand(row))

    def _binary(self, node: ast.BinOp) -> Evaluator:
        function = _BINARY.get(type(node.op))
        if function is None:
            raise self.refuse(f"operator {type(node.op).__name__}")
        left, right = self.compile(node.left), self.compile(node.right)
        return lambda row: function(left(row), right(row))

    def _compare(self, node: ast.Compare) -> Evaluator:
        functions = []
        for op, comparator in zip(node.ops, node.comparators):
            function = _COMPARE.get(type(op))
            if function is None:
                raise self.refuse(f"operator {type(op).__name__}")
            if isinstance(op, (ast.Is, ast.IsNot)) and not (
                isinstance(comparator, ast.Constant) and comparator.value is None
            ):
                # Identity of equal strings/numbers depends on interning, not on the data.
                raise self.refuse("'is' / 'is not' with anything but None (use == or !=)")
            functions.append(function)
        left = self.compile(node.left)
        rights = [self.compile(comparator) for comparator in node.comparators]

        def run(row: Mapping[str, Any]) -> bool:
            # Chained comparisons ("1 < x < 5") evaluate each operand once, left to right.
            current = left(row)
            for function, right in zip(functions, rights):
                following = right(row)
                if not function(current, following):
                    return False
                current = following
            return True

        return run

    def _conditional(self, node: ast.IfExp) -> Evaluator:
        test, body, orelse = (self.compile(part) for part in (node.test, node.body, node.orelse))
        return lambda row: body(row) if test(row) else orelse(row)

    def _call(self, node: ast.Call) -> Evaluator:
        func = node.func
        if not isinstance(func, ast.Name) or func.id not in FUNCTIONS:
            raise self.refuse(f"calling anything but {', '.join(sorted(FUNCTIONS))}")
        if node.keywords:
            raise self.refuse("keyword arguments")
        function = FUNCTIONS[func.id]
        arguments = [self.compile(argument) for argument in node.args]
        return lambda row: function(*(argument(row) for argument in arguments))

    def _sequence(self, node: ast.Tuple | ast.List) -> Evaluator:
        items = [self.compile(element) for element in node.elts]
        return lambda row: tuple(item(row) for item in items)


class Expression:
    """A compiled expression over column values.

    ``names`` lists the columns it reads, so the pipeline can verify them once
    against the table layout instead of failing on every row.
    """

    def __init__(self, text: str) -> None:
        if not isinstance(text, str) or not text.strip():
            raise ConfigError("expression must be a non-empty string")
        try:
            tree = ast.parse(text.strip(), mode="eval")
        except (SyntaxError, ValueError, RecursionError, MemoryError) as exc:
            detail = getattr(exc, "msg", None) or str(exc)
            raise ConfigError(f"invalid expression {text!r}: {detail}") from exc
        compiler = _Compiler(text)
        try:
            self._evaluate = compiler.compile(tree.body)
        except RecursionError as exc:
            raise ConfigError(f"expression {text!r} is nested too deeply") from exc
        self.text = text
        self.names = frozenset(compiler.names)

    def evaluate(self, row: Mapping[str, Any]) -> Any:
        """Compute the value for one row; bad data (None + 1, x / 0) rejects the row."""
        try:
            return self._evaluate(row)
        except (TypeError, ValueError, ArithmeticError) as exc:
            raise RowError(f"cannot evaluate {self.text!r}: {exc}") from exc
