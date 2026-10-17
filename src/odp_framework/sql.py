"""Helpers to build SQL safely from configuration."""

from __future__ import annotations

import re

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def is_identifier(name: str) -> bool:
    return bool(_IDENTIFIER.match(name or ""))


def quote_ident(name: str) -> str:
    """Identifier in backticks (escaping inner backticks)."""
    return "`" + name.replace("`", "``") + "`"


def sql_literal(value: str) -> str:
    """SQL string literal with escaped single quotes."""
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def in_condition(column: str, values: list[str] | tuple[str, ...]) -> str:
    """``col IN ('a', 'b')`` with escaped identifier and literals."""
    if not values:
        raise ValueError("in_condition requires at least one value")
    literals = ", ".join(sql_literal(v) for v in values)
    return f"{quote_ident(column)} IN ({literals})"
