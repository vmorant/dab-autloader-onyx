"""Helpers para construir SQL de forma segura a partir de la configuración."""

from __future__ import annotations

import re

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def is_identifier(name: str) -> bool:
    return bool(_IDENTIFIER.match(name or ""))


def quote_ident(name: str) -> str:
    """Identificador entre backticks (escapando backticks internos)."""
    return "`" + name.replace("`", "``") + "`"


def sql_literal(value: str) -> str:
    """Literal de texto SQL con comillas simples escapadas."""
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def in_condition(column: str, values: list[str] | tuple[str, ...]) -> str:
    """``col IN ('a', 'b')`` con identificador y literales escapados."""
    if not values:
        raise ValueError("in_condition requiere al menos un valor")
    literals = ", ".join(sql_literal(v) for v in values)
    return f"{quote_ident(column)} IN ({literals})"
