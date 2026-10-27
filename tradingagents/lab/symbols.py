"""Symbols as people type them: a bare B3 code gets Yahoo's .SA suffix."""

from __future__ import annotations

import re

_B3 = re.compile(r"[A-Z]{4}\d{1,2}F?")


def normalize(ticker: str) -> str:
    t = ticker.strip().upper()
    return f"{t}.SA" if _B3.fullmatch(t) else t
