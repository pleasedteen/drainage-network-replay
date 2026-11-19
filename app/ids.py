"""Joint / pipe reference syntax.

Two reference forms exist in the source logs:

``J-004521``
    the pre-upgrade *legacy* identifier.  City-wide unique.
``C4:004521``
    the post-upgrade *zoned* identifier: ``<zone>:<local>``.  Unique only
    inside its zone.

A colon is the discriminator, so the two forms can never be confused and a
legacy reference keeps working after the upgrade (it is mapped to the same
joint by the id table that ships with the upgrade batch).
"""

from __future__ import annotations

import re
from typing import NamedTuple

LEGACY_RE = re.compile(r"^[A-Z]{1,3}-\d{3,9}$")
ZONE_RE = re.compile(r"^[A-Z][A-Z0-9]{0,5}$")
LOCAL_RE = re.compile(r"^[0-9A-Za-z][0-9A-Za-z_.-]{0,31}$")
PIPE_RE = re.compile(r"^P-[0-9]{3,12}$")


class RefError(ValueError):
    pass


class Ref(NamedTuple):
    text: str      # canonical reference as it should be stored/quoted back
    kind: str      # 'legacy' | 'qualified'
    zone: str      # '' for legacy references
    local: str     # local part, or the whole legacy id

    @property
    def is_legacy(self) -> bool:
        return self.kind == "legacy"


def parse_ref(value) -> Ref:
    """Parse one reference; raises ``RefError`` with a readable message."""
    if isinstance(value, Ref):
        return value
    if not isinstance(value, str):
        raise RefError(f"reference must be a string, got {type(value).__name__}")
    text = value.strip()
    if not text:
        raise RefError("empty reference")
    if ":" in text:
        zone, _, local = text.partition(":")
        zone = zone.strip().upper()
        local = local.strip()
        if not ZONE_RE.match(zone):
            raise RefError(f"bad zone code {zone!r} in reference {text!r}")
        if not LOCAL_RE.match(local):
            raise RefError(f"bad local number {local!r} in reference {text!r}")
        return Ref(f"{zone}:{local}", "qualified", zone, local)
    if not LEGACY_RE.match(text):
        raise RefError(
            f"bad reference {text!r}: expected legacy 'J-000123' "
            f"or zoned 'C4:00123'"
        )
    return Ref(text, "legacy", "", text)


def make_zoned(zone: str, local: str) -> Ref:
    return parse_ref(f"{zone}:{local}")


def parse_pipe_code(value) -> str:
    if not isinstance(value, str):
        raise RefError(f"pipe code must be a string, got {type(value).__name__}")
    code = value.strip()
    if not PIPE_RE.match(code):
        raise RefError(f"bad pipe code {code!r}: expected 'P-000123'")
    return code


def join_ref(zone: str, local: str) -> str:
    return f"{zone}:{local}" if zone else local
