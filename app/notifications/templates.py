"""Tenant-authored notification templates: `{{ payload.order_id }}` lookups only.

Templates are written by tenants, so they are untrusted input running on our
servers. A general engine (Jinja2, even sandboxed) has a history of escapes —
server-side template injection. This renderer only substitutes dotted lookups
into a fixed context: no expressions, filters, loops, or attribute access on
Python objects, so there is nothing to escape from. The cost is no logic in
templates; producers put computed values into the payload instead.
"""
from __future__ import annotations

import json
import re

PLACEHOLDER = re.compile(r"\{\{\s*([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z0-9_]+)*)\s*\}\}")
ROOTS = ("event_type", "subject", "payload", "recipient")
MAX_RENDERED_CHARS = 64 * 1024


class TemplateError(ValueError):
    pass


def placeholders(template: str) -> list[str]:
    """Validate syntax at save time so authors find mistakes before producers do."""
    paths = PLACEHOLDER.findall(template)
    leftover = PLACEHOLDER.sub("", template)
    if "{{" in leftover or "}}" in leftover:
        raise TemplateError("malformed placeholder; use {{ payload.field }}")
    for path in paths:
        if path.split(".", 1)[0] not in ROOTS:
            raise TemplateError(f"unknown variable '{path}'; must start with one of: {', '.join(ROOTS)}")
    return paths


def lookup(context: dict, path: str):
    value = context
    for segment in path.split("."):
        if isinstance(value, dict) and segment in value:
            value = value[segment]
        elif isinstance(value, list) and segment.isdigit() and int(segment) < len(value):
            value = value[int(segment)]
        else:
            raise TemplateError(f"variable '{path}' is missing")
    return value


def format_value(value) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, separators=(",", ":"), sort_keys=True)
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def render(template: str, context: dict) -> str:
    """Strict: a missing variable is an error, never a silently blank email."""
    rendered = PLACEHOLDER.sub(lambda match: format_value(lookup(context, match.group(1))), template)
    if len(rendered) > MAX_RENDERED_CHARS:
        # Guards against "{{ payload.big }}" repeated many times.
        raise TemplateError("rendered content exceeds 64 KiB")
    return rendered
