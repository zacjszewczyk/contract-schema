# contract_schema/card.py
from __future__ import annotations
from typing import Any, Mapping, Sequence

__all__ = ["to_markdown_card"]

def _format_scalar(v: Any) -> str:
    """Return a Markdown-safe scalar string."""
    if v is True:   return "true"
    if v is False:  return "false"
    if v is None:   return "null"
    return str(v)

def _format_list(v: Sequence[Any]) -> str:
    """Return a bulleted Markdown list (no surrounding blank lines)."""
    lines: list[str] = []
    for item in v:
        if isinstance(item, (list, tuple)):
            # flatten 1-level nested sequences into comma-separated string
            lines.append(f"- {_format_scalar(', '.join(map(str, item)))}")
        else:
            lines.append(f"- {_format_scalar(item)}")
    return "\n".join(lines)

def to_markdown_card(data: Mapping[str, Any], *, heading_level: int = 2) -> str:
    """
    Convert *data* into a Markdown card.

    Parameters
    ----------
    data : Mapping[str, Any]
        Any mapping whose values are JSON-serialisable (schema output or
        `schema["output"]["fields"]`).
    heading_level : int, default 2
        Markdown heading level for top-level keys (##, ###, …).

    Returns
    -------
    str
        Markdown document.
    """
    h = "#" * heading_level
    parts: list[str] = []
    for key, value in data.items():
        parts.append(f"{h} {key.replace('_', ' ').title()}")
        if isinstance(value, Mapping):
            # one extra indent → sub-bullets
            sub = _format_list([f"**{k}**: {_format_scalar(v)}" for k, v in value.items()])
            parts.append(sub)
        elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
            parts.append(_format_list(value))
        else:
            parts.append(_format_scalar(value))
        parts.append("")             # blank line after each section
    return "\n".join(parts).rstrip()