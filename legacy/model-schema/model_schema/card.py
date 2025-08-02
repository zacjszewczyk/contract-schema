"""Markdown helpers for contracts & manifests.

This module converts:

1.  **The JSON contract** (schema) → a concise, human-readable *contract card*.
2.  **A single model manifest** → a richly-formatted *model card*.

Both helpers return a **Markdown** string that can be written to disk, displayed
in notebooks, or pushed to documentation sites.

Typical use
-----------

import model_schema as msc
# Contract card
md = msc.contract_card()          # Uses the packaged schema by default
Path("contract.md").write_text(md, encoding="utf-8")

# Model card
mani = msc.ModelManifest(model_type="RandomForest", ...)
mani.finalise(Path("rf.pkl"))
card = msc.model_card(mani)
Path("rf_card.md").write_text(card, encoding="utf-8")
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, Sequence

# Internal import – the packaged schema (used when no override supplied).
from .loader import OUTPUT_SCHEMA

# --- Private helpers ---

def _join_types(t: Sequence[str] | str | None) -> str:
    """Join a list of schema types into a slash-separated string.

    This helper function normalizes the representation of type information
    from the schema, ensuring a consistent, sorted, and lowercase format.

    Args:
        t (Sequence[str] | str | None): The type(s) to format. This can be
            a list of strings, a single string, or None.

    Returns:
        str: A slash-separated, lowercase string of types, or an empty
             string if the input is None or empty.
    """
    # Return an empty string immediately if the input is falsy (None or empty).
    if not t:
        return ""
    
    # Ensure the input is a list to handle both single strings and sequences.
    # Sort the list to guarantee a deterministic order for the same set of types.
    # Join the sorted types with a slash and convert to lowercase for consistency.
    return "/".join(sorted(t if isinstance(t, (list, tuple)) else [t])).lower()


def _schema_fields_to_md(fields: Mapping[str, Any], level: int = 3) -> str:
    """Recursively render a mapping of schema fields to a Markdown string.

    This function iterates through a dictionary of field definitions from a JSON
    schema, creating Markdown headers and descriptions for each field. It calls
    itself to handle nested objects, increasing the header level for visual hierarchy.

    Args:
        fields (Mapping[str, Any]): A dictionary where keys are field names
            and values are dictionaries of field metadata (e.g., type,
            description, required status).
        level (int): The starting Markdown header level (e.g., 3 corresponds to `###`).

    Returns:
        str: A formatted Markdown string representing the schema fields.
    """
    # A list to accumulate Markdown parts for each field.
    md_parts: list[str] = []
    
    # Define the Markdown header prefix based on the current recursion level.
    hdr = "#" * level
    
    # Iterate through field names in sorted order to ensure the output is
    # deterministic, which is crucial for consistent documentation generation.
    for name in sorted(fields):
        # Extract metadata for the current field.
        meta      = fields[name]
        required  = meta.get("required", False)
        req_flag  = "(Required)" if required else "(Optional)"
        typestr   = _join_types(meta.get("type"))
        desc      = meta.get("description", "").strip()

        # Construct the Markdown block for the current field, including its name,
        # requirement status, type, and description.
        md_parts.append(f"{hdr} {name}. {req_flag}\n\n"
                        f"({typestr}). {desc}".rstrip())

        # If the field contains nested fields (i.e., it's an object with its own
        # properties), recurse into the nested structure, increasing the header
        # level to create a sub-section.
        if "fields" in meta:
            md_parts.append(_schema_fields_to_md(meta["fields"], level + 1))
            
    # Join all the generated Markdown parts into a single string, separated
    # by double newlines to create distinct paragraphs.
    return "\n\n".join(md_parts)


def _manifest_to_md(obj: Any, level: int = 3) -> str:
    """Recursively convert a manifest object to a Markdown string.

    This function traverses a nested structure (typically from a `ModelManifest`)
    and generates a hierarchical Markdown representation. It handles dictionaries,
    lists, and scalar values differently to create a readable report.

    Args:
        obj (Any): The object to convert. Can be a dictionary, list, or scalar.
        level (int): The starting Markdown header level for dictionary keys.

    Returns:
        str: A formatted Markdown string of the object's contents.
    """
    # Define the Markdown header prefix for the current recursion level.
    hdr  = "#" * level
    # A list to hold the resulting Markdown strings.
    out: list[str] = []

    if isinstance(obj, Mapping):
        # The object is a dictionary-like mapping.
        # Iterate over its keys in sorted order for a stable, predictable output.
        for key in sorted(obj):
            val = obj[key]
            # Create a Markdown header for the key.
            out.append(f"{hdr} {key}\n")
            # Recurse into the value with an incremented header level.
            out.append(_manifest_to_md(val, level + 1))
            
    elif isinstance(obj, (list, tuple)):
        # The object is a list or tuple.
        # Render simple lists as a single comma-separated line. More complex
        # list items (like dicts) are serialized to a JSON string.
        rendered = ", ".join(map(lambda x: json.dumps(x, ensure_ascii=False)
                                 if isinstance(x, (dict, list)) else str(x), obj))
        # Use a placeholder if the list is empty.
        out.append(rendered or "--")
        
    else:
        # The object is a scalar (e.g., string, number, bool).
        # Convert it to its string representation. Use a placeholder for None.
        out.append(str(obj) if obj is not None else "--")

    # Join the parts into a final string, ensuring proper spacing.
    return "\n\n".join(out).rstrip()

# --- Public API ---

def contract_card(schema: Mapping[str, Any] | None = None) -> str:
    """Render a JSON schema contract into a human-readable Markdown card.

    This function takes a schema, extracts its title, description, and field
    definitions, and formats them into a structured Markdown document. If no
    schema is provided, it defaults to the packaged `OUTPUT_SCHEMA`.

    Args:
        schema (Mapping[str, Any] | None): The schema dictionary to render.
            If None, the default schema packaged with the library is used.

    Returns:
        str: A Markdown string representing the contract card.
    """
    # If no schema is provided, fall back to the default OUTPUT_SCHEMA.
    # This makes the function easy to use for the most common case.
    schema = schema or OUTPUT_SCHEMA
    
    # Extract the main title and description from the schema's top level.
    title  = schema.get("title", "Model Schema")
    desc   = schema.get("description", "").strip()

    # Generate the main body of the card by converting the schema's 'fields'
    # section into Markdown.
    body   = _schema_fields_to_md(schema.get("fields", {}), level=3)

    # Assemble the final Markdown string from the title, description, and body.
    return f"# {title}\n\n{desc}\n\n{body}\n"


def model_card(manifest: Mapping[str, Any]) -> str:
    """Render a model manifest into a detailed Markdown "model card".

    This function creates a comprehensive summary of a model based on its
    manifest. It generates a title, a summary paragraph combining key fields,
    and a detailed breakdown of all manifest contents.

    Args:
        manifest (Mapping[str, Any]): A dictionary (often a `ModelManifest`
            instance) containing the model's metadata.

    Returns:
        str: A Markdown string representing the model card.
    """
    # Create the main title for the model card using the model_type.
    mtype  = manifest.get("model_type", "Model")
    title  = f"# {mtype}"
    
    # Construct a summary paragraph by concatenating key descriptive fields.
    # This provides a high-level overview at the top of the card.
    summary_parts = [
        manifest.get("model_description", "").strip(),
        manifest.get("intended_use", "").strip(),
        manifest.get("limitations", "").strip(),
    ]
    summary = " ".join(p for p in summary_parts if p)

    # Generate the main body by recursively converting the entire manifest
    # into a key-value Markdown format. This ensures all information is
    # present in the card for full transparency.
    body   = _manifest_to_md(manifest, level=3)

    # Combine the title, summary, and detailed body into the final card.
    return f"{title}\n\n{summary}\n\n{body}\n"
