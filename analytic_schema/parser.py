import argparse
import json
import shlex
import sys
from pathlib import Path
from collections.abc import Mapping, Sequence

from .loader import INPUT_SCHEMA, SCHEMA_PATH, SCHEMA_VERSION

def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=INPUT_SCHEMA.get("description", ""),
        fromfile_prefix_chars="@",
        add_help=False
    )

    # help/version/config
    p.add_argument("-h", "--help",
                   action="help",
                   default=argparse.SUPPRESS,
                   help="Show this help message and exit.")
    p.add_argument("--version",
                   action="version",
                   version=f"{SCHEMA_PATH.name} : {SCHEMA_VERSION}",
                   help="Print schema version and exit.")
    p.add_argument("--config",
                   metavar="FILE",
                   help="JSON file containing full input object; overrides all other flags.")

    # auto-generate flags from each field in INPUT_SCHEMA["fields"]
    for name, spec in INPUT_SCHEMA["fields"].items():
        flag = f"--{name.replace('_','-')}"
        kwargs: dict = {"dest": name, "help": spec.get("description", ""), "required": False}

        # determine argparse action / type
        types = spec["type"] if isinstance(spec["type"], list) else [spec["type"]]
        if "boolean" in types:
            kwargs["action"] = "store_true"
        else:
            # pick the first sensible mapping: integer → int, number → float, else str
            if "integer" in types:
                kwargs["type"] = int
            elif "number" in types:
                kwargs["type"] = float
            else:
                kwargs["type"] = str

        if "enum" in spec:
            kwargs["choices"] = spec["enum"]

        p.add_argument(flag, **kwargs)

    return p

def parse_input(
    source: None
    | str
    | Path
    | Sequence[str]
    | Mapping[str, any] = None
) -> dict:
    """
    Convert *source* into a raw dict.  Does *not* validate.
    - Mapping → copy
    - Path → load JSON
    - str → file? JSON? CLI
    - Sequence[str] → CLI tokens
    - None → sys.argv[1:]
    """
    # already a dict
    if isinstance(source, Mapping):
        return dict(source)

    # Path → load JSON file
    if isinstance(source, Path):
        text = source.read_text(encoding="utf-8")
        return json.loads(text)

    argv: list[str]
    if isinstance(source, str):
        p = Path(source)
        if p.is_file():
            return json.loads(p.read_text(encoding="utf-8"))
        # try JSON literal
        try:
            return json.loads(source)
        except json.JSONDecodeError:
            # fallback to CLI
            argv = shlex.split(source)
    elif source is None:
        argv = sys.argv[1:]
    elif isinstance(source, Sequence) and not isinstance(source, (str, bytes)):
        argv = list(source)
    else:
        raise TypeError(f"Unsupported type for parse_input: {type(source)}")

    parser = _build_arg_parser()
    namespace, unknown = parser.parse_known_args(argv)
    if unknown:
        raise ValueError(f"Unknown argument(s): {unknown}.  Use --help.")
    return {k: v for k, v in vars(namespace).items() if v is not None}