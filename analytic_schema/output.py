import datetime as _dt
import enum
import getpass
import hashlib
import json
import socket
import uuid
from pathlib import Path
from typing import Any, Union

import pandas as pd

from .loader import OUTPUT_SCHEMA
from .validator import _validate, SchemaError

def display_output(obj: Any, **print_kwargs) -> None:
    """
    In-notebook display or fallback to print(...).
    """
    try:
        get_ipython  # type: ignore
        from IPython.display import display
        display(obj)
    except NameError:
        print(obj, **print_kwargs)

class _Level(enum.Enum):
    DEBUG = "DEBUG"
    INFO  = "INFO"
    WARN  = "WARN"
    ERROR = "ERROR"
    FATAL = "FATAL"

class OutputDoc(dict):
    """
    Build a dict that conforms to OUTPUT_SCHEMA.
    Records meta, hashes, messages, then final‐validates.
    """

    def __init__(self, *, input_data_hash: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        if not isinstance(input_data_hash, str):
            raise TypeError("input_data_hash must be a hex string")
        self["input_data_hash"] = input_data_hash
        self.__start = _dt.datetime.now(_dt.timezone.utc)
        self.setdefault("messages", [])

        # Log that the output document was created
        self.add_message("INFO", "Output document created")

    def add_message(self, level: Union[str, _Level], text: str) -> None:
        if isinstance(level, str):
            lvl = level.upper()
            try:
                lvl = _Level[lvl].value
            except KeyError:
                allowed = ", ".join(_Level.__members__)
                raise ValueError(f"Invalid log level '{level}'. Allowed: {allowed}")
        elif isinstance(level, _Level):
            lvl = level.value
        else:
            raise TypeError("level must be str or _Level")

        if not isinstance(text, str):
            raise TypeError("text must be string")

        self["messages"].append({
            "timestamp": _dt.datetime.now(_dt.timezone.utc)
                            .isoformat(timespec="seconds"),
            "level": lvl,
            "text": text
        })

    @staticmethod
    def _json_safe(x: Any) -> Any:
        if isinstance(x, pd.DataFrame):
            js = x.to_json(orient="split", date_unit="ns")
            return {"__dataframe_sha256__":
                    hashlib.sha256(js.encode()).hexdigest()}
        if isinstance(x, dict):
            return {k: OutputDoc._json_safe(v) for k, v in x.items()}
        if isinstance(x, (list, tuple)):
            return [OutputDoc._json_safe(v) for v in x]
        return x

    @classmethod
    def _hash(cls, obj: Any) -> str:
        safe = cls._json_safe(obj)
        b = json.dumps(safe, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(b).hexdigest()

    def _serial(self) -> dict[str, Any]:
        return OutputDoc._json_safe(self)

    def finalise(self) -> None:
        # must have inputs dict
        if "inputs" not in self or not isinstance(self["inputs"], dict):
            raise SchemaError("missing or invalid 'inputs' before finalise()")
        
        end = _dt.datetime.now(_dt.timezone.utc)
        # set meta‐fields if absent
        self.setdefault("run_id", str(uuid.uuid4()))
        self.setdefault("run_user", getpass.getuser() if hasattr(getpass,"getuser") else "unknown_user")
        self.setdefault("run_host", socket.gethostname() if hasattr(socket,"gethostname") else "unknown_host")
        self.setdefault("run_start_dtg", self.__start.isoformat(timespec="seconds"))
        self.setdefault("run_end_dtg",   end.isoformat(timespec="seconds"))
        self.setdefault("run_duration_seconds",
                        round((end - self.__start).total_seconds(), 6))

        # hash inputs & findings
        self["input_hash"] = OutputDoc._hash(self["inputs"])
        self.setdefault("findings", [])
        if not isinstance(self["findings"], list):
            raise SchemaError("'findings' must be a list")

        # validate each finding against the item schema
        finding_schema = OUTPUT_SCHEMA["fields"]["findings"]["items"]
        for idx, finding in enumerate(self["findings"]):
            _validate(finding, finding_schema, path=f"OutputDoc.findings[{idx}]")

        self["findings_hash"] = OutputDoc._hash(self["findings"])

        # safe defaults for other optional fields
        for k, v in {
            "input_schema_version": self["inputs"].get("input_schema_version", "UNKNOWN"),
            "output_schema_version": "UNKNOWN",
            "analytic_id":           "UNKNOWN",
            "analytic_name":         "UNKNOWN",
            "analytic_version":      "UNKNOWN",
            "status":                "UNKNOWN",
            "exit_code":             -1,
            "records_processed":     0
        }.items():
            self.setdefault(k, v)

        # Log finalization
        self.add_message("INFO", "Output document finalised.")
        
        # final validation
        _validate(self, OUTPUT_SCHEMA, path="OutputDoc")

    def save(self,
             path: Union[str, Path],
             *,
             indent: int = 2,
             quiet: bool = False) -> None:
        if "run_id" not in self:
            raise RuntimeError("save() called before finalise()")
        p = Path(path)
        doc = self._serial()
        p.write_text(json.dumps(doc, indent=indent, ensure_ascii=False),
                     encoding="utf-8")

        # Log save
        self.add_message("INFO", "Output document saved")
        
        if not quiet:
            display_output(f"Output saved to {p.resolve()}")