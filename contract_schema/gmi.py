"""Offline validation of repository-level interchange declarations and examples for GMI."""
from __future__ import annotations

from pathlib import Path
import json
import hashlib

ALLOWED_ROLES = {"authoring", "training_source", "task_standard_source", "scenario_library", "delivery"}


def validate_gmi_contracts(root: Path | str) -> dict:
    """Validate GMI contracts, integration declaration, and schema pins within root directory."""
    root = Path(root).resolve()
    integration_file = root / "integration.json"
    if not integration_file.is_file():
        raise FileNotFoundError(f"Missing integration declaration: {integration_file}")

    d = json.loads(integration_file.read_text(encoding="utf-8"))
    assert d.get("schema") == "gmi/repository-integration@1", f"Unexpected schema: {d.get('schema')}"
    assert d.get("role") in ALLOWED_ROLES and str(d.get("repository", "")).startswith("zacjszewczyk/"), (
        f"Invalid role or repository in {integration_file}"
    )
    assert len(set(d.get("produces", []))) == len(d.get("produces", [])), "Duplicate produces declarations"
    assert len(set(d.get("consumes", []))) == len(d.get("consumes", [])), "Duplicate consumes declarations"

    for path in sorted(root.glob("examples/*.json")):
        obj = json.loads(path.read_text(encoding="utf-8"))
        schema = obj.get("schema")
        if schema == "gmi/competency@1":
            assert obj.get("id") and str(obj.get("task", "")).strip() and obj.get("conditions") and obj.get("standards")
            ids = [s["criterion_id"] for s in obj["standards"]]
            assert len(ids) == len(set(ids)), f"Duplicate criterion_ids in {path}"
            for standard in obj["standards"]:
                assert all(str(standard.get(k, "")).strip() for k in ("description", "evidence", "pass_rule")), (
                    f"Incomplete standard in {path}"
                )
        elif schema == "gmi/scenario-reference@1":
            assert obj.get("scenario_id") and obj.get("scenario_version") and obj.get("readiness") in (
                "design", "reviewed", "released"
            ), f"Invalid scenario-reference in {path}"
        else:
            raise ValueError(f"unsupported example schema in {path}")

    pins_file = root / "schema-pins.json"
    if pins_file.is_file():
        pins = json.loads(pins_file.read_text(encoding="utf-8"))
        assert pins.get("schema") == "gmi/schema-pins@1" and pins.get("source") == "zacjszewczyk/chiron"
        assert set(pins.get("blobs", {})).issubset({p.name for p in root.glob("gmi-*.schema.json")})
        for name, expected in pins.get("blobs", {}).items():
            payload = (root / name).read_bytes()
            computed = hashlib.sha1(b"blob " + str(len(payload)).encode() + b"\x00" + payload).hexdigest()
            assert computed == expected, f"canonical contract drift: {name}"

    return {
        "status": "ok",
        "repository": d["repository"],
        "role": d["role"],
        "examples_validated": len(list(root.glob("examples/*.json"))),
    }


def main():
    root = Path(__file__).resolve().parent
    result = validate_gmi_contracts(root)
    print(f"ok: {result['repository']} contract declaration and examples")


if __name__ == "__main__":
    main()
