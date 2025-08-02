from contract_schema import Contract, validator, loader
import pytest, json, copy

def test_good_contract_loads():
    Contract.load("analytic_schema.json")   # should not raise

def test_missing_required_key():
    bad = copy.deepcopy(loader.load_schema("analytic_schema.json"))
    bad.pop("title")                        # break the meta-schema
    with pytest.raises(ValueError):
        Contract(                           # simulate loading from disk
            **{k: bad.get(k) for k in ("title","description","version","input","output")}
        )

def test_fields_required():
    bad = copy.deepcopy(loader.load_schema("analytic_schema.json"))
    bad["input"].pop("fields")
    with pytest.raises(ValueError):
        validator.validate(
            bad, 
            schema=loader.load_schema("contract_meta_schema.json")
        )