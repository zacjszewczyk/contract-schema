from pathlib import Path
from contract_schema.gmi import validate_gmi_contracts


def test_gmi_infrastructure_contracts():
    infra_contracts = Path("/home/jovyan/work/general-method-institute-infrastructure/contracts")
    if infra_contracts.is_dir():
        res = validate_gmi_contracts(infra_contracts)
        assert res["status"] == "ok"
        assert res["role"] == "delivery"


def test_gmi_scenarios_contracts():
    scenarios_contracts = Path("/home/jovyan/work/general-method-institute-scenarios/contracts")
    if scenarios_contracts.is_dir():
        res = validate_gmi_contracts(scenarios_contracts)
        assert res["status"] == "ok"
        assert res["role"] == "scenario_library"
