# example_usage.py
from analytic_schema import parse_input, validate_input, OutputDoc

import time

# 1) read & validate inputs
cli_params = (
    "--input-schema-version 1.0.0 "
    "--start-dtg 2025-06-01T00:00:00Z "
    "--end-dtg 2025-06-02T00:00:00Z "
    "--data-source-type file "
    "--data-source /tmp/conn.csv"
)
raw = parse_input(cli_params)  # or parse_input("..."), etc.
params = validate_input(raw)

# 2) your analytic logic
start = time.perf_counter()
# ... do work, e.g. scan a PCAP ...
total = 123
findings = [
    {
      "finding_id": "123e4567-e89b-12d3-a456-426614174000",
      "title": "Suspicious DNS query",
      "description": "High‐volume NXDOMAIN ...",
      "event_dtg": "2025-06-07T12:34:56Z",
      "severity": "high",
      "confidence": "0.85",
      "observables": ["evil.example.com"],
      "mitre_attack_tactics": ["TA0001"],
      "mitre_attack_techniques": ["T1001"],
      "recommended_actions": "Block domain",
      "recommended_pivots": "Check DNS logs",
      "classification": "U"
    }
]
duration = (time.perf_counter() - start) * 1000

# 3) build & emit output
out = OutputDoc(
    input_data_hash="f2ca1bb6c7e907d06dafe4687e579fce76b37e4e93b7605022da52e6ccc26fd2", # Dummy hash
    inputs=params
)
# record some messages
out.add_message("INFO", "Analysis started")
out.add_message("INFO", "Found %d records" % total)

out["records_processed"] = total
out["findings"] = findings

out.finalise()
out.save("notebook_output.json")