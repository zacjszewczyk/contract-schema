"""Unit tests for analytic_schema.py

These tests exercise the public API exposed by analytic_schema:
  • parse_input()
  • validate_input()
  • OutputDoc

Only the Python standard library is used (unittest, tempfile, json, pathlib, os).
Run with:
    python -m unittest test_analytic_schema.py

Assumes test_analytic_schema.py is in the same directory as analytic_schema.py,
or that analytic_schema is otherwise importable on PYTHONPATH.
"""

import json
import os
import tempfile
import unittest
from pathlib import Path

import analytic_schema as asc  # The module under test


raw_params = asc.parse_input('--input_schema_version 1.0.0 ' \
                             '--start_dtg 2025-06-01T00:00:00Z ' \
                             '--end_dtg 2025-06-02T00:00:00Z ' \
                             '--data_source_type file ' \
                             '--data_source /tmp/conn.csv'.split()) \
params = validate_input(raw_params)
>>> raw_data_sha256 = "e3b0c4...55"  # Example hash
>>> out = OutputDoc(
...        input_schema_version=params['input_schema_version'],
...        output_schema_version='1.1.0',
...        analytic_id='notebooks/beacon_detection.ipynb',
...        analytic_name='Beacon Detection',
...        analytic_version='2.3.1',
...        inputs=params,
...        input_data_hash=raw_data_sha256,
...        status='success',
...        exit_code=0,
...        findings=[],
...        records_processed=0
... )
>>> out.finalise()
>>> out.save('run-results.json')