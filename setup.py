# setup.py
from setuptools import setup, find_packages

setup(
    name="analytic-schema",           # the *distribution* name on PyPI
    version="1.0.0",
    packages=find_packages(),         # will find analytic_schema/
    install_requires=["pandas"],      # your only runtime dependency
    include_package_data=True,        # so we can bundle the JSON contract
    package_data={
        "analytic_schema": ["analytic_schema.json"],
    },
    description="I/O schema loader + validator for analytics notebooks",
    author="Your Name",
    license="Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License",
)