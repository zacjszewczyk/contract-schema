# ==============================================================================
# Makefile for the contract-schema project
#
# This file provides a set of commands to streamline common development tasks,
# including testing, cleaning, building, and publishing the Python package.
# It also includes helper commands for Git operations and other custom scripts.
# ==============================================================================

# ------------------------------------------------------------------------------
# Phony Targets
#
# Declares targets that are not actual files. This prevents `make` from
# being confused by files that might have the same name as a target. All
# command targets should be listed here.
# ------------------------------------------------------------------------------
.PHONY: default help clean test build verify publish-test publish package redeploy push pull gi gi-mod


# ------------------------------------------------------------------------------
# Configuration
#
# Defines variables used throughout the Makefile for consistency. These can be
# modified to match project-specific naming conventions.
# ------------------------------------------------------------------------------
PACKAGE_NAME := contract-schema
BUILD_DIR    := dist


# ------------------------------------------------------------------------------
# Core Commands
#
# These are the primary, standalone commands for interacting with the project.
# ------------------------------------------------------------------------------

# The default target that is executed when `make` is run without arguments.
# It prints the help message to guide the user.
default: help

# Displays a helpful message listing all available commands and their functions.
# This serves as the primary documentation for the Makefile's usage.
help:
    @echo "Available commands:"
    @echo
    @echo "  Core & Testing"
    @echo "    help          Show this help message."
    @echo "    clean         Remove all build artifacts and temporary files."
    @echo "    test          Run the project's unit tests."
    @echo
    @echo "  Python Package Management"
    @echo "    build         Build the source and wheel distributions."
    @echo "    verify        Verify the integrity of the built distributions."
    @echo "    publish-test  Upload the package to the TestPyPI repository."
    @echo "    publish       Upload the package to the official PyPI repository."
    @echo
    @echo "  Git Operations"
    @echo "    push          Push all branches to both 'origin' and 'gitlab' remotes."
    @echo "    pull          Pull the master branch from 'origin' and 'gitlab'."
    @echo
    @echo "  Alias & Convenience Commands"
    @echo "    package       A shortcut for the 'build' and 'verify' sequence."
    @echo "    redeploy      A shortcut to package and publish to both TestPyPI and PyPI."
    @echo "    gi            Run the 'gitingest' script with default settings."
    @echo "    gi-mod        Run 'gitingest', excluding common project metadata."

# Removes temporary build artifacts and caches. This is useful for ensuring
# a clean state before a new build, preventing issues with outdated files.
clean:
    @echo "Removing build artifacts..."
    rm -rf $(BUILD_DIR) build *.egg-info


# ------------------------------------------------------------------------------
# Testing
# ------------------------------------------------------------------------------

# Runs the project's test suite using the standard `unittest` module.
# It first attempts to use `python3` and falls back to `python` if the
# initial command fails, providing compatibility across different systems.
test:
    python3 -m unittest discover -s tests || python -m unittest discover -s tests


# ------------------------------------------------------------------------------
# Python Package Management
#
# These targets handle the full lifecycle of packaging and distributing the
# Python library, from building the assets to publishing them.
# ------------------------------------------------------------------------------

# Builds the source (.tar.gz) and wheel (.whl) distributions for the package.
# This target depends on `clean`, ensuring that every build is fresh and
# does not include any old artifacts. It uses the `build` package.
build: clean
    @echo "Building source and wheel distributions..."
    python3 -m build --sdist --wheel

# Checks the built distributions for validity and metadata correctness.
# This target depends on `build` to ensure there are packages to check.
# It uses `twine check` to catch potential uploading issues early.
verify: build
    @echo "Verifying archives with twine..."
    twine check $(BUILD_DIR)/*

# Uploads the package to the TestPyPI repository for a dry run. This allows
# developers to verify the package rendering and installation process in a
# low-stakes environment before a real release.
publish-test: verify
    @echo "Uploading to TestPyPI..."
    twine upload --repository testpypi $(BUILD_DIR)/*

# Uploads the package to the official Python Package Index (PyPI).
# This is the final step for a public release and should only be run when
# the package version is ready for production use.
publish: verify
    @echo "Uploading to PyPI..."
    twine upload $(BUILD_DIR)/*


# ------------------------------------------------------------------------------
# Git Operations
#
# Helper commands to simplify interacting with multiple Git remotes.
# ------------------------------------------------------------------------------

# Pushes all local branches to both the `origin` and `gitlab` remotes.
# The push to `gitlab` explicitly disables SSL verification, which may be
# necessary for self-hosted GitLab instances with custom certificates.
push:
    @echo "Pushing to origin and gitlab..."
    git push origin --all
    git -c http.sslverify=false push gitlab --all

# Pulls the latest changes from the `master` branch of both `origin` and
# `gitlab` remotes. It first checks out the `master` branch locally.
# Like the `push` command, it disables SSL verification for `gitlab`.
pull:
    @echo "Pulling from origin/master and gitlab/master..."
    git checkout master
    git pull origin master
    git -c http.sslverify=false pull gitlab master


# ------------------------------------------------------------------------------
# Alias & Convenience Targets
#
# These targets are shortcuts that chain other commands together to simplify
# common multi-step workflows.
# ------------------------------------------------------------------------------

# A convenience target that runs the full build and verification sequence.
# Its dependency on `verify` implicitly triggers the `build` and `clean`
# targets in the correct order.
package: verify

# A high-level alias to build, verify, and publish to both TestPyPI and PyPI.
# This command automates the entire release process.
redeploy: package publish-test publish

# A custom script alias that runs the `gitingest` tool with its default settings.
# The purpose of `gitingest` is assumed to be project-specific.
gi:
    gitingest

# A custom script alias that runs `gitingest` with specific exclusions.
# This is useful for creating a context that omits common project metadata,
# documentation, and test files.
gi-mod:
    gitingest -e "README.md, example_analytic.py, example_model.py, LICENSE.md, makefile, pyproject.toml, tests/"