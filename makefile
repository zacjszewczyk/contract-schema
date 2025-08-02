# ──────────────────────────────────────────────────────────────────────────────
# Makefile -- helper & packaging commands for model-schema
# ──────────────────────────────────────────────────────────────────────────────

.PHONY: default help clean push pull build verify publish-test publish package

# ─── Default target: show help ────────────────────────────────────────────────
default: help

help:
	@echo "Available commands:"
	@echo "  help           Show this message"
	@echo "  clean          Remove build artifacts"
	@echo "  push           Push all branches to origin & gitlab"
	@echo "  pull           Pull master from origin & gitlab"
	@echo
	@echo "  build          Build source & wheel distributions"
	@echo "  verify         Verify the distributions with twine"
	@echo "  publish-test   Upload distributions to TestPyPI"
	@echo "  publish        Upload distributions to PyPI"
	@echo "  package        Alias for build + verify"
	@echo "  redeploy       Alias for package + publish-test + publish"

# ─── Git helpers ──────────────────────────────────────────────────────────────
push:
	@echo "→ Pushing to origin and gitlab…"
	git push origin --all
	git -c http.sslverify=false push gitlab --all

pull:
	@echo "→ Pulling from origin/master and gitlab/master…"
	git checkout master
	git pull origin master
	git -c http.sslverify=false pull gitlab master

# ─── Packaging settings ──────────────────────────────────────────────────────
PACKAGE_NAME := model-schema
BUILD_DIR    := dist

# ─── Clean up build artifacts ────────────────────────────────────────────────
clean:
	@echo "→ Removing build artifacts…"
	rm -rf $(BUILD_DIR) build *.egg-info

# ─── Build source & wheel distributions ─────────────────────────────────────
build: clean
	@echo "→ Building source and wheel…"
	python3 -m build --sdist --wheel

# ─── Verify distributions with twine ─────────────────────────────────────────
verify: build
	@echo "→ Verifying archives…"
	twine check $(BUILD_DIR)/*

# ─── Upload to TestPyPI (dry-run) ────────────────────────────────────────────
publish-test: verify
	@echo "→ Uploading to TestPyPI…"
	twine upload --repository testpypi $(BUILD_DIR)/*

# ─── Upload to PyPI ─────────────────────────────────────────────────────────-
publish: verify
	@echo "→ Uploading to PyPI…"
	twine upload $(BUILD_DIR)/*

# ─── Alias targets ───────────────────────────────────────────────────────────
package: verify
redeploy: package publish-test publish