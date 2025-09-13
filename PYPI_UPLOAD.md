# Uploading proto_db to PyPI

This guide describes how to build, verify, and publish the proto_db package to PyPI (and TestPyPI). It also includes a reproducible local smoke test and CI automation.

## Prerequisites

- Python 3.11+
- A PyPI account and API token (and optionally a TestPyPI token)
- Tools:
  ```bash
  python -m pip install --upgrade build twine
  ```

## Build and verify locally

Run from the project root:

```bash
# Clean previous artifacts
rm -rf dist build *.egg-info

# Build sdist and wheel
python -m build

# Validate metadata
python -m twine check dist/*

# Smoke test install from the built wheel in a clean venv
python -m venv .venv-test
. .venv-test/bin/activate
python -m pip install --upgrade pip
pip install dist/*.whl
python - <<'PY'
import proto_db
print('proto_db', proto_db.__version__)
PY

# Optional: smoke test extras (if you have deps available)
# Parquet/Arrow
# pip install "dist/*.whl[parquet]" && python -c "import proto_db.arrow_bridge as ab; print('arrow ok')"
# Vectors
# pip install "dist/*.whl[vectors]" && python -c "import numpy as np; print('vectors ok')"

deactivate
```

## Test upload to TestPyPI

```bash
python -m twine upload --repository testpypi dist/*
# Then test install
pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple proto_db
```

## Publish to PyPI

```bash
python -m twine upload dist/*
```

Use an API token stored as `__token__` / `pypi-XXXX` when prompted or via environment variables in CI.

## Versioning and release process

1. Bump version in `proto_db/__init__.py` (and keep it in sync with `pyproject.toml`).
2. Update `CHANGELOG.md`.
3. Commit and tag the release: `git tag vX.Y.Z && git push --tags`.
4. CI will build and publish on tags (see workflow below).

## GitHub Actions (publish on tags)

Create `.github/workflows/publish.yml`:

```yaml
name: Publish to PyPI / TestPyPI

on:
  push:
    tags:
      - 'v*'

jobs:
  build-and-publish:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.12'
      - name: Install build tooling
        run: |
          python -m pip install --upgrade pip
          pip install build twine
      - name: Build
        run: |
          python -m build
          python -m twine check dist/*
      - name: Publish to TestPyPI for pre-releases
        if: contains(github.ref_name, 'rc') || contains(github.ref_name, 'beta') || contains(github.ref_name, 'a') || contains(github.ref_name, 'b')
        env:
          TWINE_USERNAME: __token__
          TWINE_PASSWORD: ${{ secrets.TEST_PYPI_API_TOKEN }}
        run: |
          python -m twine upload --repository testpypi dist/*
      - name: Publish to PyPI for stable tags
        if: ${{ ! (contains(github.ref_name, 'rc') || contains(github.ref_name, 'beta') || contains(github.ref_name, 'a') || contains(github.ref_name, 'b')) }}
        env:
          TWINE_USERNAME: __token__
          TWINE_PASSWORD: ${{ secrets.PYPI_API_TOKEN }}
        run: |
          python -m twine upload dist/*
```

Optional: add a separate job to run unit tests before publishing.

## Optional extras installation in docs

Users can enable optional functionality via extras:

- Parquet/Arrow: `pip install "proto_db[parquet]"`
- Vectors: `pip install "proto_db[vectors]"`
- Dev tooling: `pip install "proto_db[dev]"`

## References

- Packaging Python Projects: https://packaging.python.org/tutorials/packaging-projects/
- Twine: https://twine.readthedocs.io/en/latest/
- TestPyPI: https://test.pypi.org/