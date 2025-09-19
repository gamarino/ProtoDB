# Building the documentation (HTML)

This repository ships with a Sphinx documentation project under `docs/`.
It also includes prebuilt HTML under `docs/build/` so you can browse it right away.

If you want to rebuild the HTML from sources, follow these steps.

## Prerequisites

- Python 3.11+
- Virtual environment (recommended)
- Sphinx and a few optional extensions

Install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -U pip
pip install sphinx sphinx-rtd-theme myst-parser
```

Note: The Sphinx configuration mocks optional heavy dependencies (e.g. `pyarrow`,
`numpy`, `boto3`) so they are not required to build the docs.

## Build commands

From the repository root:

```bash
# Build HTML into docs/build/html
sphinx-build -b html docs/source docs/build/html

# Or using the classic Makefile (if available on your system):
# make -C docs html
```

After a successful build, open:

```
docs/build/html/index.html
```

## Troubleshooting

- If your shell cannot find `sphinx-build`, ensure your virtual environment is
  activated and that `pip show sphinx` lists the installation path.
- If you modify the Python package and rely on `autodoc`, install the package in
  editable mode so Sphinx can import it:

```bash
pip install -e .
```

- If optional integrations are missing on your machine, the docs will still
  build thanks to `autodoc_mock_imports` configured in `docs/source/conf.py`.
