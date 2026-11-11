# Building the documentation (HTML)

This repository ships with a Sphinx documentation project under `docs/`.

## Why the build needs the project's environment

The API pages use `autodoc`, which imports `proto_db`.  `proto_db` refuses to import with a
msgpack older than 1.2.0 (the format needs a packer that nests 1024 containers; see
`format-spec.rst`).  A Sphinx installed on the system Python therefore fails when that Python's
msgpack is older, whatever the docs say.  Build the docs with the Python of the environment in
which the package and its dependencies are installed, never with a system `sphinx-build`.

`docs/requirements.txt` lists what the build needs on top of the package.

## Build

From the repository root, with the project's virtual environment (`.venv314t` on the
maintainer's machine; any environment with `proto_db` and `docs/requirements.txt` installed
works):

```bash
# Once: install the documentation requirements into the project's environment.
uv pip install --python .venv314t/bin/python -r docs/requirements.txt
#   (or, inside an activated environment: python -m pip install -r docs/requirements.txt)

# Build HTML into docs/build/html; -W turns every warning into an error, as CI does.
.venv314t/bin/python -m sphinx -W -b html docs/source docs/build/html
```

A fresh environment needs the package itself as well:

```bash
python -m venv .venv-docs
.venv-docs/bin/python -m pip install -e . -r docs/requirements.txt
.venv-docs/bin/python -m sphinx -W -b html docs/source docs/build/html
```

`make -C docs html SPHINXBUILD=".venv314t/bin/python -m sphinx"` runs the same build through the
Makefile.

After a successful build, open `docs/build/html/index.html`.

## Troubleshooting

- `ProtoNotSupportedError: ... msgpack ...` during autodoc: the Sphinx in use runs on a Python
  whose msgpack is older than 1.2.0.  Use the project's environment as above.
- Optional integrations (`pyarrow`, `numpy`, `boto3`, `google-cloud-storage`, `hnswlib`) need not
  be installed: `autodoc_mock_imports` in `docs/source/conf.py` mocks them.
