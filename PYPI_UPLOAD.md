# Uploading proto_db to PyPI

This document provides instructions for building and uploading the proto_db package to the Python Package Index (PyPI).

## Prerequisites

Before you can upload to PyPI, you need to:

1. Create an account on [PyPI](https://pypi.org/account/register/)
2. Install the required tools:
   ```bash
   pip install build twine
   ```

## Building the Package

To build the package, run the following command from the project root directory:

```bash
python -m build
```

This will create both source distribution (.tar.gz) and wheel (.whl) files in the `dist/` directory.

## Testing the Package

Before uploading to PyPI, it's a good idea to test the package by uploading it to the PyPI test server:

```bash
python -m twine upload --repository-url https://test.pypi.org/legacy/ dist/*
```

You can then install and test the package from the test server:

```bash
pip install --index-url https://test.pypi.org/simple/ --no-deps proto_db
```

## Uploading to PyPI

Once you've verified that the package works correctly, you can upload it to the real PyPI:

```bash
python -m twine upload dist/*
```

You'll be prompted for your PyPI username and password.

## Updating the Package

To update the package:

1. Update the version number in `pyproject.toml`
2. Build the package again
3. Upload the new version to PyPI

## Automating Uploads with GitHub Actions

You can automate the process of building and uploading to PyPI using GitHub Actions. Here's a sample workflow file:

```yaml
name: Upload Python Package

on:
  release:
    types: [created]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v2
    - name: Set up Python
      uses: actions/setup-python@v2
      with:
        python-version: '3.11'
    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install build twine
    - name: Build and publish
      env:
        TWINE_USERNAME: ${{ secrets.PYPI_USERNAME }}
        TWINE_PASSWORD: ${{ secrets.PYPI_PASSWORD }}
      run: |
        python -m build
        twine upload dist/*
```

To use this workflow, you'll need to add your PyPI username and password as secrets in your GitHub repository settings.

## Additional Resources

- [Packaging Python Projects](https://packaging.python.org/tutorials/packaging-projects/)
- [PyPI documentation](https://pypi.org/help/)
- [Twine documentation](https://twine.readthedocs.io/en/latest/)