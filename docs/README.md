# ProtoBase Documentation

This directory contains the documentation for the ProtoBase project.

## Building the Documentation

### Prerequisites

- Python 3.11 or higher
- Sphinx
- sphinx_rtd_theme

You can install the required packages with:

```bash
pip install sphinx sphinx_rtd_theme
```

### Building HTML Documentation

To build the HTML documentation:

#### On Linux/macOS:

```bash
cd docs
make html
```

#### On Windows:

```bash
cd docs
make.bat html
```

The generated HTML documentation will be in the `docs/build/html` directory. You can open `docs/build/html/index.html`
in a web browser to view it.

### Building Other Formats

The documentation can also be built in other formats:

#### On Linux/macOS:

```bash
cd docs
make latex    # Build LaTeX documentation
make pdf      # Build PDF documentation (requires LaTeX)
make epub     # Build EPUB documentation
```

#### On Windows:

```bash
cd docs
make.bat latex    # Build LaTeX documentation
make.bat pdf      # Build PDF documentation (requires LaTeX)
make.bat epub     # Build EPUB documentation
```

## Documentation Structure

The documentation is organized as follows:

- `source/index.rst`: The main index file
- `source/introduction.rst`: Introduction to ProtoBase
- `source/installation.rst`: Installation instructions
- `source/quickstart.rst`: Quickstart guide
- `source/architecture.rst`: Architecture overview
- `source/api/`: API reference documentation
- `source/advanced_usage.rst`: Advanced usage guide
- `source/development.rst`: Development guide

## Contributing to the Documentation

If you want to contribute to the documentation:

1. Make your changes to the relevant `.rst` files in the `source` directory
2. Build the documentation to verify your changes
3. Submit a pull request with your changes

## License

The documentation is licensed under the same license as the ProtoBase project.