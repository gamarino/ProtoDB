# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import os
import sys

sys.path.insert(0, os.path.abspath('../..'))

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'ProtoDB'
copyright = '2026, Gustavo Marino'
author = 'Gustavo Marino'
# Try to read version from pyproject.toml; fallback to semantic string
try:
    import tomllib  # Python 3.11+
    with open(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../pyproject.toml')), 'rb') as _f:
        _data = tomllib.load(_f)
        release = _data.get('project', {}).get('version', '2.0.0')
except Exception:
    release = '2.0.0'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.viewcode',
    'sphinx.ext.napoleon',
    'sphinx.ext.intersphinx',
]

# Optionally enable Markdown support via MyST if available
try:
    import myst_parser  # type: ignore  # noqa: F401
    extensions.append('myst_parser')
    source_suffix = {
        '.rst': 'restructuredtext',
        '.md': 'markdown',
    }
except Exception:
    # Fallback: only .rst files are processed
    source_suffix = {
        '.rst': 'restructuredtext',
    }

templates_path = ['_templates']
exclude_patterns = []

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

# Prefer Read the Docs theme if available; otherwise fallback to a built-in theme
try:
    import sphinx_rtd_theme  # type: ignore
    html_theme = 'sphinx_rtd_theme'
except Exception:  # pragma: no cover
    html_theme = 'alabaster'

html_static_path = []

# -- Extension configuration -------------------------------------------------
# Make autodoc robust when optional deps are missing
# Mock imports for optional/extra dependencies so autodoc doesn't fail
autodoc_mock_imports = [
    'pyarrow', 'pyarrow.parquet', 'pyarrow.dataset',
    'numpy', 'sklearn', 'sklearn.neighbors',
    'boto3', 'botocore', 'botocore.client',
    'google', 'google.cloud', 'google.cloud.storage', 'hnswlib',
]

autodoc_member_order = 'bysource'
autodoc_typehints = 'description'
napoleon_google_docstring = True
napoleon_numpy_docstring = False
napoleon_include_init_with_doc = True
napoleon_include_private_with_doc = False
napoleon_include_special_with_doc = True
napoleon_use_admonition_for_examples = False
napoleon_use_admonition_for_notes = False
napoleon_use_admonition_for_references = False
napoleon_use_ivar = False
napoleon_use_param = True
napoleon_use_rtype = True
napoleon_preprocess_types = False
napoleon_type_aliases = None
napoleon_attr_annotations = True


# -- Docstring hygiene -------------------------------------------------------------------------
# Two module docstrings in proto_db (the cluster coordinator's protocol notes and the Arrow bridge's
# mapping table) are plain text, not reStructuredText: parsed as RST they raise errors.  They are shown
# as literal blocks, whole, instead.  This hook is temporary: ruling R128 fixes those two docstrings
# and then removes it.
_LITERAL_MODULE_DOCSTRINGS = {"proto_db.cluster.coordinator", "proto_db.interop.arrow"}


def _literal_module_docstring(app, what, name, obj, options, lines):
    if what == "module" and name in _LITERAL_MODULE_DOCSTRINGS:
        lines[:] = ["::", ""] + ["   " + line for line in lines]


def setup(app):
    app.connect("autodoc-process-docstring", _literal_module_docstring)
