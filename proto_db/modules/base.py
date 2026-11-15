"""The registry module ``base``: the default mechanisms (runtime registry, spec section 7).

``base`` is installed first, with no dependencies, in every database that installs a module
(ruling R205: ``Transaction.install_module`` adds it before anything else when the list lacks
it, and ``Registry.build`` refuses a non-empty list that does not start with it).  A database
that never installs a module has no ``base`` and pays nothing.  The registry module ``base``
extends the registry name ``base``, which every other registry name has as its implicit last
parent (ruling R204, :meth:`proto_db.registry.Registry.parents_of`): an object whose parents
reach any registry name finds ``base``'s definitions last, after every module's, so any module
may override them and reach them with ``self._super()``.  An object with no registry-name
parent never reaches ``base``.

The default mapping imports the registry module ``base`` from this Python module; a runtime's
``modules_mapping`` may point it elsewhere.  ``base`` is always allowed, whatever the
runtime's ``modules_allowed`` (ruling R207).

Definitions
    ``read_data_file(self, path, *, module=None, binary=False)``
        Read the data file ``path`` (a view definition, a template, ...) of a registry module
        and return its content: a ``str`` decoded as UTF-8, or ``bytes`` with ``binary=True``
        (ruling R206).  Other modules override it to transform or replace what is read, as
        Odoo's ``get_view`` is overridden, and call ``self._super().read_data_file(path,
        module=module, binary=binary)`` to reach the next definition; this one, last, reads the
        file.

        ``path`` is relative to the directory of the Python module that declares the registry
        module ``module`` (``os.path.dirname`` of its ``__file__``).  When ``module`` is omitted,
        it is the registry module of the method that asked for the file: the nearest calling
        frame whose code is a registry method that is not itself a definition of
        ``read_data_file`` (so an override that passes ``module=None`` on to ``_super()`` keeps
        the asking module, ruling R208).  When that frame is not a registry method (code outside
        the registry, a helper function, a closure), ``ProtoUserError`` asks for ``module=``.

        Refused with ``ProtoValidationError``: a ``path`` that is not a non-empty ``str``, that
        holds a NUL character, that is absolute (or names a drive), or that leaves the module's
        directory once resolved -- by ``..`` or through a symbolic link, both resolved with
        :func:`os.path.realpath` before the check -- and a ``module`` the object's registry does
        not contain.  The resolved path is the one opened.  A file that does not exist, or
        cannot be read, raises the ``OSError`` of ``open`` unchanged; content that is not UTF-8
        raises ``UnicodeDecodeError``.  The check confines reads to the module's directory
        against a mistaken or data-supplied path; it is not a boundary against whoever can write
        that directory (they can change the module's code).
"""
from __future__ import annotations

import os
import sys

from proto_db.errors import ProtoNotSupportedError, ProtoUserError, ProtoValidationError
from proto_db.registry import BASE, METHOD, module

base = module(BASE)

_READ = "read_data_file"


def _asking_module(registry, frame) -> str:
    """The registry module of the method that asked for a data file: walking outward from
    ``frame``, the first registry method that is not a definition of ``read_data_file``."""
    start = frame
    while frame is not None:
        extension = registry.extension_of_code(frame.f_code)
        if extension is None:
            break
        kind, value = extension.definitions.get(_READ, (None, None))
        if kind is METHOD and value.__code__ is frame.f_code:
            frame = frame.f_back            # an override passing the request on
            continue
        return extension.module
    code = (frame or start).f_code
    raise ProtoUserError(
        f"read_data_file() called from {code.co_qualname} ({code.co_filename}:{code.co_firstlineno}), which is "
        f"not a registry method: pass module= to name the registry module whose data file it reads")


def _module_directory(registry, name) -> str:
    source = registry.source_of(name) if type(name) is str else None
    if source is None:
        raise ProtoValidationError(f"read_data_file(): module {name!r} is not in the registry {registry.key!r}")
    found = sys.modules.get(source)
    filename = getattr(found, "__file__", None)
    if not filename:
        raise ProtoNotSupportedError(
            f"read_data_file(): the Python module {source!r} of registry module {name!r} has no file, so its "
            f"data files have no directory")
    return os.path.realpath(os.path.dirname(filename))


def _resolved(directory: str, name: str, path) -> str:
    if type(path) is not str or not path:
        raise ProtoValidationError(f"read_data_file(): the path must be a non-empty str, not {path!r}")
    if "\0" in path:
        raise ProtoValidationError(f"read_data_file(): the path {path!r} holds a NUL character")
    if os.path.isabs(path) or os.path.splitdrive(path)[0]:
        raise ProtoValidationError(
            f"read_data_file(): the path {path!r} is absolute; a data file is named relative to its module's directory")
    target = os.path.realpath(os.path.join(directory, path))
    if os.path.commonpath((directory, target)) != directory:
        raise ProtoValidationError(
            f"read_data_file(): the path {path!r} leaves the directory of module {name!r} ({directory})")
    return target


@base.extends(BASE)
class Base:
    def read_data_file(self, path, *, module=None, binary=False):
        """Read the data file ``path`` of the registry module ``module`` (by default, the module
        of the method that asked); see the module docstring."""
        from proto_db.resolution import registry_of
        registry = registry_of(self)
        if registry is None:
            raise ProtoUserError("read_data_file() called with no registry current")
        if module is None:
            frame = sys._getframe(1)
            try:
                module = _asking_module(registry, frame)
            finally:
                del frame
        directory = _module_directory(registry, module)
        target = _resolved(directory, module, path)
        with open(target, "rb") as f:
            data = f.read()
        return data if binary else data.decode("utf-8")
