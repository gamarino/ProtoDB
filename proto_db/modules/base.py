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
``modules_mapping`` may point it elsewhere.  When ``install_module`` adds it implicitly (for
another module) or a module list that holds it is built, ``base`` is allowed whatever the
runtime's ``modules_allowed``; an explicit ``install_module("base")`` obeys ``modules_allowed``
(ruling R207 as narrowed by review M8).  An extension of ``base`` may not declare parents
(``base`` is the root of the registry-name graph; review I1).

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
        holds a NUL character, that is absolute -- a POSIX or Windows root (``/x``, ``\\x``), a
        drive (``C:x``, ``C:\\x``) or a UNC path, on every platform -- or that leaves the module's
        directory once resolved (by ``..`` or through a symbolic link, resolved with
        :func:`os.path.realpath`; a containment that cannot be compared, such as another Windows
        drive, is refused too), a file that is not a regular file (a FIFO, a device, a
        directory), and a ``module`` the object's registry does not contain (ruling R209).
        Symbolic links that stay inside the directory are followed.

        The checks do not depend on timing (review I2): after the path checks, the file is
        opened (without blocking, so a FIFO cannot hang the open), checked to be a regular file,
        and its real location is taken **from the open descriptor** (``/proc/self/fd`` on Linux,
        ``F_GETPATH`` on macOS; elsewhere ``realpath`` after the open, accepted only when it
        names the very file opened, :func:`os.path.samestat`) and checked again to be inside the
        module's directory; the content is read from that descriptor.  A component swapped
        between the path check and the open is therefore refused.  A file that does not exist,
        or cannot be opened, raises the ``OSError`` of the open unchanged; content that is not
        UTF-8 raises ``UnicodeDecodeError``.  On a case-insensitive file system (macOS APFS)
        ``realpath`` keeps the spelling given, so a path that climbs out and back in with other
        letter case is refused (a false refusal, the safe direction; review M3).

        A module imported from a zip archive (or by any loader without a directory) has no data
        directory: ``ProtoNotSupportedError`` (review M4; ``importlib.resources`` would be the
        way to support it later).

Reserved names (review M5)
    Every registry linearization ends with ``base``, so the names ``base`` defines are found by
    every object that reaches a registry name, and the Python-members check of
    :mod:`proto_db.resolution` refuses a ``DBObject`` subclass that defines one of them as a
    Python member (it would hide the registry definition).  ``base`` therefore keeps its public
    names few and distinctive: today only ``read_data_file``.
"""
from __future__ import annotations

import ntpath
import os
import stat
import sys
import zipimport

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
    loader = getattr(getattr(found, "__spec__", None), "loader", None)
    if isinstance(loader, zipimport.zipimporter):
        raise ProtoNotSupportedError(
            f"read_data_file(): module {name!r} has no directory (imported from a zip archive, "
            f"Python module {source!r})")
    directory = os.path.realpath(os.path.dirname(filename)) if filename else None
    if directory is None or not os.path.isdir(directory):
        raise ProtoNotSupportedError(
            f"read_data_file(): module {name!r} has no directory (its Python module {source!r} was not "
            f"imported from a file)")
    return directory


def _checked_path(directory: str, name: str, path) -> str:
    """The path to open for ``path`` (joined to ``directory``), after the checks that need no
    file: type, NUL, absolute, and containment of its ``realpath``."""
    if type(path) is not str or not path:
        raise ProtoValidationError(f"read_data_file(): the path must be a non-empty str, not {path!r}")
    if "\0" in path:
        raise ProtoValidationError(f"read_data_file(): the path {path!r} holds a NUL character")
    if os.path.isabs(path) or path[:1] in ("/", "\\") or ntpath.splitdrive(path)[0]:
        raise ProtoValidationError(
            f"read_data_file(): the path {path!r} is absolute; a data file is named relative to its module's directory")
    joined = os.path.join(directory, path)
    _check_inside(directory, name, path, os.path.realpath(joined))
    return joined


def _check_inside(directory: str, name: str, path, real: str) -> None:
    """``ProtoValidationError`` unless ``real`` is inside ``directory`` (both real paths)."""
    try:
        inside = os.path.commonpath((directory, real)) == directory
    except ValueError:                  # another drive, or absolute against relative: not inside
        inside = False
    if not inside:
        raise ProtoValidationError(
            f"read_data_file(): the path {path!r} leaves the directory of module {name!r} ({directory})")


#: Hooks (tests replace them): the open, and the realpath of the fallback location.
_open = os.open
_realpath_after_open = os.path.realpath

_FLAGS = (os.O_RDONLY | getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_BINARY", 0)
          | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOCTTY", 0))


def _descriptor_path(fd: int) -> str | None:
    """Where the open descriptor ``fd`` really points, from the descriptor itself, or ``None``
    when the platform cannot tell (then :func:`_read_inside` uses the fallback)."""
    if sys.platform.startswith("linux"):
        try:
            return os.readlink(f"/proc/self/fd/{fd}")
        except OSError:
            return None
    if sys.platform == "darwin":
        import fcntl
        try:
            raw = fcntl.fcntl(fd, fcntl.F_GETPATH, bytes(1024))
        except OSError:
            return None
        return os.fsdecode(raw.split(b"\0", 1)[0])
    return None


def _read_inside(directory: str, name: str, path: str, joined: str) -> bytes:
    """Open ``joined``, check the open file (regular, really inside ``directory``), and read it
    from the descriptor: no check depends on the file staying put after it (review I2)."""
    fd = _open(joined, _FLAGS)
    try:
        opened = os.fstat(fd)
        if not stat.S_ISREG(opened.st_mode):
            raise ProtoValidationError(
                f"read_data_file(): the path {path!r} of module {name!r} is not a regular file")
        real = _descriptor_path(fd)
        if real is not None:
            _check_inside(directory, name, path, real)
        else:
            real = _realpath_after_open(joined)
            _check_inside(directory, name, path, real)
            try:
                same = os.path.samestat(opened, os.stat(real))
            except OSError:
                same = False
            if not same:
                raise ProtoValidationError(
                    f"read_data_file(): the file {path!r} of module {name!r} changed while it was opened")
        f = os.fdopen(fd, "rb")
        fd = None
    finally:
        if fd is not None:
            os.close(fd)
    with f:
        return f.read()


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
        data = _read_inside(directory, module, path, _checked_path(directory, module, path))
        return data if binary else data.decode("utf-8")
