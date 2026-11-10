"""Tests of proto_db.interop.arrow without pyarrow (Task 8.1, D42); they run whether or not pyarrow is installed.

Catches: pyarrow imported when the module is imported (0.1 imported it eagerly,
under ``except Exception``); a missing pyarrow reported as anything other than
``ProtoNotSupportedError`` (a ProtoDBError) chained from the ``ImportError``,
with an install hint.
"""
import subprocess
import sys

import pytest

from proto_db.errors import ProtoDBError, ProtoNotSupportedError
from proto_db.interop import arrow as bridge


def test_module_import_does_not_import_pyarrow():
    code = "import sys; import proto_db.interop.arrow; print(sys.modules.get('pyarrow') is not None)"
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert out.stdout.strip() == "False"


@pytest.mark.parametrize("call", [
    lambda: bridge.to_arrow([{"a": 1}]),
    lambda: bridge.vectors_fixed_size_list([[1.0]], 1),
    lambda: bridge.to_parquet([{"a": 1}], "x.parquet"),
    lambda: bridge.scan_parquet("x.parquet"),
    lambda: bridge.from_parquet_to_rows("x.parquet"),
])
def test_missing_pyarrow_raises_not_supported(monkeypatch, call):
    for name in ("pyarrow", "pyarrow.parquet", "pyarrow.dataset"):
        monkeypatch.setitem(sys.modules, name, None)  # makes "import pyarrow" raise ImportError
    with pytest.raises(ProtoNotSupportedError, match="pip install") as info:
        call()
    assert isinstance(info.value, ProtoDBError)
    assert isinstance(info.value.__cause__, ImportError)
