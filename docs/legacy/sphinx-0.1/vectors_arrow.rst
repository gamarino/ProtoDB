Vectors and Arrow Bridge
========================

ProtoBase includes optional utilities for vector data and for interoperability with Apache Arrow/Parquet.

Vectors
-------

The proto_db.vectors module provides a small Vector type that can wrap existing buffers without copy
and export views back to NumPy when available.

.. code-block:: python

    from proto_db.vectors import Vector

    # From a Python buffer (array, memoryview, bytes) without copying when possible
    import array
    a = array.array('f', [1.0, 2.0, 3.5])
    v = Vector.from_buffer(memoryview(a), dtype='float32', copy='auto')

    # Export as a memoryview
    mv = v.as_buffer()

    # If NumPy is present, export zero‑copy view when compatible
    try:
        arr = v.as_numpy(copy=False)
    except Exception:
        arr = None

Arrow/Parquet bridge
--------------------

The arrow_bridge helpers convert simple row data to Arrow tables and write Parquet files when pyarrow
is available. If the dependency is not installed, ArrowNotAvailable is raised.

.. code-block:: python

    from proto_db.arrow_bridge import to_arrow, table_to_parquet, ArrowNotAvailable

    rows = [{"a": 1}, {"a": 2}]
    try:
        tbl = to_arrow(rows, columns=["a"])  # returns pyarrow.Table
        table_to_parquet(tbl, "out.parquet")
    except ArrowNotAvailable:
        print("Install pyarrow to enable Arrow/Parquet features")
