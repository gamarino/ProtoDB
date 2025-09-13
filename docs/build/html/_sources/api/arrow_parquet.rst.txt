Arrow / Parquet Bridge
=======================

.. module:: proto_db.arrow_bridge

ProtoBase provides an optional, minimal-overhead bridge to Apache Arrow / Parquet when ``pyarrow`` is available.
The bridge focuses on copy-avoidance and pragmatic APIs suitable for exporting/importing data and working with
vector columns as FixedSizeList<float32>[dim].

Availability
------------

These APIs require ``pyarrow`` to be installed. If not available, calling them raises ``ArrowNotAvailable``.

Export
------

.. autofunction:: to_arrow
.. autofunction:: table_to_parquet
.. autofunction:: to_parquet_from_records

Import / Scan
-------------

.. autofunction:: scan_parquet
.. autofunction:: from_parquet_to_rows

Vectors
-------

.. autofunction:: vectors_fixed_size_list

Notes
-----
- Copy control is left to the caller by providing numpy arrays (zero-copy where supported by Arrow).
- Vector columns should use float32 and Arrow FixedSizeList for best interoperability.
