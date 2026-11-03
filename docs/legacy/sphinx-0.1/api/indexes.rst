Secondary Indexes
==================

.. module:: proto_db.indexes

ProtoBase provides a lightweight, immutable secondary index infrastructure that collections can leverage to accelerate queries and lookups.

Concepts
--------

IndexDefinition
~~~~~~~~~~~~~~~

.. autoclass:: IndexDefinition
   :members:
   :special-members: __init__

Defines an index name and an extractor callable that maps an item to one or more index keys. The extractor may return:

- a single key value (applies to this index name),
- an iterable of key values (applies to this index name), or
- an iterable of ``(index_name, key_value)`` tuples to feed multiple indexes.

IndexRegistry
~~~~~~~~~~~~~

.. autoclass:: IndexRegistry
   :members:
   :special-members: __init__

An immutable registry maintaining a mapping ``index_name -> {key_value -> frozenset(obj_id)}``. All updates (add/remove/replace) return a new registry, allowing structural sharing and safe reuse across versions of a collection.

Using indexes with collections
------------------------------

The built-in collections can maintain internal secondary indexes using ``IndexRegistry``:

- ``List``: call ``set_index_defs(defs)`` to configure index definitions. Each element gets a stable id (Atom.hash() if Atom, otherwise a derived stable hash).
- ``Dictionary`` and ``RepeatedKeysDictionary``: call ``set_index_defs(defs)``; the natural object id is the dictionary key.
- ``Set``: call ``set_index_defs(defs)``; the natural object id is the element hash.

Mutating operations return new collection instances with indexes updated functionally (``with_add``, ``with_remove``, ``with_replace``), avoiding recomputation.

Example
-------

.. code-block:: python

    from proto_db.indexes import IndexDefinition
    from proto_db.lists import List

    # Index list items by a field 'status'
    defs = (IndexDefinition(name='status', extractor=lambda row: row.get('status')),)

    lst = List()
    lst.set_index_defs(defs)

    # After appending items via List API, the internal indexes are kept consistent
    # and can be reused by index-aware query plans.
