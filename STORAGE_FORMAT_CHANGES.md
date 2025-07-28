# Storage Format Changes in proto_db

## Overview

This document describes the changes made to the storage format in proto_db to support multiple serialization formats. The changes include:

1. Adding a format indicator after the size header to specify the serialization format
2. Supporting JSON UTF-8 (the original format) and MessagePack (a new efficient binary format)
3. Maintaining backward compatibility with existing data

## Storage Format

### Original Format

The original storage format consisted of:

```
+----------------+----------------+
| Size (8 bytes) | Data (n bytes) |
+----------------+----------------+
```

Where:
- **Size**: An 8-byte unsigned long (Q) that indicates the size of the data in bytes
- **Data**: The serialized data (JSON UTF-8 encoded for atoms, raw bytes for binary data)

### New Format

The new storage format adds a format indicator after the size header:

```
+----------------+---------------------+----------------+
| Size (8 bytes) | Format (1 byte)     | Data (n bytes) |
+----------------+---------------------+----------------+
```

Where:
- **Size**: An 8-byte unsigned long (Q) that indicates the size of the data in bytes
- **Format**: A 1-byte indicator that specifies the serialization format:
  - `0x00`: Raw binary data (no serialization)
  - `0x01`: JSON UTF-8 (for backward compatibility, this is the default for atoms)
  - `0x02`: MessagePack
- **Data**: The serialized data in the specified format

## Backward Compatibility

To maintain backward compatibility with existing data, the system checks if the byte after the size header is a valid format indicator. If it is, the system uses the corresponding deserialization method. If not, it assumes the data is in the original format (JSON UTF-8 without a format indicator) and includes the byte as part of the data.

## New Methods

The following new methods have been added to support the new formats:

### `push_atom(atom: dict, format_type: int = FORMAT_JSON_UTF8) -> Future[AtomPointer]`

This method now accepts an optional `format_type` parameter that specifies the serialization format to use. The default is `FORMAT_JSON_UTF8` for backward compatibility.

### `push_atom_msgpack(atom: dict) -> Future[AtomPointer]`

A convenience method that calls `push_atom` with `FORMAT_MSGPACK`. This method serializes the atom using MessagePack, which is more efficient than JSON for many use cases.

### `push_bytes(data: bytes, format_type: int = FORMAT_RAW_BINARY) -> Future[tuple[uuid.UUID, int]]`

This method now accepts an optional `format_type` parameter that specifies the format of the data. The default is `FORMAT_RAW_BINARY` for backward compatibility.

### `push_bytes_msgpack(data: dict) -> Future[tuple[uuid.UUID, int]]`

A convenience method that serializes a dictionary to MessagePack format and then calls `push_bytes` with `FORMAT_MSGPACK`.

## Benefits

The new storage format provides several benefits:

1. **Flexibility**: The system can now support multiple serialization formats, allowing for more efficient storage and retrieval of data.
2. **Efficiency**: MessagePack is more compact and faster to serialize/deserialize than JSON for many use cases.
3. **Future-proofing**: The format indicator system allows for adding new serialization formats in the future without breaking backward compatibility.
4. **Cross-language compatibility**: Both JSON and MessagePack are widely supported across programming languages, making it easier to implement proto_db in other languages while maintaining a consistent storage format.

## Usage Examples

### Using JSON UTF-8 (default)

```python
# Push an atom with JSON UTF-8 format (default)
atom = {"className": "TestAtom", "attr1": "value1", "attr2": 123}
future = storage.push_atom(atom)
atom_pointer = future.result()
```

### Using MessagePack

```python
# Push an atom with MessagePack format
atom = {"className": "TestAtom", "attr1": "value1", "attr2": 123}
future = storage.push_atom_msgpack(atom)
atom_pointer = future.result()

# Or using the format_type parameter
future = storage.push_atom(atom, format_type=FORMAT_MSGPACK)
atom_pointer = future.result()

# Push a dictionary with MessagePack format
data = {"key1": "value1", "key2": 123}
future = storage.push_bytes_msgpack(data)
transaction_id, offset = future.result()
```