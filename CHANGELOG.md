# Changelog

All notable changes to ProtoBase will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2023

### Added

#### Google Cloud Storage Support

- Added `GoogleCloudClient` as an implementation of `CloudStorageClient` for Google Cloud Storage
- Ensured that `CloudBlockProvider`, `CloudFileStorage`, and `CloudClusterFileStorage` work with both `S3Client` and `GoogleCloudClient`
- Added comprehensive test suites for Google Cloud Storage integration
- Added usage examples for Google Cloud Storage in documentation

#### Multiple Serialization Formats Support

- Added a format indicator after the size header to specify the serialization format
- Added support for JSON UTF-8 (the original format) and MessagePack (a new efficient binary format)
- Maintained backward compatibility with existing data
- Added new methods to support the new formats:
  - `push_atom(atom: dict, format_type: int = FORMAT_JSON_UTF8) -> Future[AtomPointer]`
  - `push_atom_msgpack(atom: dict) -> Future[AtomPointer]`
  - `push_bytes(data: bytes, format_type: int = FORMAT_RAW_BINARY) -> Future[tuple[uuid.UUID, int]]`
  - `push_bytes_msgpack(data: dict) -> Future[tuple[uuid.UUID, int]]`

#### PyPI Package Support

- Created `pyproject.toml` with package metadata, dependencies, and build system information
- Added MIT License file
- Created `MANIFEST.in` to include non-Python files in the package distribution
- Added `test_install.py` script to test the local installation of the package
- Created `PYPI_UPLOAD.md` with documentation on how to build and upload the package to PyPI
- Updated README.md with installation instructions and license information

#### Advanced Storage Options Documentation

- Added comprehensive documentation for `ClusterFileStorage` and `CloudFileStorage`
- Updated `__init__.py` to expose the advanced storage options
- Added detailed use cases for the advanced storage options

### Fixed

#### CloudFileStorage Test Hanging Issue

- Added an `uploader_running` flag to control the background uploader thread
- Modified the `_background_uploader` method to check both the `state` and the `uploader_running` flag
- Updated the `close` method to properly stop the background uploader thread and wait for it to finish
- Added test verification to ensure the uploader thread is properly stopped

### Changed

- Completed the implementation of `S3Client` by adding missing abstract methods:
  - `_init_client`: Initializes the S3 client using boto3 or falls back to a mock implementation
  - `get_object`: Gets an object from S3
  - `put_object`: Puts an object to S3
  - `list_objects`: Lists objects in S3
  - `delete_object`: Deletes an object from S3