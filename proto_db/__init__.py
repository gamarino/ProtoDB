# Lightweight package initializer to avoid heavy imports and circular dependencies during testing.
# Export exceptions into package namespace for modules that import from proto_db import ProtoCorruptionException, etc.
from .exceptions import (
    ProtoBaseException, ProtoUserException, ProtoCorruptionException,
    ProtoValidationException, ProtoNotSupportedException, ProtoNotAuthorizedException
)

# Optional: expose storage backends if dependencies are available, otherwise keep None to avoid ImportError
try:
    from .cloud_file_storage import CloudFileStorage, CloudBlockProvider, S3Client
    from .cluster_file_storage import ClusterFileStorage
    from .standalone_file_storage import StandaloneFileStorage
except Exception:
    CloudFileStorage = None
    CloudBlockProvider = None
    S3Client = None
    ClusterFileStorage = None
    StandaloneFileStorage = None
