# Lightweight package initializer to avoid heavy imports and circular dependencies during testing.
# Export exceptions into package namespace for modules that import from proto_db import ProtoCorruptionException, etc.

# Optional: expose storage backends if dependencies are available, otherwise keep None to avoid ImportError
from .exceptions import ProtoBaseException, ProtoUserException, ProtoCorruptionException, \
    ProtoValidationException, ProtoNotSupportedException, ProtoNotAuthorizedException
from . import common
from . import dictionaries
from . import exceptions
from . import lists
from . import sets
from .cloud_file_storage import CloudFileStorage, CloudBlockProvider, S3Client
from .cluster_file_storage import ClusterFileStorage
from .common import Atom, Literal, DBObject, MutableObject, DBCollections, QueryPlan, ConcurrentOptimized
from .db_access import ObjectSpace, Database, ObjectTransaction, BytesAtom
from .fsm import Timer, FSM
from .hash_dictionaries import HashDictionary
from .lists import List
from .dictionaries import Dictionary, RepeatedKeysDictionary
from .sets import Set
from .file_block_provider import FileBlockProvider
from .memory_storage import MemoryStorage
from .queries import FromPlan, WherePlan, ListPlan, SelectPlan
from .standalone_file_storage import StandaloneFileStorage
# LINQ-like API (Phase 1)
from .linq import Queryable, Policy, Grouping, F, from_collection
from .cloud_file_storage import CloudFileStorage, CloudBlockProvider, S3Client
from .cluster_file_storage import ClusterFileStorage
from .standalone_file_storage import StandaloneFileStorage
