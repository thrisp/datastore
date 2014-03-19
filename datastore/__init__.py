'''
__doc__ =
datastore is a generic layer of abstraction for data store and database access.
It is a **simple** API with the aim to enable application development in a
datastore-agnostic way, allowing datastores to be swapped seamlessly without
changing application code. Thus, one can leverage different datastores with
different strengths without committing the application to one datastore
throughout its lifetime.
'''


from .key import Key, Namespace
from .basic import (Datastore, NullDatastore, DictDatastore,
        InterfaceMappingDatastore, ShimDatastore, CacheShimDatastore,
        LoggingDatastore, KeyTransformDatastore, LowercaseKeyDatastore,
        NamespaceDatastore, NestedPathDatastore, SymlinkDatastore,
        DirectoryTreeDatastore, DirectoryDatastore, DatastoreCollection,
        ShardedDatastore, TieredDatastore)
from .query import Query, Cursor
from .serialize import SerializerShimDatastore

import pkg_resources
pkg_resources.declare_namespace(__name__)
