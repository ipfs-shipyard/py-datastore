"""
datastore is a generic layer of abstraction for data store and database access.
It is a **simple** API with the aim to enable application development in a
datastore-agnostic way, allowing datastores to be swapped seamlessly without
changing application code. Thus, one can leverage different datastores with
different strengths without committing the application to one datastore
throughout its lifetime.
"""

__version__ = '0.3.6'
__author__ = 'Juan Batiz-Benet, Alexander Schlarb'
__email__ = 'juan@benet.ai, alexander@ninetailed.ninja'

# import key
from .key import Key
from .key import Namespace

# import binarystore, objectstore
from .binarystore import NullDatastore as BinaryNullDatastore
from .binarystore import DictDatastore as BinaryDictDatastore

from .objectstore import NullDatastore as ObjectNullDatastore
from .objectstore import DictDatastore as ObjectDictDatastore

from .binarystore import DatastoreAdapter as BinaryDatastoreAdapter
from .objectstore import DatastoreAdapter as ObjectatastoreAdapter

#from .basic import LoggingAdapter
#from .basic import KeyTransformAdapter
#from .basic import LowercaseKeyAdapter
#from .basic import NamespaceAdapter
#from .basic import NestedPathAdapter
#from .basic import DirectoryTreeAdaoter
#from .basic import DatastoreDirectoryMixin
#
#from .basic import DatastoreCollectionMixin
#from .basic import ShardedAdapter
#from .basic import TieredAdapter

# import query
from .query import Query
from .query import Cursor

# import serialize
from .serialize import SerializerAdapter
