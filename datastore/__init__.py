"""
Datastore is a generic layer of abstraction for data store and database access.
It is a **simple** API with the aim to enable application development in a
datastore-agnostic way, allowing datastores to be swapped seamlessly without
changing application code. Thus, one can leverage different datastores with
different strengths without committing the application to one datastore
throughout its lifetime.
"""

__version__ = "0.3.6"
__author__ = "Juan Batiz-Benet, Alexander Schlarb"
__email__ = "juan@benet.ai, alexander@ninetailed.ninja"
__all__ = (
	"Key", "Namespace",
	"BinaryNullDatastore", "BinaryDictDatastore",
	"ObjectNullDatastore", "ObjectDictDatastore",
	"Query", "Cursor",
	"SerializerAdapter",
	
	"datastore_abc", "datastore_typing", "util"
)


# import core.key
from .core.key import Key
from .core.key import Namespace

# import core.binarystore, core.objectstore
from .core.binarystore import NullDatastore as BinaryNullDatastore
from .core.binarystore import DictDatastore as BinaryDictDatastore

from .core.objectstore import NullDatastore as ObjectNullDatastore
from .core.objectstore import DictDatastore as ObjectDictDatastore

# import core.query
from .core.query import Query
from .core.query import Cursor

# import core.serialize
from .core.serialize import SerializerAdapter


### Exposed submodules ###
from . import datastore_abc
from . import datastore_typing
from . import util
