import typing

import datastore

from . import _support
from ._support import DS, RT, RV, T_co

__all__ = (
	"BinaryAdapter",
	"ObjectAdapter",
	
	"BinaryLowercaseKeyAdapter",
	"ObjectLowercaseKeyAdapter",
	
	"BinaryNamespaceAdapter",
	"ObjectNamespaceAdapter",
	
	"BinaryNestedPathAdapter",
	"ObjectNestedPathAdapter",
)



KEY_TRANSFORM_T = typing.Callable[[datastore.Key], datastore.Key]


class _Adapter(typing.Generic[DS, RT, RV]):
	"""Represents a simple DatastoreAdapter that applies a transform on all incoming
	   keys. For example:

		>>> import datastore.core
		>>> def transform(key):
		...   return key.reverse
		...
		>>> ds = datastore.DictDatastore()
		>>> kt = datastore.KeyTransformDatastore(ds, keytransform=transform)
		None
		>>> ds.put(datastore.Key('/a/b/c'), 'abc')
		>>> ds.get(datastore.Key('/a/b/c'))
		'abc'
		>>> kt.get(datastore.Key('/a/b/c'))
		None
		>>> kt.get(datastore.Key('/c/b/a'))
		'abc'
		>>> ds.get(datastore.Key('/c/b/a'))
		None
	"""
	
	FORWARD_CONTAINS = True
	FORWARD_GET_ALL  = True
	
	key_transform_fn: _support.FunctionProperty[KEY_TRANSFORM_T]
	
	
	def __init__(self, *args, key_transform: KEY_TRANSFORM_T = (lambda k: k), **kwargs):
		"""Initializes KeyTransformDatastore with `keytransform` function."""
		self.key_transform_fn = key_transform
		super().__init__(*args, **kwargs)  # type: ignore[call-arg] # noqa: F821
	
	
	async def get(self, key: datastore.Key) -> RT:
		"""Return the object named by keytransform(key)."""
		return await super().get(self.key_transform_fn(key))  # type: ignore[misc] # noqa: F821
	
	
	async def get_all(self, key: datastore.Key) -> RV:
		"""Return the object named by keytransform(key)."""
		return await super().get_all(self.key_transform_fn(key))  # type: ignore[misc] # noqa: F821
	
	
	async def _put(self, key: datastore.Key, value: RT) -> None:
		"""Stores the object names by keytransform(key)."""
		await super()._put(self.key_transform_fn(key), value)  # type: ignore[misc] # noqa: F821
	
	
	async def delete(self, key: datastore.Key) -> None:
		"""Removes the object named by keytransform(key)."""
		await super().delete(self.key_transform_fn(key))  # type: ignore[misc] # noqa: F821
	
	
	async def contains(self, key: datastore.Key) -> bool:
		"""Returns whether the object named by key is in this datastore."""
		return await super().contains(self.key_transform_fn(key))  # type: ignore[misc] # noqa: F821
	
	
	async def query(self, query: datastore.Query) -> datastore.Cursor:
		"""Returns a sequence of objects matching criteria expressed in `query`"""
		query = query.copy()
		query.key = self.key_transform_fn(query.key)
		return await super().query(query)  # type: ignore[misc] # noqa: F821


class BinaryAdapter(
		_Adapter[datastore.abc.BinaryDatastore, datastore.abc.ReceiveStream, bytes],
		datastore.abc.BinaryAdapter
):
	...


class ObjectAdapter(
		typing.Generic[T_co],
		_Adapter[
			datastore.abc.ObjectDatastore[T_co],
			datastore.abc.ReceiveChannel[T_co],
			typing.List[T_co]
		],
		datastore.abc.ObjectAdapter[T_co, T_co]
):
	...



class _LowercaseKeyAdapter(_Adapter[DS, RT, RV], typing.Generic[DS, RT, RV]):
	"""Represents a simple DatastoreAdapter that lowercases all incoming keys.
	
	For example:
	
		>>> import datastore.core
		>>> ds = datastore.DictDatastore()
		>>> ds.put(datastore.Key('hello'), 'world')
		>>> ds.put(datastore.Key('HELLO'), 'WORLD')
		>>> ds.get(datastore.Key('hello'))
		'world'
		>>> ds.get(datastore.Key('HELLO'))
		'WORLD'
		>>> ds.get(datastore.Key('HeLlO'))
		None
		>>> lds = datastore.LowercaseKeyDatastore(ds)
		>>> lds.get(datastore.Key('HeLlO'))
		'world'
		>>> lds.get(datastore.Key('HeLlO'))
		'world'
		>>> lds.get(datastore.Key('HeLlO'))
		'world'
	"""
	
	def __init__(self, *args, **kwargs):
		"""Initializes KeyTransformDatastore with `key_transform` function."""
		super().__init__(*args, key_transform=self.lowercase_key, **kwargs)
	
	@classmethod
	def lowercase_key(cls, key: datastore.Key) -> datastore.Key:
		"""Returns a lowercased `key`."""
		return datastore.Key(str(key).lower())


class BinaryLowercaseKeyAdapter(
		_LowercaseKeyAdapter[datastore.abc.BinaryDatastore, datastore.abc.ReceiveStream, bytes],
		datastore.abc.BinaryAdapter
):
	...


class ObjectLowercaseKeyAdapter(
		typing.Generic[T_co],
		_LowercaseKeyAdapter[
			datastore.abc.ObjectDatastore[T_co],
			datastore.abc.ReceiveChannel[T_co],
			typing.List[T_co]
		],
		datastore.abc.ObjectAdapter[T_co, T_co]
):
	...



class _NamespaceAdapter(_Adapter[DS, RT, RV], typing.Generic[DS, RT, RV]):
	"""Represents a simple DatastoreAdapter that namespaces all incoming keys.
	   For example:

		>>> import datastore.core
		>>>
		>>> ds = datastore.DictDatastore()
		>>> ds.put(datastore.Key('/a/b'), 'ab')
		>>> ds.put(datastore.Key('/c/d'), 'cd')
		>>> ds.put(datastore.Key('/a/b/c/d'), 'abcd')
		>>>
		>>> nd = datastore.NamespaceDatastore('/a/b', ds)
		>>> nd.get(datastore.Key('/a/b'))
		None
		>>> nd.get(datastore.Key('/c/d'))
		'abcd'
		>>> nd.get(datastore.Key('/a/b/c/d'))
		None
		>>> nd.put(datastore.Key('/c/d'), 'cd')
		>>> ds.get(datastore.Key('/a/b/c/d'))
		'cd'
	"""
	
	namespace: datastore.Key

	def __init__(self, namespace: typing.Union[str, datastore.Key], *args, **kwargs):
		"""Initializes NamespaceDatastore with `key` namespace."""
		self.namespace = datastore.Key(namespace)
		super().__init__(*args, key_transform = self.namespace_key, **kwargs)
	
	def namespace_key(self, key: datastore.Key) -> datastore.Key:
		"""Returns a namespaced `key`: namespace.child(key)."""
		return self.namespace.child(key)


class BinaryNamespaceAdapter(
		_NamespaceAdapter[datastore.abc.BinaryDatastore, datastore.abc.ReceiveStream, bytes],
		datastore.abc.BinaryAdapter
):
	...


class ObjectNamespaceAdapter(
		typing.Generic[T_co],
		_NamespaceAdapter[
			datastore.abc.ObjectDatastore[T_co],
			datastore.abc.ReceiveChannel[T_co],
			typing.List[T_co]
		],
		datastore.abc.ObjectAdapter[T_co, T_co]
):
	...



class _NestedPathAdapter(_Adapter[DS, RT, RV], typing.Generic[DS, RT, RV]):
	"""Represents a simple DatastoreAdapter that shards/namespaces incoming keys.

	Incoming keys are sharded into nested namespaces. The idea is to use the key
	name to separate into nested namespaces. This is akin to the directory
	structure that ``git`` uses for objects. For example:

		>>> import datastore.core
		>>>
		>>> ds = datastore.DictDatastore()
		>>> np = datastore.NestedPathDatastore(ds, depth=3, length=2)
		>>>
		>>> np.put(datastore.Key('/abcdefghijk'), 1)
		>>> np.get(datastore.Key('/abcdefghijk'))
		1
		>>> ds.get(datastore.Key('/abcdefghijk'))
		None
		>>> ds.get(datastore.Key('/ab/cd/ef/abcdefghijk'))
		1
		>>> np.put(datastore.Key('abc'), 2)
		>>> np.get(datastore.Key('abc'))
		2
		>>> ds.get(datastore.Key('/ab/ca/bc/abc'))
		2
	"""

	_default_depth:  int = 3
	_default_length: int = 2
	
	@staticmethod
	def _default_keyfn(key: datastore.Key) -> str:
		return key.name
	
	
	def __init__(self, *args,
	             depth: int = None,
	             length: int = None,
	             key_fn: typing.Callable[[datastore.Key], str] = None,
	             **kwargs):
		"""Initializes KeyTransformDatastore with keytransform function.

		Arguments
		---------
		depth
			The nesting level depth (e.g. 3 => /1/2/3/123); default: 3
		length:
			The nesting level length (e.g. 2 => /12/123456); default: 2
		keyfn:
			A function that maps key paths to the name they should be stored as
		"""

		# assign the nesting variables
		self.nest_depth  = depth if depth is not None else self._default_depth
		self.nest_length = length if length is not None else self._default_length
		self.nest_keyfn  = key_fn if key_fn is not None else self._default_keyfn

		super().__init__(*args, key_transform=self.nest_key, **kwargs)
	
	
	async def query(self, query: datastore.Query) -> datastore.Cursor:
		# Requires supporting * operator on queries.
		raise NotImplementedError()
	
	
	def nest_key(self, key: datastore.Key) -> datastore.Key:
		"""Returns a nested `key`."""
		
		nest = self.nest_keyfn(key)
		
		# if depth * length > len(key.name), we need to pad.
		mult = 1 + int(self.nest_depth * self.nest_length / len(nest))
		nest = nest * mult
		
		pref = datastore.Key(self.nested_path(nest, self.nest_depth, self.nest_length))
		return pref.child(key)
	
	
	@staticmethod
	def nested_path(path: str, depth: int, length: int) -> str:
		"""Returns a nested version of `basename`, using the starting characters.
		
		For example:
		
			>>> NestedPathDatastore.nested_path('abcdefghijk', 3, 2)
			'ab/cd/ef'
			>>> NestedPathDatastore.nested_path('abcdefghijk', 4, 2)
			'ab/cd/ef/gh'
			>>> NestedPathDatastore.nested_path('abcdefghijk', 3, 4)
			'abcd/efgh/ijk'
			>>> NestedPathDatastore.nested_path('abcdefghijk', 1, 4)
			'abcd'
			>>> NestedPathDatastore.nested_path('abcdefghijk', 3, 10)
			'abcdefghij/k'
		"""
		components = [path[n:n + length] for n in range(0, len(path), length)]
		components = components[:depth]
		return '/'.join(components)


class BinaryNestedPathAdapter(
		_NestedPathAdapter[datastore.abc.BinaryDatastore, datastore.abc.ReceiveStream, bytes],
		datastore.abc.BinaryAdapter
):
	...


class ObjectNestedPathAdapter(
		typing.Generic[T_co],
		_NestedPathAdapter[
			datastore.abc.ObjectDatastore[T_co],
			datastore.abc.ReceiveChannel[T_co],
			typing.List[T_co]
		],
		datastore.abc.ObjectAdapter[T_co, T_co]
):
	...
