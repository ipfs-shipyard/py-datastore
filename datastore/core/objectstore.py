import abc
import typing

import trio

from . import key as key_
from . import query as query_
class util:  # noqa
	from .util import stream



class Datastore:
	"""A Datastore represents storage for any string key to an arbitrary Python object pair.

	Datastores are general enough to be backed by all kinds of different storage:
	in-memory caches, databases, a remote datastore, flat files on disk, etc.
	
	The general idea is to wrap a more complicated storage facility in a simple,
	uniform interface, keeping the freedom of using the right tools for the job.
	In particular, a Datastore can aggregate other datastores in interesting ways,
	like sharded (to distribute load) or tiered access (caches before databases).
	
	While Datastores should be written general enough to accept all sorts of
	values, some implementations will undoubtedly have to be specific (e.g. SQL
	databases where fields should be decomposed into columns), particularly to
	support queries efficiently.
	"""

	# Main API. Datastore implementations MUST implement these methods.
	
	
	@abc.abstractmethod
	async def get(self, key: key_.Key) -> trio.abc.ReceiveChannel:
		"""Returns the objects named by `key` or raises `KeyError` otherwise
		
		If this datastore does not support chunking all data will be returned
		in a tuple object with exactly one value.
		
		Important
		---------
		You **must** exhaust or manually close the returned iterable to ensure
		that possibly associated resources, like open file descriptors, are
		free'd.

		Arguments
		---------
		key
			Key naming the object stream to retrieve
		
		Raises
		------
		KeyError
			The given object was not present in this datastore
		RuntimeError
			An internal error occurred
		"""
		pass


	async def put(self, key: key_.Key, value: util.stream.ArbitraryReceiveChannel) -> None:
		"""Stores or replaces the object named by `key` with `value`
		
		Arguments
		---------
		key
			Key naming the object to store
		value
			A synchronous or asynchronous bytes or iteratable of bytes object
			yielding the data to store
		
		Raises
		------
		RuntimeError
			An internal error occurred
		"""
		await self._put(key, util.stream.WrapingReceiveChannel(value))
	

	@abc.abstractmethod
	async def _put(self, key: key_.Key, value: util.stream.ReceiveChannel) -> None:
		"""Like :meth:`put`, but always receives a `datastore.util.ReceiveCahnnel`
		   compatible object, so that your datastore implementation doesn't
		   have to do any conversion
		"""
		pass
	
	
	@abc.abstractmethod
	async def delete(self, key: key_.Key) -> None:
		"""Removes the object named by `key`
		
		Arguments
		---------
		key
			Key naming the object to remove
		
		Raises
		------
		KeyError
			The given object was not present in this datastore
		RuntimeError
			An internal error occurred
		"""
		pass
	
	
	@abc.abstractmethod
	async def query(self, query: query_.Query) -> query_.Cursor:
		"""Returns an iterable of objects matching criteria expressed in `query`
		
		Implementations of query will be the largest differentiating factor
		amongst datastores. All datastores **must** implement query, even using
		query's worst case scenario, see :ref:class:`Query` for details.
		
		Arguments
		---------
		query
			Object describing which objects to match and return
		
		Raises
		------
		RuntimeError
			An internal error occurred
		"""
		pass
	
	
	# Secondary API. Datastores MAY provide optimized implementations.
	
	
	async def contains(self, key: key_.Key) -> bool:
		"""Returns whether an object named by `key` exists
		
		The default implementation pays the cost of a get. Some datastore
		implementations may optimize this.
		
		Arguments
		---------
		key
			Key naming the object to check.
		"""
		try:
			await (await self.get(key)).aclose()
			return True
		except KeyError:
			return False



class NullDatastore(Datastore):
	"""Stores nothing, but conforms to the API. Useful to test with."""

	async def get(self, key: key_.Key) -> util.stream.ReceiveChannel:
		"""Unconditionally raise `KeyError`"""
		raise KeyError(key)

	async def _put(self, key: key_.Key, value: util.stream.ReceiveChannel) -> None:
		"""Do nothing with `key` and ignore the `value`"""
		pass

	async def delete(self, key: key_.Key) -> None:
		"""Pretend there is any object that could be removed by the name `key`"""
		pass

	async def query(self, query: query_.Query) -> query_.Cursor:
		"""This won't ever match anything"""
		return query([])



class DictDatastore(Datastore):
	"""Simple straw-man in-memory datastore backed by nested dicts."""

	_items: typing.Dict[str, typing.Dict[key_.Key, object]]
	
	def __init__(self):
		self._items = dict()
	
	
	def _collection(self, key: key_.Key) -> typing.Dict[key_.Key, object]:
		"""Returns the namespace collection for `key`."""
		collection = str(key.path)
		if collection not in self._items:
			self._items[collection] = dict()
		return self._items[collection]
	
	
	async def get(self, key: key_.Key) -> trio.abc.ReceiveChannel:
		"""Returns the object named by `key` or raises `KeyError`.
		
		Retrieves the object from the collection corresponding to ``key.path``.
		
		Arguments
		---------
		key
			Key naming the object to retrieve.
		"""
		return util.stream.WrapingReceiveChannel(self._collection(key)[key])
	
	
	async def _put(self, key: key_.Key, value: util.stream.ReceiveChannel) -> None:
		"""Stores the object `value` named by `key`.
		
		Stores the object in the collection corresponding to ``key.path``.
		
		Arguments
		---------
		key
			Key naming `value`
		value
			The object to store
		"""
		self._collection(key)[key] = await value.collect()
	
	
	async def delete(self, key: key_.Key) -> None:
		"""Removes the object named by `key` or raises `KeyError` if it did not
		   exist.
		
		Removes the object from the collection corresponding to ``key.path``.
		
		Arguments
		---------
		key
			Key naming the object to remove.
		"""
		del self._collection(key)[key]
		
		if len(self._collection(key)) == 0:
			del self._items[str(key.path)]
	
	
	async def contains(self, key: key_.Key) -> bool:
		"""Returns whether the object named by `key` exists.
		
		Checks for the object in the collection corresponding to ``key.path``.
		
		Arguments
		---------
		key
			Key naming the object to check.
		"""
		return key in self._collection(key)
	
	
	async def query(self, query: query_.Query) -> query_.Cursor:
		"""Returns an iterable of objects matching criteria expressed in `query`

		Naively applies the query operations on the objects within the namespaced
		collection corresponding to ``query.key.path``.

		Arguments
		---------
		query
			Query object describing the objects to return.
		"""
		# entire dataset already in memory, so ok to apply query naively
		if str(query.key) in self._items:
			return query(self._items[str(query.key)].values())
		else:
			return query([])
	
	
	def __len__(self) -> int:
		return sum(map(len, self._items.values()))



class Adapter(Datastore):
	"""Represents a non-concrete datastore that adds functionality between the
	   client and a lower-level datastore.
	
	Shim datastores do not actually store
	data themselves; instead, they delegate storage to an underlying child
	datastore. The default implementation just passes all calls to the child.
	"""
	child_datastore: Datastore
	
	
	def __init__(self, datastore: Datastore):
		"""Initializes this DatastoreAdapter with child `datastore`."""
		self.child_datastore = datastore
	
	
	# default implementation just passes all calls to child
	async def get(self, key: key_.Key) -> util.stream.ReceiveChannel:
		"""Returns the object named by `key` or raises `KeyError` if it does
		   not exist.

		Default shim implementation simply returns ``child_datastore.get(key)``
		Override to provide different functionality, for example::

			def get(self, key):
			  value = self.child_datastore.get(key)
			  return json.loads(value)

		Arguments
		---------
		key
			Key naming the object to retrieve
		"""
		return await self.child_datastore.get(key)
	
	
	async def _put(self, key: key_.Key, value: util.stream.ReceiveChannel) -> None:
		"""Stores the object `value` named by `key`.
		
		Default shim implementation simply calls ``child_datastore.put(key, value)``
		Override to provide different functionality, for example::
		
			def put(self, key, value):
			  value = json.dumps(value)
			  self.child_datastore.put(key, value)
		
		Arguments
		---------
		key
			Key naming `value`.
		value
			The object to store.
		"""
		await self.child_datastore.put(key, value)
	
	
	async def delete(self, key: key_.Key) -> None:
		"""Removes the object named by `key`.

		Default shim implementation simply calls ``child_datastore.delete(key)``
		Override to provide different functionality.

		Args:
		  key: Key naming the object to remove.
		"""
		await self.child_datastore.delete(key)
	
	
	async def query(self, query: query_.Query) -> query_.Cursor:
		"""Returns an iterable of objects matching criteria expressed in `query`.
		
		Default shim implementation simply returns ``child_datastore.query(query)``
		Override to provide different functionality, for example::
		
			def query(self, query):
			  cursor = self.child_datastore.query(query)
			  cursor._iterable = deserialized(cursor._iterable)
			  return cursor
		
		Arguments
		---------
		query
			Query object describing the objects to return.
		"""
		return await self.child_datastore.query(query)
	
	
	async def contains(self, key: key_.Key) -> bool:
		"""Returns whether any object named by `key` exists
		
		Arguments
		---------
		key
			Key naming the object to check.
		"""
		return await self.child_datastore.contains(key)



class LoggingAdapter(Adapter):
	"""Wraps a datastore with a logging shim."""
	
	def __init__(self, child_datastore, logger=None):
		if not logger:
			import logging
			logger = logging
		
		self.logger = logger
		
		super().__init__(child_datastore)
	
	
	async def get(self, key: key_.Key) -> util.stream.ReceiveChannel:
		"""Return the object named by key or None if it does not exist.
		   LoggingDatastore logs the access.
		"""
		self.logger.info('%s: get %s' % (self, key))
		value = await super().get(key)
		self.logger.debug('%s: %s' % (self, value))
		return value
	
	
	async def _put(self, key: key_.Key, value: util.stream.ReceiveChannel) -> None:
		"""Stores the object `value` named by `key`self.
		   LoggingDatastore logs the access.
		"""
		self.logger.info('%s: put %s' % (self, key))
		self.logger.debug('%s: %s' % (self, value))
		await super()._put(key, value)
	
	
	async def delete(self, key: key_.Key) -> None:
		"""Removes the object named by `key`.
		   LoggingDatastore logs the access.
		"""
		self.logger.info('%s: delete %s' % (self, key))
		await super().delete(key)
	
	
	async def contains(self, key: key_.Key) -> bool:
		"""Returns whether the object named by `key` exists.
		   LoggingDatastore logs the access.
		"""
		self.logger.info('%s: contains %s' % (self, key))
		return await super().contains(key)
	
	
	async def query(self, query: query_.Query) -> query_.Cursor:
		"""Returns an iterable of objects matching criteria expressed in `query`.
		   LoggingDatastore logs the access.
		"""
		self.logger.info('%s: query %s' % (self, query))
		return await super().query(query)



class KeyTransformAdapter(Adapter):
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
	
	def __init__(self, *args, keytransform=(lambda k: k), **kwargs):
		"""Initializes KeyTransformDatastore with `keytransform` function."""
		self.keytransform = keytransform
		super().__init__(*args, **kwargs)
	
	
	async def get(self, key: key_.Key) -> util.stream.ReceiveChannel:
		"""Return the object named by keytransform(key)."""
		return await super().get(self.keytransform(key))
	
	
	async def _put(self, key: key_.Key, value: util.stream.ReceiveChannel) -> None:
		"""Stores the object names by keytransform(key)."""
		await super().put(self.keytransform(key), value)
	
	
	async def delete(self, key: key_.Key) -> None:
		"""Removes the object named by keytransform(key)."""
		await super().delete(self.keytransform(key))
	
	
	async def contains(self, key: key_.Key) -> bool:
		"""Returns whether the object named by key is in this datastore."""
		return await super().contains(self.keytransform(key))
	
	
	async def query(self, query: query_.Query) -> query_.Cursor:
		"""Returns a sequence of objects matching criteria expressed in `query`"""
		query = query.copy()
		query.key = self.keytransform(query.key)
		return await super().query(query)


class LowercaseKeyAdapter(KeyTransformAdapter):
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
		"""Initializes KeyTransformDatastore with keytransform function."""
		super().__init__(*args, keytransform=self.lowercase_key, **kwargs)
	
	
	@classmethod
	def lowercase_key(cls, key: key_.Key) -> key_.Key:
		"""Returns a lowercased `key`."""
		return key_.Key(str(key).lower())



class NamespaceAdapter(KeyTransformAdapter):
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

	def __init__(self, namespace, *args, **kwargs):
		"""Initializes NamespaceDatastore with `key` namespace."""
		self.namespace = key_.Key(namespace)
		super().__init__(*args, keytransform = self.namespace_key, **kwargs)
	
	
	def namespace_key(self, key: key_.Key) -> key_.Key:
		"""Returns a namespaced `key`: namespace.child(key)."""
		return self.namespace.child(key)



class NestedPathAdapter(KeyTransformAdapter):
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
	_default_keyfn:  typing.Callable[[key_.Key], str] = staticmethod(lambda key: key.name)

	def __init__(self, *args,
	             depth: int = None,
	             length: int = None,
	             keyfn: typing.Callable[[key_.Key], str] = None,
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
		self.nest_depth  = depth  if depth  is not None else self._default_depth
		self.nest_length = length if length is not None else self._default_length
		self.nest_keyfn  = keyfn  if keyfn  is not None else self._default_keyfn

		super().__init__(*args, keytransform=self.nest_key, **kwargs)
	
	
	async def query(self, query: query_.Query) -> query_.Cursor:
		# Requires supporting * operator on queries.
		raise NotImplementedError()
	
	
	def nest_key(self, key: key_.Key) -> key_.Key:
		"""Returns a nested `key`."""
		
		nest = self.nest_keyfn(key)
		
		# if depth * length > len(key.name), we need to pad.
		mult = 1 + int(self.nest_depth * self.nest_length / len(nest))
		nest = nest * mult
		
		pref = key_.Key(self.nested_path(nest, self.nest_depth, self.nest_length))
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



class DatastoreDirectoryMixin:
	"""Datastore that allows manual tracking of directory entries.
	
	For example:
	
		>>> ds = DirectoryDatastore(ds)
		>>>
		>>> # initialize directory at /foo
		>>> ds.directory(Key('/foo'))
		>>>
		>>> # adding directory entries
		>>> ds.directoryAdd(Key('/foo'), Key('/foo/bar'))
		>>> ds.directoryAdd(Key('/foo'), Key('/foo/baz'))
		>>>
		>>> # value is a generator returning all the keys in this dir
		>>> for key in ds.directoryRead(Key('/foo')):
		...   print key
		Key('/foo/bar')
		Key('/foo/baz')
		>>>
		>>> # querying for a collection works
		>>> for item in ds.query(Query(Key('/foo'))):
		...  print item
		'bar'
		'baz'
	"""

	async def directory(self, dir_key: key_.Key, exist_ok: bool = False) -> bool:
		"""Initializes directory at dir_key.
		
		Returns a boolean of whether a new directory was actually created or
		not."""
		try:
			await (await self.get(dir_key)).aclose()
			if not exist_ok:
				raise KeyError(str(dir_key))
			return False
		except KeyError:
			await self._put(dir_key, util.stream.ReceiveChannel([]))  #XXX: Or something like this…
			return True

	async def directory_read(self, dir_key: key_.Key) -> typing.AsyncIterable[key_.Key]:
		"""Returns a generator that iterates over all keys in the directory
		referenced by `dir_key`

		Raises `KeyError` if the directory `dir_key` does not exist
		"""
		async with await self.get(dir_key) as dir_items_iter:
			async for dir_item in dir_items_iter:
				yield key_.Key(dir_item)
	
	
	async def directory_add(self, dir_key: key_.Key, key: key_.Key,
	                        create: bool = False) -> None:
		"""Adds directory entry `key` to directory at `dir_key`.

		If the directory `dir_key` does not exist, `KeyError` will be raised
		unless `create` is True in which case the directory will be created
		instead.
		"""
		key_str = str(key)
		
		dir_items: typing.List[str] = []
		try:
			dir_items = [item async for item in await self.get(dir_key)]
		except KeyError:
			if not create:
				raise
		
		if key_str not in dir_items:
			dir_items.append(key_str)
			await self._put(dir_key, util.stream.WrapingReceiveChannel(dir_items))
	
	
	async def directory_remove(self, dir_key: key_.Key, key: key_.Key,
	                           missing_ok: bool = False) -> None:
		"""Removes directory entry `key` from directory at `dir_key`.

		If either the directory `dir_key` or the directory entry `key` don't
		exist, `KeyError` will be raised unless `missing_ok` is set to `True`.
		"""
		key_str = str(key)
		
		try:
			dir_items = [item async for item in await self.get(dir_key)]
		except KeyError:
			if not missing_ok:
				raise
			return
		
		if key_str in dir_items:
			dir_items = [k for k in dir_items if k != key]
			self._put(dir_key, util.stream.WrapingReceiveChannel(dir_items))
		elif not missing_ok:
			raise KeyError(f"{key} in {dir_key}")



class DirectoryTreeAdapter(DatastoreDirectoryMixin, Adapter):
	"""Datastore that tracks directory entries, like in a filesystem.
	All key changes cause changes in a collection-like directory.

	For example:

		>>> import datastore.core
		>>>
		>>> dds = datastore.DictDatastore()
		>>> rds = datastore.DirectoryTreeDatastore(dds)
		>>>
		>>> a = datastore.Key('/A')
		>>> b = datastore.Key('/A/B')
		>>> c = datastore.Key('/A/C')
		>>>
		>>> rds.get(a)
		[]
		>>> rds.put(b, 1)
		>>> rds.get(b)
		1
		>>> rds.get(a)
		['/A/B']
		>>> rds.put(c, 1)
		>>> rds.get(c)
		1
		>>> rds.get(a)
		['/A/B', '/A/C']
		>>> rds.delete(b)
		>>> rds.get(a)
		['/A/C']
		>>> rds.delete(c)
		>>> rds.get(a)
		[]
	"""
	
	
	async def _put(self, key: key_.Key, value: util.stream.ReceiveChannel) -> None:
		"""Stores the object `value` named by `key`.
		   DirectoryTreeDatastore stores a directory entry.
		"""
		await super()._put(key, value)

		# ignore root
		if key.is_top_level():
			return

		# Add entry to directory
		dir_key = key.parent.instance('directory')
		await super().directory_add(dir_key, key, create=True)
	
	
	async def delete(self, key: key_.Key) -> None:
		"""Removes the object named by `key`.
		   DirectoryTreeDatastore removes the directory entry.
		"""
		await super().delete(key)
		
		dir_key = key.parent.instance('directory')
		await super().directory_remove(dir_key, key, missing_ok=True)
	
	
	async def query(self, query: query_.Query) -> query_.Cursor:
		"""Returns objects matching criteria expressed in `query`.
		DirectoryTreeDatastore uses directory entries.
		"""
		return query(self.directory_read(query.key))



class DatastoreCollectionMixin:
	"""Represents a collection of datastores."""

	def __init__(self, stores: typing.Collection[Datastore] = []):
		"""Initialize the datastore with any provided datastores."""
		if not isinstance(stores, list):
			stores = list(stores)
		self._stores = stores

	def get_datastore_at(self, index: int) -> Datastore:
		"""Returns the datastore at `index`."""
		return self._stores[index]

	def append_datastore(self, store: Datastore) -> None:
		"""Appends datastore `store` to this collection."""
		self._stores.append(store)

	def remove_datastore(self, store: Datastore) -> None:
		"""Removes datastore `store` from this collection."""
		self._stores.remove(store)

	def insert_datastore(self, index: int, store: Datastore) -> None:
		"""Inserts datastore `store` into this collection at `index`."""
		self._stores.insert(index, store)


class TieredAdapter(Adapter, DatastoreCollectionMixin):
	"""Represents a hierarchical collection of datastores.

	Each datastore is queried in order. This is helpful to organize access
	order in terms of speed (i.e. read caches first).

	Datastores should be arranged in order of completeness, with the most complete
	datastore last, as it will handle query calls.

	Semantics:
	
		* get      : returns first found value
		* put      : writes through to all
		* delete   : deletes through to all
		* contains : returns first found value
		* query    : queries bottom (most complete) datastore
	"""

	async def get(self, key: key_.Key) -> util.stream.ReceiveChannel:
		"""Return the object named by key. Checks each datastore in order."""
		found = False
		value = None
		exceptions = []
		for store in self._stores:
			try:
				value = await store.get(key)
				found = True
			except KeyError as exc:
				exceptions.append(exc)
			else:
				break
		if not found:
			raise trio.MultiError(exceptions)
		
		# Add model to lower stores only
		async with trio.open_nursery() as nursery:
			for store2 in self._stores:
				if store == store2:
					break
				nursery.call_soon(store2.put(key, value))
		
		return value
	
	
	async def _put(self, key: key_.Key, value: util.stream.ReceiveChannel) -> None:
		"""Stores the object in all underlying datastores."""
		async with trio.open_nursery() as nursery:
			for store in self._stores:
				nursery.call_soon(store.put(key, value))
	
	
	async def delete(self, key: key_.Key) -> None:
		"""Removes the object from all underlying datastores."""
		async def swallow_key_error(coroutine):
			try:
				await coroutine
			except KeyError:
				pass
		
		async with trio.open_nursery() as nursery:
			for store in self._stores:
				nursery.call_soon(swallow_key_error(store.delete()))
	
	
	async def query(self, query: query_.Query) -> query_.Cursor:
		"""Returns a sequence of objects matching criteria expressed in `query`.
		The last datastore will handle all query calls, as it has a (if not
		the only) complete record of all objects.
		"""
		# queries hit the last (most complete) datastore
		return self._stores[-1].query(query)
	
	
	async def contains(self, key: key_.Key) -> bool:
		"""Returns whether the object is in this datastore."""
		for store in self._stores:
			if await store.contains(key):
				return True
		return False



class ShardedAdapter(Adapter, DatastoreCollectionMixin):
	"""Represents a collection of datastore shards.
	
	A datastore is selected based on a sharding function.
	Sharding functions should take a Key and return an integer.
	
	Caution
	-------
	Adding or removing datastores while mid-use may severely affect consistency.
	Also ensure the order is correct upon initialization. While this is not as
	important for caches, it is crucial for persistent datastores.
	"""
	
	def __init__(self, stores: typing.Collection[Datastore] = [],
	             shardingfn: typing.Callable[[key_.Key], int] = hash):
		"""Initialize the datastore with any provided datastore."""
		DatastoreCollectionMixin.__init__(self, stores)
		self._shardingfn = shardingfn
	
	
	def shard(self, key: key_.Key):
		"""Returns the shard index to handle `key`, according to sharding fn."""
		return self._shardingfn(key) % len(self._stores)
	
	
	def get_sharded_datastore(self, key: key_.Key):
		"""Returns the shard to handle `key`."""
		return self.get_datastore_at(self.shard(key))
	
	
	async def get(self, key: key_.Key) -> util.stream.ReceiveChannel:
		"""Return the object named by key from the corresponding datastore."""
		return await self.get_sharded_datastore(key).get(key)
	
	
	async def _put(self, key: key_.Key, value: util.stream.ReceiveChannel) -> None:
		"""Stores the object to the corresponding datastore."""
		await self.get_sharded_datastore(key).put(key, value)
	
	
	async def delete(self, key: key_.Key) -> None:
		"""Removes the object from the corresponding datastore."""
		await self.get_sharded_datastore(key).delete(key)
	
	
	async def contains(self, key: key_.Key) -> bool:
		"""Returns whether the object is in this datastore."""
		return await self.get_sharded_datastore(key).contains(key)
	
	
	async def query(self, query: query_.Query) -> query_.Cursor:
		"""Returns a sequence of objects matching criteria expressed in `query`"""
		cursor = query_.Cursor(query, self._shard_query_generator(query))
		cursor.apply_order()  # ordering sharded queries is expensive (no generator)
		return cursor
	
	
	def _shard_query_generator(self, query):
		"""A generator that queries each shard in sequence."""
		shard_query = query.copy()
		
		for shard in self._stores:
			# yield all items matching within this shard
			cursor = shard.query(shard_query)
			for item in cursor:
				yield item
			
			# update query with results of first query
			shard_query.offset = max(shard_query.offset - cursor.skipped, 0)
			if shard_query.limit:
				shard_query.limit = max(shard_query.limit - cursor.returned, 0)
				
				if shard_query.limit <= 0:
					break  # we're already done!


"""

Hello Tiered Access

	>>> import pymongo
	>>> import datastore.core
	>>>
	>>> from datastore.impl.mongo import MongoDatastore
	>>> from datastore.impl.lrucache import LRUCache
	>>> from datastore.impl.filesystem import FileSystemDatastore
	>>>
	>>> conn = pymongo.Connection()
	>>> mongo = MongoDatastore(conn.test_db)
	>>>
	>>> cache = LRUCache(1000)
	>>> fs = FileSystemDatastore('/tmp/.test_db')
	>>>
	>>> ds = datastore.TieredDatastore([cache, mongo, fs])
	>>>
	>>> hello = datastore.Key('hello')
	>>> ds.put(hello, 'world')
	>>> ds.contains(hello)
	True
	>>> ds.get(hello)
	'world'
	>>> ds.delete(hello)
	>>> ds.get(hello)
	None

Hello Sharding

	>>> import datastore.core
	>>>
	>>> shards = [datastore.DictDatastore() for i in range(0, 10)]
	>>>
	>>> ds = datastore.ShardedDatastore(shards)
	>>>
	>>> hello = datastore.Key('hello')
	>>> ds.put(hello, 'world')
	>>> ds.contains(hello)
	True
	>>> ds.get(hello)
	'world'
	>>> ds.delete(hello)
	>>> ds.get(hello)
	None
"""
