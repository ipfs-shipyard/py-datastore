import abc
import collections.abc
import typing
import uuid

import trio

from . import key as key_
from . import query as query_


class util:  # noqa
	from .util import decorator
	from .util import metadata
	from .util import stream


def is_valid_value_type(value: util.stream.ArbitraryReceiveStream) -> bool:
	"""Checks that `value` is of the right type for `Datastore.put`
	
	It's just too easy to accidentally pass in the wrong type without this check.
	Unfortunately this cannot check whether iterators return the correct types,
	so the utility of this function unfortunately is limited to some extent.
	"""
	return isinstance(value, (
		trio.abc.ReceiveStream,
		collections.abc.AsyncIterable,
		collections.abc.Awaitable,
		collections.abc.Iterable,
		bytes
	)) and not isinstance(value, str)



class Datastore(trio.abc.AsyncResource):
	"""A Datastore represents storage for any string key to binary value pair.

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
	
	__slots__ = ()

	# Some possibly useful types (assigned at the end of this file)
	ADAPTER_T:  type
	METADATA_T: type
	RECEIVE_T:  type
	
	_DS = typing.TypeVar("_DS", bound="Datastore")
	
	_PUT_NEW_INDIRECT_RT = typing.Tuple[
		key_.Key,
		typing.Callable[[util.stream.ReceiveStream], typing.Awaitable[None]]
	]
	
	
	@classmethod
	@util.decorator.awaitable_to_context_manager
	async def create(cls: typing.Type[_DS], *args: typing.Any, **kwargs: typing.Any) -> _DS:
		return cls(*args, **kwargs)  # type: ignore[call-arg]
	
	# Main API. Datastore implementations MUST implement these methods.
	
	
	@abc.abstractmethod
	async def get(self, key: key_.Key) -> util.stream.ReceiveStream:
		"""Returns the data named by `key` or raises `KeyError` otherwise
		
		Important
		---------
		You **must** exhaust or manually close the returned iterable to ensure
		that possibly associated resources, like open file descriptors, are
		free'd.

		Arguments
		---------
		key
			Key naming the binary data to retrieve
		
		Raises
		------
		KeyError
			The given object was not present in this datastore
		RuntimeError
			An internal error occurred
		"""
		pass
	
	
	async def put(self, key: key_.Key, value: util.stream.ArbitraryReceiveStream, *,
	              create: bool = True, replace: bool = True, **kwargs: typing.Any) -> None:
		"""Stores or replaces the data in *value* at name *key*
		
		This operation is similar to opening a regular file on a filesystem for
		writing or doing an ``INSERT INTO OR UPDATE`` style SQL operation with a
		primary key (or parts thereof depending on the provided flags).
		
		Arguments
		---------
		key
			Key naming the binary data slot to store at
		value
			A synchronous or asynchronous bytes or iterable of bytes object
		create
			Create the given key if it does not exist?
		replace
			Replace the given key if it does exist?
		
		Raises
		------
		RuntimeError
			An internal error occurred
		RuntimeError
			Arguments *create* and *replace* cannot both be ``False``
		NotImplementedError
			This datastore does not support the argument *create* or *replace* being ``False``,
			but it is not ``True``
		"""
		assert is_valid_value_type(value)
		if not create and not replace:
			raise RuntimeError("Arguments create and replace cannot both be False")
		await self._put(key, util.stream.receive_stream_from(value),
		                create=create, replace=replace, **kwargs)
	
	
	@abc.abstractmethod
	async def _put(self, key: key_.Key, value: util.stream.ReceiveStream, *,
	               create: bool, replace: bool, **kwargs: typing.Any) -> None:
		"""Like :meth:`put`, but always receives a :type:`datastore.abc.ReceiveStream` compatible object
		
		This way your datastore implementation doesn't have to do the
		conversion from the several supported input types supported itself.
		"""
		pass
	
	
	async def put_new(self, prefix: key_.Key, value: util.stream.ArbitraryReceiveStream,
	                  **kwargs: typing.Any) -> key_.Key:
		"""Create a new data key below *prefix* with the given value
		
		This operation is similar to creating a temporary file below some
		directory on a filesystem or doing an ``INSERT INTO`` SQL operation.
		
		Arguments
		---------
		prefix
			The key prefix under which to create the target key
		value
			A synchronous or asynchronous bytes or iterable of bytes object
		
		Returns
		-------
		The created key
		
		Raises
		------
		RuntimeError
			An internal error occurred
		NotImplementedError
			This datastore does not support this operation
		"""
		assert is_valid_value_type(value)
		return await self._put_new(prefix, util.stream.receive_stream_from(value), **kwargs)
	
	
	async def _put_new(self, prefix: key_.Key, value: util.stream.ReceiveStream,
	                   **kwargs: typing.Any) -> key_.Key:
		"""Like :meth:`put_new`, but always receives a :type:`datastore.abc.ReceiveStream` compatible object
		
		This way your datastore implementation doesn't have to do the
		conversion from the several supported input types supported itself.
		"""
		key, callable = await self._put_new_indirect(prefix, **kwargs)
		await callable(value)
		return key
	
	
	async def _put_new_indirect(self, prefix: key_.Key, **kwargs: typing.Any) -> _PUT_NEW_INDIRECT_RT:
		"""Like :meth:`_put_new`, but returns the target key before starting to read data
		
		This method initially only receives the key *prefix* below which to create the target key
		and should come up (possibly using asynchronous code) with the actual key that will be
		created and a callback function that receives the actual data and writes it to the
		backend. Datastores should strive to implement this method rather than just :func:`_put_new`
		to allow for compatiblity with the tiered datastore.
		"""
		raise NotImplementedError()
	
	
	@abc.abstractmethod
	async def delete(self, key: key_.Key) -> None:
		"""Removes the data named by `key`
		
		Arguments
		---------
		key
			Key naming the binary data slot to remove
		
		Raises
		------
		KeyError
			The given object was not present in this datastore
		RuntimeError
			An internal error occurred
		"""
		pass
	
	
	# Secondary API. Datastores MAY provide optimized implementations.
	
	
	async def contains(self, key: key_.Key) -> bool:
		"""Returns whether any data named by `key` exists
		
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
	
	
	async def get_all(self, key: key_.Key) -> bytes:
		"""Returns all the data named by `key` at once or raises `KeyError`
		   otherwise
		
		Arguments
		---------
		key
			Key naming the binary data to retrieve
		
		Raises
		------
		KeyError
			The given object was not present in this datastore
		RuntimeError
			An internal error occurred
		"""
		return await (await self.get(key)).collect()
	
	
	async def stat(self, key: key_.Key) -> util.metadata.StreamMetadata:
		"""Returns any metadata associated with the data stream named by `key`
		or raises `KeyError` otherwise
		
		Arguments
		---------
		key
			Key naming the data stream to query
		
		Raises
		------
		KeyError
			The given object was not present in this datastore
		RuntimeError
			An internal error occurred
		"""
		async with await self.get(key) as chann:
			return util.metadata.StreamMetadata(
				atime = chann.atime,
				mtime = chann.mtime,
				btime = chann.btime,
				size  = chann.size
			)
	
	
	def datastore_stats(self, selector: key_.Key = None, *, _seen: typing.Set[int] = None) \
	    -> util.metadata.DatastoreMetadata:
		"""Returns metadata of this datastore
		
		Unless overwritten this will not return any interesting value. In general,
		datastore backing implementations should try to at least expose a proper
		size measure if that is possible without any major accounting overhead.
		
		Arguments
		---------
		selector
			Used to select the backing store for some datastore adapters (such as
			mount) that have more than one backing store
			
			For datastore backends this will generally be ignored.
		_seen
			Set of Python object IDs of datastores already visited while gathering
			stats from datastore adapters with more than one then one backing store
			
			For datastore backends this must be silently ignored.
		
		Raises
		------
		RuntimeError
			An internal error occurred
		"""
		# The following should NOT be `util.metadata.DatastoreMetadata.IGNORE` as
		# that value would indicate that this datastore should be ignored during
		# size estimation rather than not implementing size estimation
		return util.metadata.DatastoreMetadata()
	
	
	async def aclose(self) -> None:
		"""Closes this any resources held by this datastore, possibly blocking
		
		Carefully read the documentation of :class:`trio.abc.AsyncResource`,
		particularly with regards to cancellation and forceful closings, when
		implementing this.
		"""
		pass



class NullDatastore(Datastore):
	"""Stores nothing, but conforms to the API. Useful to test with."""
	
	__slots__ = ()

	async def get(self, key: key_.Key) -> util.stream.ReceiveStream:
		"""Unconditionally raise `KeyError`"""
		raise KeyError(key)

	async def _put(self, key: key_.Key,  # type: ignore[override]
	               value: util.stream.ReceiveStream, *, create: bool, replace: bool) -> None:
		"""Do nothing with `key` and ignore the `value`"""
		pass

	async def delete(self, key: key_.Key) -> None:
		"""Pretend there is any object that could be removed by the name `key`"""
		pass

	async def query(self, query: query_.Query) -> query_.Cursor:
		"""This won't ever match anything"""
		return query([])  # type: ignore[no-any-return]



class DictDatastore(Datastore):
	"""Simple straw-man in-memory datastore backed by nested dicts."""
	
	__slots__ = ("_items",)

	_items: typing.Dict[str, typing.Dict[key_.Key, bytes]]

	def __init__(self) -> None:
		self._items = {}
	
	
	def _collection(self, key: key_.Key, parent: bool = True) \
	    -> typing.Dict[key_.Key, bytes]:
		"""Returns the namespace collection for `key`."""
		collection = str(key.path if parent else key)
		if collection not in self._items:
			self._items[collection] = dict()
		return self._items[collection]
	
	
	async def get(self, key: key_.Key) -> util.stream.ReceiveStream:
		"""Returns the object named by `key` or raises `KeyError`.
		
		Retrieves the object from the collection corresponding to ``key.path``.
		
		Arguments
		---------
		key
			Key naming the object to retrieve.
		"""
		return util.stream.receive_stream_from(self._collection(key)[key])
	
	
	async def get_all(self, key: key_.Key) -> bytes:
		"""Returns the object named by `key` or raises `KeyError`.
		
		Retrieves the object from the collection corresponding to ``key.path``.
		
		Arguments
		---------
		key
			Key naming the object to retrieve.
		"""
		return self._collection(key)[key]
	
	
	async def _put(self, key: key_.Key,  # type: ignore[override]
	               value: util.stream.ReceiveStream, *, create: bool, replace: bool) -> None:
		"""Stores the object `value` named by `key`.
		
		Stores the object in the collection corresponding to ``key.path``.
		
		Arguments
		---------
		key
			Key naming `value`
		value
			The object to store
		create
			Create the given key if it does not exist?
		replace
			Replace the given key if it does exist?
		"""
		# This isn't thread-safe, but that's OK as we don't actually
		# guarantee that, rather we are only safe with regards to trio's
		# task scheduling
		collection = self._collection(key)
		if not create and key not in collection:
			raise KeyError(key)
		if not replace and key in collection:
			raise KeyError(key)
		collection[key] = await value.collect()
	
	
	async def _put_new_indirect(self, prefix: key_.Key,  # type: ignore[override]
	) -> Datastore._PUT_NEW_INDIRECT_RT:
		"""Stores the data passed to the returned callback in a new key below *prefix*
		
		Stores the data in the collection corresponding to ``key``.
		
		Arguments
		---------
		prefix
			Key below which to store the given data
		"""
		# This isn't thread-safe, but that's OK as we don't actually
		# guarantee that, rather we are only safe with regards to trio's
		# task scheduling
		collection = self._collection(prefix, parent=False)
		while True:
			key = prefix.child(str(uuid.uuid4()))
			if key not in collection:
				break
		
		# Reserve key
		collection[key] = b""
		
		# Receive and write data later
		async def callback(value: util.stream.ReceiveStream) -> None:
			collection[key] = await value.collect()
		return key, callback
	
	
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
	
	
	async def stat(self, key: key_.Key) -> util.metadata.StreamMetadata:
		"""Returns the length of the byte sequence named by `key` if it exists.
		
		Checks for the sequence in the collection corresponding to ``key.path``.
		
		Arguments
		---------
		key
			Key naming a byte sequence
		"""
		return util.metadata.StreamMetadata(size=len(self._collection(key)[key]))
	
	
	def datastore_stats(self, selector: key_.Key = None, *, _seen: typing.Set[int] = None) \
	    -> util.metadata.DatastoreMetadata:
		"""Returns the number of bytes stored in this datastore
		
		Arguments
		---------
		selector
			Ignored by backing datastores
		"""
		size = sum(map(lambda c: sum(map(len, c.values())), self._items.values()))
		return util.metadata.DatastoreMetadata(size=size, size_accuracy="exact")
	
	
	def __len__(self) -> int:
		return sum(map(len, self._items.values()))
	
	
	async def aclose(self) -> None:
		"""Deletes all items from this datastore"""
		self._items.clear()
		await super().aclose()



class Adapter(Datastore):
	"""Represents a non-concrete datastore that adds functionality between the
	   client and a lower-level datastore.
	
	Shim datastores do not actually store
	data themselves; instead, they delegate storage to an underlying child
	datastore. The default implementation just passes all calls to the child.
	"""
	__slots__ = ("child_datastore",)
	
	FORWARD_CONTAINS:  bool = False
	FORWARD_GET_ALL:   bool = False
	FORWARD_PUT_NEW:   bool = False  # Not always true
	FORWARD_PUT_NEW_D: typing.Optional[bool] = None  # Defaults to the value of `FORWARD_PUT_NEW`
	FORWARD_PUT_NEW_I: typing.Optional[bool] = None  # Defaults to the value of `FORWARD_PUT_NEW`
	FORWARD_STAT:      bool = False
	
	child_datastore: Datastore
	
	def __init__(self, datastore: Datastore):
		"""Initializes this DatastoreAdapter with child `datastore`."""
		self.child_datastore = datastore
	
	
	# default implementation just passes all calls to child
	
	
	async def get(self, key: key_.Key) -> util.stream.ReceiveStream:
		"""Returns the binary stream named by `key` or raises `KeyError` if
		   it does not exist.

		Default shim implementation simply returns ``child_datastore.get(key)``
		Override to provide different functionality, for example::

			async def get(self, key):
				# Collect the data returned by child and decode it as JSON
				# (Note: Use `datastore.serializer.json` rather than this for real apps.)
				value = await self.child_datastore.get_all(key)
				return datastore.util.receive_stream_from(json.loads(value))

		Arguments
		---------
		key
			Key naming the data to retrieve.
		"""
		return await self.child_datastore.get(key)
	
	
	async def _put(self, key: key_.Key, value: util.stream.ReceiveStream, *,
	               create: bool, replace: bool, **kwargs: typing.Any) -> None:
		"""Stores the data from the binary stream *value* at name *key*.
		
		Default shim implementation simply calls ``child_datastore._put(key, value)``
		Override to provide different functionality, for example::
		
			async def _put(self, key, value):
				value = json.dumps(await value.collect())
				await self.child_datastore._put(key, value)
		
		Arguments
		---------
		key
			Key naming `value`
		value
			The data to store
		create
			Create the given key if it does not exist?
		replace
			Replace the given key if it does exist?
		"""
		await self.child_datastore._put(key, value, create=create, replace=replace, **kwargs)
	
	
	async def _put_new(self, prefix: key_.Key, value: util.stream.ReceiveStream,
	                   **kwargs: typing.Any) -> key_.Key:
		"""Stores the data from the binary stream *value* below *prefix*
		
		Default shim implementation simply returns ``child_datastore._put_new(prefix, value)``
		if ``FORWARD_PUT_NEW`` or ``FORWARD_PUT_NEW_D`` are `True`, a value based on
		:meth:`_put_new_indirect` otherwise.
		
		In general prefer overriding :meth:`_put_new_indirect` rather than this method to
		also support tiered datastore. Note however that both methods will have to be overriden
		to fully support datastores that only support `_put_new`, but not `_put_new_indirect`,
		on the one hand and ones that support both methods on the other.
		
		Arguments
		---------
		prefix
			Key below which to store *value*
		value
			The data to store
		"""
		if (self.FORWARD_PUT_NEW_D is not None and self.FORWARD_PUT_NEW_D) \
		   or (self.FORWARD_PUT_NEW_D is None and self.FORWARD_PUT_NEW):
			return await self.child_datastore._put_new(prefix, value, **kwargs)
		else:
			return await Datastore._put_new(self, prefix, value, **kwargs)  # May raise
	
	
	async def _put_new_indirect(self, prefix: key_.Key, **kwargs: typing.Any) \
	      -> Datastore._PUT_NEW_INDIRECT_RT:
		"""Stores the data from the binary stream passed to the returned callback below *prefix*
		
		Default shim implementation simply returns ``child_datastore._put_new_indirect(prefix)``
		if ``FORWARD_PUT_NEW`` or ``FORWARD_PUT_NEW_I`` are `True`, raises `NotImplementedError`
		otherwise.
		
		Override to provide different functionality, for example::
		
			async def _put_new_indirect(self, prefix):
				key, callback = await self.child_datastore._put_new_indirect(prefix)
				async def callback_wrapper(value):
					value = json.dumps(await value.collect())
					await callback(value)
				return key, callback_wrapper
		
		Arguments
		---------
		prefix
			Key below which to store the given data
		"""
		if (self.FORWARD_PUT_NEW_I is not None and self.FORWARD_PUT_NEW_I) \
		   or (self.FORWARD_PUT_NEW_I is None and self.FORWARD_PUT_NEW):
			return await self.child_datastore._put_new_indirect(prefix, **kwargs)
		else:
			return await Datastore._put_new_indirect(self, prefix, **kwargs)  # Will raise
	
	
	async def delete(self, key: key_.Key) -> None:
		"""Removes the object named by `key`.

		Default shim implementation simply calls ``child_datastore.delete(key)``
		Override to provide different functionality.

		Arguments
		---------
		key
			Key naming the data to remove.
		"""
		await self.child_datastore.delete(key)
	
	
	async def get_all(self, key: key_.Key) -> bytes:
		"""Returns the binary data named by `key` or raises `KeyError` if it
		   does not exist.
		
		Default shim implementation simply returns ``child_datastore.get_all(key)``
		if ``FORWARD_GET_ALL`` is `True`, ``(await get(key)).collect()`` otherwise.
		
		Override to provide different functionality, for example::
		
			async def get_all(self, key):
				# Collect the data returned by child and decode it as JSON
				# (Note: Use `datastore.serializer.json` rather than this for real apps.)
				value = await self.child_datastore.get_all(key)
				return datastore.util.receive_stream_from(json.loads(value))

		Arguments
		---------
		key
			Key naming the object to retrieve
		"""
		if self.FORWARD_GET_ALL:
			return await self.child_datastore.get_all(key)
		else:
			return await Datastore.get_all(self, key)
	
	
	async def contains(self, key: key_.Key) -> bool:
		"""Returns whether any data named by `key` exists
		
		Default shim implementation simply returns ``child_datastore.contains(key)``
		if ``FORWARD_CONTAINS`` is `True`, ``not (get(key) raises KeyError)`` otherwise.
		
		Arguments
		---------
		key
			Key naming the object to check.
		"""
		if self.FORWARD_CONTAINS:
			return await self.child_datastore.contains(key)
		else:
			return await Datastore.contains(self, key)
	
	
	async def stat(self, key: key_.Key) -> util.metadata.StreamMetadata:
		"""Returns the metadata of the stream named by `key` if it exists
		
		Default shim implementation simply returns ``child_datastore.stat(key)``
		if ``FORWARD_STAT`` is `True`, ``get(key)`` otherwise.
		
		Arguments
		---------
		key
			Key naming the stream to check.
		"""
		if self.FORWARD_STAT:
			return await self.child_datastore.stat(key)
		else:
			return await Datastore.stat(self, key)
	
	
	def datastore_stats(self, selector: key_.Key = None, *, _seen: typing.Set[int] = None) \
	    -> util.metadata.DatastoreMetadata:
		"""Returns metadata of the child datastore
		
		Arguments
		---------
		selector
			Used to select the backing store for some datastore adapters (such as
			mount) that have more than one backing store
			
			If this is ``None``, the result will be the sum of all datastores
			attached to this adapter.
		_seen
			Set of Python object IDs of datastores already visited while gathering
			stats from datastore adapters with more than one then one backing store
			
			This is required to ensure that no backing datastore is counted more
			than once if `selector` is ``None``.
		
		Raises
		------
		RuntimeError
			An internal error occurred in the child datastore
		"""
		_seen = _seen if _seen is not None else set()
		
		if id(self.child_datastore) in _seen:
			return util.metadata.DatastoreMetadata.IGNORE
		
		_seen.add(id(self.child_datastore))
		return self.child_datastore.datastore_stats(selector, _seen=_seen)
	
	
	async def aclose(self) -> None:
		"""Closes this any resources held by the child datastore
		
		Carefully read the documentation of :class:`trio.abc.AsyncResource`,
		particularily with regards to concellation and forceful closings, when
		implementating this.
		"""
		try:
			await self.child_datastore.aclose()
		finally:
			await super().aclose()


Datastore.ADAPTER_T  = Adapter
Datastore.METADATA_T = util.metadata.StreamMetadata
Datastore.RECEIVE_T  = util.stream.ReceiveStream


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
