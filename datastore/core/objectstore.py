import abc
import collections.abc
import typing

import trio

from . import key as key_
from . import query as query_
class util:  # noqa
	from .util import stream


T_co = typing.TypeVar("T_co", covariant=True)
U_co = typing.TypeVar("U_co", covariant=True)



def is_valid_value_type(value: util.stream.ArbitraryReceiveChannel) -> bool:
	"""Checks that `value` is of the right type for `Datastore.put`
	
	It's just too easy to acidentally pass in the wrong type without this check.
	Unfortunately this cannot check whether iterators return the correct types,
	so the utility of this function unfortunately is limited to some extent.
	"""
	return isinstance(value, (
		trio.abc.ReceiveChannel,
		collections.abc.AsyncIterable,
		collections.abc.Awaitable,
		collections.abc.Iterable
	)) and not isinstance(value, (str, bytes))


class Datastore(typing.Generic[T_co]):
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
	
	# Some possibly useful types (assigned at the end of this file)
	ADAPTER_T: type
	RECEIVE_T: type

	# Main API. Datastore implementations MUST implement these methods.
	
	
	@abc.abstractmethod
	async def get(self, key: key_.Key) -> util.stream.ReceiveChannel[T_co]:
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


	async def put(self, key: key_.Key, value: util.stream.ArbitraryReceiveChannel[T_co]) -> None:
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
		assert is_valid_value_type(value)
		await self._put(key, util.stream.receive_channel_from(value))
	

	@abc.abstractmethod
	async def _put(self, key: key_.Key, value: util.stream.ReceiveChannel[T_co]) -> None:
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


	async def get_all(self, key: key_.Key) -> typing.List[T_co]:
		"""Returns all the data named by `key` at once or raises `KeyError`
		   otherwise
		
		Arguments
		---------
		key
			Key naming the object list to retrieve
		
		Raises
		------
		KeyError
			The given object was not present in this datastore
		RuntimeError
			An internal error occurred
		"""
		return await (await self.get(key)).collect()



class NullDatastore(Datastore[T_co], typing.Generic[T_co]):
	"""Stores nothing, but conforms to the API. Useful to test with."""

	async def get(self, key: key_.Key) -> util.stream.ReceiveChannel[T_co]:
		"""Unconditionally raise `KeyError`"""
		raise KeyError(key)

	async def _put(self, key: key_.Key, value: util.stream.ReceiveChannel[T_co]) -> None:
		"""Do nothing with `key` and ignore the `value`"""
		pass

	async def delete(self, key: key_.Key) -> None:
		"""Pretend there is any object that could be removed by the name `key`"""
		pass

	async def query(self, query: query_.Query) -> query_.Cursor:
		"""This won't ever match anything"""
		return query([])



class DictDatastore(Datastore[T_co], typing.Generic[T_co]):
	"""Simple straw-man in-memory datastore backed by nested dicts."""
	
	_items: typing.Dict[str, typing.Dict[key_.Key, typing.List[T_co]]]
	
	def __init__(self):
		self._items = dict()
	
	
	def _collection(self, key: key_.Key) -> typing.Dict[key_.Key, typing.List[T_co]]:
		"""Returns the namespace collection for `key`."""
		collection = str(key.path)
		if collection not in self._items:
			self._items[collection] = dict()
		return self._items[collection]
	
	
	async def get(self, key: key_.Key) -> util.stream.ReceiveChannel[T_co]:
		"""Returns the object named by `key` or raises `KeyError`.
		
		Retrieves the object from the collection corresponding to ``key.path``.
		
		Arguments
		---------
		key
			Key naming the object to retrieve.
		"""
		return util.stream.receive_channel_from(self._collection(key)[key])
	
	
	async def get_all(self, key: key_.Key) -> typing.List[T_co]:
		"""Returns all the data named by `key` at once or raises `KeyError`
		   otherwise
		
		Arguments
		---------
		key
			Key naming the object list to retrieve
		
		Raises
		------
		KeyError
			The given object was not present in this datastore
		RuntimeError
			An internal error occurred
		"""
		return self._collection(key)[key]
	

	
	async def _put(self, key: key_.Key, value: util.stream.ReceiveChannel[T_co]) -> None:
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



class Adapter(Datastore[T_co], typing.Generic[T_co, U_co]):
	"""Represents a non-concrete datastore that adds functionality between the
	   client and a lower-level datastore.
	
	Shim datastores do not actually store
	data themselves; instead, they delegate storage to an underlying child
	datastore. The default implementation just passes all calls to the child.
	"""
	
	child_datastore: Datastore[U_co]
	
	
	def __init__(self, datastore: Datastore[U_co]):
		"""Initializes this DatastoreAdapter with child `datastore`."""
		self.child_datastore = datastore
	
	
	# default implementation just passes all calls to child
	async def get(self, key: key_.Key) -> util.stream.ReceiveChannel[T_co]:
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
		# Cast the following so that we can have child object stores with
		# types different from their parent
		# (It's basically the callers job to ensure that the datastore adapter
		#  used can in fact perform this conversion.)
		return typing.cast(util.stream.ReceiveChannel[T_co], await self.child_datastore.get(key))
	
	
	async def _put(self, key: key_.Key, value: util.stream.ReceiveChannel[T_co]) -> None:
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
		# See :meth:`~get` for why we cast here
		await self.child_datastore.put(key, typing.cast(util.stream.ReceiveChannel[U_co], value))
	
	
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


Datastore.ADAPTER_T = Adapter
Datastore.RECEIVE_T = util.stream.ReceiveChannel



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
