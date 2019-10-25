import typing

import datastore
import trio

from . import _support
from ._support import DS, RT


__all__ = ["BinaryAdapter", "ObjectAdapter"]



class _Adapter(_support.DatastoreCollectionMixin[DS], typing.Generic[DS]):
	"""Represents a collection of datastore shards.
	
	A datastore is selected based on a sharding function.
	Sharding functions should take a Key and return an integer.
	
	Caution
	-------
	Adding or removing datastores while mid-use may severely affect consistency.
	Also ensure the order is correct upon initialization. While this is not as
	important for caches, it is crucial for persistent datastores.
	"""
	
	_shardingfn: _support.FunctionProperty[typing.Callable[[datastore.Key], int]]
	
	
	def __init__(self, stores: typing.Collection[DS] = [],
	             sharding_fn: typing.Callable[[datastore.Key], int] = hash):
		"""Initialize the datastore with any provided datastore."""
		_support.DatastoreCollectionMixin.__init__(self, stores)
		self._shardingfn = sharding_fn
	
	
	def shard(self, key: datastore.Key):
		"""Returns the shard index to handle `key`, according to sharding fn."""
		return self._shardingfn(key) % len(self._stores)
	
	
	def get_sharded_datastore(self, key: datastore.Key):
		"""Returns the shard to handle `key`."""
		return self.get_datastore_at(self.shard(key))
	
	
	async def get(self, key: datastore.Key) -> RT:
		"""Return the object named by key from the corresponding datastore."""
		return await self.get_sharded_datastore(key).get(key)
	
	
	async def _put(self, key: datastore.Key, value: RT) -> None:
		"""Stores the object to the corresponding datastore."""
		await self.get_sharded_datastore(key).put(key, value)
	
	
	async def delete(self, key: datastore.Key) -> None:
		"""Removes the object from the corresponding datastore."""
		await self.get_sharded_datastore(key).delete(key)
	
	
	async def contains(self, key: datastore.Key) -> bool:
		"""Returns whether the object is in this datastore."""
		return await self.get_sharded_datastore(key).contains(key)
	
	
	async def query(self, query: datastore.Query) -> datastore.Cursor:
		"""Returns a sequence of objects matching criteria expressed in `query`"""
		cursor = datastore.Cursor(query, self._shard_query_generator(query))
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

class BinaryAdapter(_Adapter[datastore.abc.BinaryDatastore], datastore.abc.BinaryAdapter): ...
class ObjectAdapter(_Adapter[datastore.abc.ObjectDatastore], datastore.abc.ObjectAdapter): ...
