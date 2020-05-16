import typing

import datastore

from . import _support
from ._support import DS, MD, RT, RV, T_co

__all__ = ("BinaryAdapter", "ObjectAdapter")



class _Adapter(_support.DatastoreCollectionMixin[DS], typing.Generic[DS, MD, RT, RV]):
	"""Represents a collection of datastore shards
	
	A datastore is selected based on a sharding function.
	Sharding functions should take a Key and return an integer.
	
	Caution
	-------
	Adding or removing datastores while mid-use may severely affect consistency.
	Also ensure the order is correct upon initialization. While this is not as
	important for caches, it is crucial for persistent datastores.
	"""
	
	__slots__ = ()
	
	_shardingfn: _support.FunctionProperty[typing.Callable[[datastore.Key], int]]
	
	
	def __init__(self, stores: typing.Collection[DS] = [],
	             sharding_fn: typing.Callable[[datastore.Key], int] = hash):
		"""Initialize the datastore with any provided datastore."""
		_support.DatastoreCollectionMixin.__init__(self, stores)
		self._shardingfn = sharding_fn
	
	
	def shard(self, key: datastore.Key) -> int:
		"""Returns the shard index to handle `key`, according to sharding fn."""
		return self._shardingfn(key) % len(self._stores)
	
	
	def get_sharded_datastore(self, key: datastore.Key) -> DS:
		"""Returns the shard to handle `key`."""
		return self.get_datastore_at(self.shard(key))
	
	
	async def get(self, key: datastore.Key) -> RT:
		"""Return the object named by key from the corresponding datastore."""
		return await self.get_sharded_datastore(key).get(key)  # type: ignore[return-value]
	
	
	async def get_all(self, key: datastore.Key) -> RV:
		"""Return the object named by key from the corresponding datastore."""
		return await self.get_sharded_datastore(key).get_all(key)  # type: ignore[return-value]
	
	
	async def _put(self, key: datastore.Key, value: RT, **kwargs: typing.Any) -> None:
		"""Stores the object to the corresponding datastore."""
		await self.get_sharded_datastore(key).put(key, value, **kwargs)
	
	
	async def delete(self, key: datastore.Key) -> None:
		"""Removes the object from the corresponding datastore."""
		await self.get_sharded_datastore(key).delete(key)
	
	
	async def contains(self, key: datastore.Key) -> bool:
		"""Returns whether the object is in this datastore."""
		return await self.get_sharded_datastore(key).contains(key)
	
	
	async def stat(self, key: datastore.Key) -> MD:
		"""Returns the metadata of the object named by key from the corresponding datastore"""
		return await self.get_sharded_datastore(key).stat(key)  # type: ignore[return-value]
	
	
	def datastore_stats(self, selector: datastore.Key = None, *, _seen: typing.Set[int] = None) \
	    -> datastore.util.DatastoreMetadata:
		"""Returns metadata of the child datastore corresponding to `selector` or all children
		
		Arguments
		---------
		selector
			Key that is used to look up which child datastore should be queried
			
			If this is ``None``, the result will be the sum of all datastores
			attached to this adapter.
		
		Raises
		------
		RuntimeError
			An internal error occurred in (one of) the child datastore(s)
		"""
		if selector is not None:
			return self.get_sharded_datastore(selector).datastore_stats(selector, _seen=_seen)
		return super().datastore_stats(_seen=_seen)
	
	
	async def query(self, query: datastore.Query) -> datastore.Cursor:
		"""Returns a sequence of objects matching criteria expressed in `query`"""
		cursor = datastore.Cursor(query, self._shard_query_generator(query))
		cursor.apply_order()  # ordering sharded queries is expensive (no generator)
		return cursor
	
	
	def _shard_query_generator(self, query):  # type: ignore  #FIXME: broken
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
	
	
	async def aclose(self) -> None:
		"""Closes and removes all added datastores"""
		await self._stores_cleanup()



class BinaryAdapter(
		_Adapter[
			datastore.datastore_abc.BinaryDatastore,
			datastore.util.StreamMetadata,
			datastore.datastore_abc.ReceiveStream,
			bytes
		],
		datastore.datastore_abc.BinaryAdapter
):
	__slots__ = ("_shardingfn", "_stores")


class ObjectAdapter(
		typing.Generic[T_co],
		_Adapter[
			datastore.datastore_abc.ObjectDatastore[T_co],
			datastore.util.ChannelMetadata,
			datastore.datastore_abc.ReceiveChannel[T_co],
			typing.List[T_co]
		],
		datastore.datastore_abc.ObjectAdapter[T_co, T_co]
):
	__slots__ = ("_shardingfn", "_stores")
