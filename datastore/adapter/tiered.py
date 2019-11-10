import typing

import datastore
import datastore.abc
import trio

from . import _support
from ._support import DS, RT


__all__ = ["BinaryAdapter", "ObjectAdapter"]


import datastore.core.util.stream


@typing.no_type_check
async def run_put_task(receive_stream: trio.abc.ReceiveStream, store: DS, key: datastore.Key):
	await store.put(key, receive_stream)


class _Adapter(_support.DatastoreCollectionMixin[DS], typing.Generic[DS]):
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
	
	@typing.no_type_check
	async def get(self, key: datastore.Key) -> RT:
		"""Return the object named by key. Checks each datastore in order."""
		value: typing.Optional[RT] = None
		exceptions: typing.List[KeyError] = []
		
		# Take snapshot of store list so that the list will remain consistent
		# over the full execution of this method, even as other tasks may run
		# during `await`/`async with`
		stores: typing.List[DS] = self._stores.copy()
		for store in stores:
			try:
				value_: RT = await store.get(key)  # type: ignore[assignment]
			except KeyError as exc:
				exceptions.append(exc)
			else:
				value = value_
				break
		if value is None:
			raise trio.MultiError(exceptions)
		
		# Add model to lower stores only
		if isinstance(self._stores[0], datastore.abc.BinaryDatastore):
			result_stream = datastore.core.util.stream.TeeingReceiveStream(value)
		else:
			result_stream = datastore.core.util.stream.TeeingReceiveChannel(value)
		
		for store2 in stores:
			if store is store2:
				break
			result_stream.start_task_soon(run_put_task, store2, key)
		
		return result_stream
	
	
	@typing.no_type_check
	async def _put(self, key: datastore.Key, value: RT) -> None:
		"""Stores the object in all underlying datastores."""
		if isinstance(self._stores[0], datastore.abc.BinaryDatastore):
			result_stream = datastore.core.util.stream.TeeingReceiveStream(value)
		else:
			result_stream = datastore.core.util.stream.TeeingReceiveChannel(value)
		
		for store in self._stores:
			if store is self._stores[-1]:
				break  # Last store drives this `TeeingReceiveStream`
			result_stream.start_task_soon(run_put_task, store, key)
		await self._stores[-1].put(key, result_stream)
	
	
	@typing.no_type_check
	async def delete(self, key: datastore.Key) -> None:
		"""Removes the object from all underlying datastores."""
		error_count = 0
		async def count_key_errors(coroutine):
			nonlocal error_count
			try:
				await coroutine
			except KeyError:
				error_count += 1
		
		async with trio.open_nursery() as nursery:
			for store in self._stores:
				nursery.start_soon(count_key_errors, store.delete(key))
		
		# Raise exception if non of the subordinated datastores contained this
		# key (and hence it wasn't actually available in the first place)
		if error_count >= len(self._stores):
			raise KeyError(key)
	
	
	@typing.no_type_check
	async def query(self, query: datastore.Query) -> datastore.Cursor:
		"""Returns a sequence of objects matching criteria expressed in `query`.
		The last datastore will handle all query calls, as it has a (if not
		the only) complete record of all objects.
		"""
		# queries hit the last (most complete) datastore
		return await self._stores[-1].query(query)
	
	
	@typing.no_type_check
	async def contains(self, key: datastore.Key) -> bool:
		"""Returns whether the object is in this datastore."""
		for store in self._stores:
			if await store.contains(key):
				return True
		return False


class BinaryAdapter(_Adapter[datastore.abc.BinaryDatastore], datastore.abc.BinaryAdapter): ...
class ObjectAdapter(_Adapter[datastore.abc.ObjectDatastore], datastore.abc.ObjectAdapter): ...
