import typing

import trio

import datastore
import datastore.abc
import datastore.core.util.stream

from . import _support
from ._support import DS, MD, RT, RV, T_co

__all__ = ("BinaryAdapter", "ObjectAdapter")




@typing.no_type_check
async def run_put_task(receive_stream: datastore.abc.ReceiveStream, store: DS, key: datastore.Key,
                       kwargs: typing.Dict[str, typing.Any] = {}) -> None:
	await store.put(key, receive_stream, **kwargs)


class _Adapter(_support.DatastoreCollectionMixin[DS], typing.Generic[DS, MD, RT, RV]):
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
		* stat     : returns first found value
		* query    : queries bottom (most complete) datastore
	"""
	__slots__ = ()
	
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
		result_stream: typing.Union[
			datastore.core.util.stream.TeeingReceiveStream,
			datastore.core.util.stream.TeeingReceiveChannel[T_co]
		]
		if isinstance(self, datastore.abc.BinaryDatastore):
			result_stream = datastore.core.util.stream.TeeingReceiveStream(value)
		elif isinstance(self, datastore.abc.ObjectDatastore):
			result_stream = datastore.core.util.stream.TeeingReceiveChannel(value)
		else:
			assert False
		
		for store2 in stores:
			if store is store2:
				break
			result_stream.start_task_soon(run_put_task, store2, key)
		
		return result_stream
	
	
	async def get_all(self, key: datastore.Key) -> RV:
		"""Return the object named by key. Checks each datastore in order."""
		return await (await self.get(key)).collect()  # type: ignore[return-value]
	
	
	async def _put(self, key: datastore.Key, value: RT, **kwargs: typing.Any) -> None:
		"""Stores the object in all underlying datastores."""
		result_stream: typing.Union[
			datastore.core.util.stream.TeeingReceiveStream,
			datastore.core.util.stream.TeeingReceiveChannel[T_co]
		]
		if isinstance(self, datastore.abc.BinaryDatastore):
			result_stream = datastore.core.util.stream.TeeingReceiveStream(value)
		elif isinstance(self, datastore.abc.ObjectDatastore):
			result_stream = datastore.core.util.stream.TeeingReceiveChannel(value)
		else:
			assert False
		
		try:
			for store in self._stores:
				if store is self._stores[-1]:
					break  # Last store drives this `TeeingReceiveStream`
				result_stream.start_task_soon(run_put_task, store, key, kwargs)
			await self._stores[-1]._put(key, result_stream, **kwargs)  # type: ignore[arg-type]
		except BaseException:
			# Ensure the other tasks are immediately canceled if the final
			# store's put raises an exception
			#
			# Without this the nursery and its attached tasks will stay open
			# and cause an undecipherable “the init task should be the last
			# task to exit” error on loop exit.
			await result_stream.aclose()
			raise
	
	
	async def _put_new_indirect(self, prefix: datastore.Key, **kwargs: typing.Any) \
	      -> typing.Tuple[datastore.Key, typing.Callable[[RT], typing.Awaitable[None]]]:
		"""Stores the object in all underlying datastores."""
		result_stream: typing.Union[
			datastore.core.util.stream.TeeingReceiveStream,
			datastore.core.util.stream.TeeingReceiveChannel[T_co]
		]
		if isinstance(self, datastore.abc.BinaryDatastore):
			result_stream = datastore.core.util.stream.TeeingReceiveStream(None)
		elif isinstance(self, datastore.abc.ObjectDatastore):
			result_stream = datastore.core.util.stream.TeeingReceiveChannel(None)
		else:
			assert False
		
		kwargs2 = kwargs.copy()
		kwargs2["create"]  = True
		kwargs2["replace"] = False
		
		try:
			# Call `_put_new_indirect` to determine the key to create up-front
			key, callback = await self._stores[-1]._put_new_indirect(prefix, **kwargs)
			
			try:
				# Do a regular `put` with the received key
				for store in self._stores:
					if store is self._stores[-1]:
						break  # Last store drives this `TeeingReceiveStream`
					result_stream.start_task_soon(run_put_task, store, key, kwargs2)
				
				async def callback_wrapper(value: RT) -> None:  # type: ignore[return]  # mypy bug
					result_stream.source = value
					await callback(result_stream)  # type: ignore[arg-type]
				
				return key, callback_wrapper
			except BaseException:
				# Propage a cancellation to the final datastore to release any resources
				# it may hold in the expectation of being called “soon”
				with trio.CancelScope(deadline=0):
					await callback(result_stream)  # type: ignore[arg-type]
				raise
		except BaseException:
			# Ensure the other tasks are immediately canceled if the final
			# store's put raises an exception
			#
			# Without this the nursery and its attached tasks will stay open
			# and cause an undecipherable “the init task should be the last
			# task to exit” error on loop exit.
			await result_stream.aclose()
			raise
	
	
	async def delete(self, key: datastore.Key) -> None:
		"""Removes the object from all underlying datastores."""
		error_count = 0

		async def count_key_errors(  # type: ignore[return]  # mypy bug
				store: DS, key: datastore.Key
		) -> None:
			nonlocal error_count
			try:
				await store.delete(key)
			except KeyError:
				error_count += 1
		
		async with trio.open_nursery() as nursery:
			for store in self._stores:
				nursery.start_soon(count_key_errors, store, key)
		
		# Raise exception if none of the subordinated datastores contained this
		# key (and hence it wasn't actually available in the first place)
		if error_count >= len(self._stores):
			raise KeyError(key)
	
	
	async def query(self, query: datastore.Query) -> datastore.Cursor:
		"""Returns a sequence of objects matching criteria expressed in `query`.
		The last datastore will handle all query calls, as it has a (if not
		the only) complete record of all objects.
		"""
		# queries hit the last (most complete) datastore
		return await self._stores[-1].query(query)  # type: ignore[attr-defined, no-any-return]
	
	
	async def contains(self, key: datastore.Key) -> bool:
		"""Returns whether the object is in this datastore."""
		for store in self._stores:
			if await store.contains(key):
				return True
		return False
	
	
	async def rename(self, key1: datastore.Key, key2: datastore.Key, *,
	                 replace: bool = True) -> None:
		"""Renames item *key1* to *key2*"""
		renamed: typing.List[DS] = []
		
		async def do_rename(store: DS) -> None:  # type: ignore[return]  # mypy bug
			await store.rename(key1, key2, replace=replace)
			renamed.append(store)
		
		try:
			async with trio.open_nursery() as nursery:
				for store in self._stores:
					nursery.start_soon(do_rename, store)
		except BaseException:
			# Try to undo our changes
			with trio.CancelScope(shield=True):
				for store in reversed(renamed):
					try:
						await store.rename(key2, key1, replace=False)
					except BaseException:
						pass  # Swallow all exceptions
			raise
	
	
	async def stat(self, key: datastore.Key) -> MD:
		"""Returns the metadata of the object named by key. Checks each
		datastore in order."""
		metadata: typing.Optional[MD] = None
		exceptions: typing.List[KeyError] = []
		
		# Take snapshot of store list so that the list will remain consistent
		# during the following iteration, even as other tasks may run
		# during `await`/`async with`
		stores: typing.List[DS] = self._stores.copy()
		for store in stores:
			try:
				metadata_: MD = await store.stat(key)  # type: ignore[assignment]
			except KeyError as exc:
				exceptions.append(exc)
			else:
				metadata = metadata_
				break
		if metadata is None:
			raise trio.MultiError(exceptions)
		
		return metadata
	
	
	async def aclose(self) -> None:
		"""Closes and removes all added datastores"""
		await self._stores_cleanup()


class BinaryAdapter(
		_Adapter[
			datastore.abc.BinaryDatastore,
			datastore.util.StreamMetadata,
			datastore.abc.ReceiveStream,
			bytes
		],
		datastore.abc.BinaryAdapter
):
	__slots__ = ("_stores",)


class ObjectAdapter(
		typing.Generic[T_co],
		_Adapter[
			datastore.abc.ObjectDatastore[T_co],
			datastore.util.ChannelMetadata,
			datastore.abc.ReceiveChannel[T_co],
			typing.List[T_co]
		],
		datastore.abc.ObjectAdapter[T_co, T_co]
):
	__slots__ = ("_stores",)
