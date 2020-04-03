import typing

import trio

import datastore

from ._support import DS, MD, RT, RV, T_co

__all__ = [
	"BinaryAdapter",
	"ObjectAdapter"
]


mount_item_t = typing.Tuple['mount_tree_t[DS]', typing.Optional[DS]]


class mount_tree_t(typing.Dict[str, mount_item_t[DS]]):
	...


class _Adapter(typing.Generic[DS, MD, RT, RV]):
	__slots__ = ()
	
	mounts: mount_tree_t[DS]
	
	
	def __init__(self, *args: typing.Any, **kwargs: typing.Any):
		self.mounts = mount_tree_t()
	
	
	def _find_mountpoint(self, key: datastore.Key) \
	    -> typing.Tuple[typing.Optional[DS], datastore.Key, datastore.Key]:
		current = self.mounts
		
		ds = None
		offset = 0
		for idx, part in enumerate(map(str, key.list)):
			mount = current.get(part)
			if mount is None:
				break
			
			if mount[1] is not None:
				ds = mount[1]
				offset = idx
			
			current = mount[0]
		
		return ds, datastore.Key(key.list[:offset]), datastore.Key(key.list[(offset + 1):])
	
	
	def _store_iter(self, *, mount_tree: mount_tree_t[DS] = None) -> typing.Iterator[DS]:
		if mount_tree is None:
			mount_tree = self.mounts
		
		for subtree, ds in mount_tree.values():
			if ds is not None:
				yield ds
			
			if subtree is not None:
				yield from self._store_iter(mount_tree=subtree)
	
	
	async def get(self, key: datastore.Key) -> RT:
		ds, _, subkey = self._find_mountpoint(key)
		if ds is None:
			raise KeyError(key)
		return await ds.get(subkey)  # type: ignore[return-value]
	
	
	async def get_all(self, key: datastore.Key) -> RV:
		ds, _, subkey = self._find_mountpoint(key)
		if ds is None:
			raise KeyError(key)
		return await ds.get_all(subkey)  # type: ignore[return-value]
	
	
	async def _put(self, key: datastore.Key, value: RT, **kwargs: typing.Any) -> None:
		ds, _, subkey = self._find_mountpoint(key)
		if ds is None:
			raise RuntimeError(f"Cannot put key {key}: No datastore mounted at that path")
		await ds._put(subkey, value, **kwargs)  # type: ignore[arg-type]
	
	async def _put_new(self, prefix: datastore.Key, value: RT, **kwargs: typing.Any) \
	      -> datastore.Key:
		ds, dskey, subkey = self._find_mountpoint(prefix)
		if ds is None:
			raise RuntimeError(f"Cannot put below key {prefix}: No datastore mounted at that path")
		return dskey.child(await ds._put_new(subkey, value, **kwargs))  # type: ignore[arg-type]
	
	
	async def _put_new_indirect(self, prefix: datastore.Key, **kwargs: typing.Any) \
	      -> typing.Tuple[datastore.Key, typing.Callable[[RT], typing.Awaitable[None]]]:
		ds, dskey, subkey = self._find_mountpoint(prefix)
		if ds is None:
			raise RuntimeError(f"Cannot put below key {prefix}: No datastore mounted at that path")
		new_subkey, callback = await ds._put_new_indirect(subkey, **kwargs)
		return dskey.child(new_subkey), callback  # type: ignore[return-value]
	
	
	async def delete(self, key: datastore.Key) -> None:
		ds, _, subkey = self._find_mountpoint(key)
		if ds is None:
			raise KeyError(key)
		await ds.delete(subkey)
	
	
	async def contains(self, key: datastore.Key) -> bool:
		ds, _, subkey = self._find_mountpoint(key)
		if ds is None:
			return False
		return await ds.contains(subkey)
	
	
	async def stat(self, key: datastore.Key) -> MD:
		ds, _, subkey = self._find_mountpoint(key)
		if ds is None:
			raise KeyError(key)
		return await ds.stat(subkey)  # type: ignore[return-value]
	
	
	def datastore_stats(self, selector: datastore.Key = None, *, _seen: typing.Set[int] = None) \
	    -> datastore.util.DatastoreMetadata:
		"""Returns metadata of the child datastore refered to by `selector` or all children
		
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
		RuntimeError
			Argument `selector` was not ``None`` and there was no datastore mounted
			that matched its value
		"""
		_seen = _seen if _seen is not None else set()
		
		if selector is not None:
			ds, _, subkey = self._find_mountpoint(selector)
			if ds is None:
				raise RuntimeError(f"Cannot retrieve stats of datastore mounted at {selector}: "
				                   f"No datastore mounted at that path")
			
			if id(ds) in _seen:
				return datastore.util.DatastoreMetadata.IGNORE
			
			_seen.add(id(ds))
			return ds.datastore_stats(subkey, _seen=_seen)
		else:
			metadata = datastore.util.DatastoreMetadata.IGNORE
			for ds in self._store_iter():
				if id(ds) in _seen:
					continue
				
				_seen.add(id(ds))
				metadata += ds.datastore_stats(_seen=_seen)
			return metadata
	
	
	def mount(self, prefix: datastore.Key, ds: DS) -> None:
		"""Mounts the datastore `ds` at key `prefix`
		
		Raises
		------
		KeyError
			Another datastore was already mounted at the given key
		"""
		current:  mount_tree_t[DS] = self.mounts
		previous: mount_tree_t[DS]
		
		# Walk and create all parent key parts
		for part in map(str, prefix.list):
			if part not in current:
				current[part] = (mount_tree_t(), None)
			previous = current
			current  = current[part][0]
		
		# Add the given datastore as final entry value
		entry = previous[str(prefix.list[-1])]
		if entry[1] is not None:
			raise KeyError(prefix)
		previous[str(prefix.list[-1])] = (entry[0], ds)
	
	
	def unmount(self, prefix: datastore.Key) -> DS:
		"""Unmounts and returns the datastore at key `prefix`
		
		The returned datastore is not closed; it is the callers responsibility
		to ensure this by using ``await m.unmount(key).aclose()`` or similar.
		
		Raises
		------
		KeyError
			No datastore was mounted at the given key
		"""
		current: mount_tree_t[DS] = self.mounts
		
		# Walk and create all parent key parts
		visited: typing.List[mount_tree_t[DS]] = []
		for part in map(str, prefix.list):
			if part not in current:
				raise KeyError(prefix)
			visited.append(current)
			current = current[part][0]
		
		# Remove the datastore from the final key part
		entry = visited[-1][str(prefix.list[-1])]
		if entry[1] is None:
			raise KeyError(prefix)
		visited[-1][str(prefix.list[-1])] = (entry[0], None)
		
		# Remove all now-empty parents
		for current in reversed(visited):
			if list(current.values()) == [({}, None)]:
				current.clear()
		
		return entry[1]
	
	
	def unmount_all(self) -> typing.List[DS]:
		"""Unmounts and returns all datastores currently mounted
		
		The returned datastores are not closed; it is the callers responsibility
		to ensure this. For removing __and__ closing all mounts use the
		:meth:`aclose` method instead.
		"""
		unmounted: typing.List[DS] = []
		
		# Start at mount hierarchy root
		current: mount_tree_t[DS] = self.mounts
		
		# Walk and create all parent key parts
		stack: typing.List[mount_tree_t[DS]] = []
		while len(self.mounts) > 0 or len(stack) > 0:
			try:
				# Pop item from submounts map
				_, (next, ds) = current.popitem()
				
				# Move datastore (if any) to list of unmounted datastores
				if ds is not None:
					unmounted.append(ds)
				
				# Recurse into submount
				stack.append(current)
				current = next
			except KeyError:
				# Move out of empty submount (this assignment drops the last
				# reference to the submount)
				current = stack.pop()
		
		assert self.mounts == {}
		
		return unmounted
	
	
	async def aclose(self) -> None:
		"""Closes and removes all mounted datastores"""
		errors: typing.List[Exception] = []
		
		for store in self.unmount_all():
			try:
				await store.aclose()
			except trio.Cancelled:
				pass  # We check for cancellation later on
			except Exception as error:
				errors.append(error)
		
		# Ensure error propagation
		if errors:
			raise trio.MultiError(errors)
		
		# Ensure cancellation is propagated
		await trio.sleep(0)


class BinaryAdapter(
		_Adapter[
			datastore.abc.BinaryDatastore,
			datastore.util.StreamMetadata,
			datastore.abc.ReceiveStream,
			bytes
		],
		datastore.abc.BinaryAdapter
):
	__slots__ = ("mounts",)


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
	__slots__ = ("mounts",)
