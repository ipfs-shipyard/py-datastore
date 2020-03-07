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
	    -> typing.Tuple[typing.Optional[DS], datastore.Key]:
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
		
		return ds, datastore.Key(key.list[(offset + 1):])
	
	
	async def get(self, key: datastore.Key) -> RT:
		ds, subkey = self._find_mountpoint(key)
		if ds is None:
			raise KeyError(key)
		return await ds.get(subkey)  # type: ignore[return-value]
	
	
	async def get_all(self, key: datastore.Key) -> RV:
		ds, subkey = self._find_mountpoint(key)
		if ds is None:
			raise KeyError(key)
		return await ds.get_all(subkey)  # type: ignore[return-value]
	
	
	async def _put(self, key: datastore.Key, value: RT) -> None:
		ds, subkey = self._find_mountpoint(key)
		if ds is None:
			raise RuntimeError(f"Cannot put key {key}: No datastore mounted at this path")
		await ds.put(subkey, value)
	
	
	async def delete(self, key: datastore.Key) -> None:
		ds, subkey = self._find_mountpoint(key)
		if ds is None:
			raise KeyError(key)
		await ds.delete(subkey)
	
	
	async def contains(self, key: datastore.Key) -> bool:
		ds, subkey = self._find_mountpoint(key)
		if ds is None:
			return False
		return await ds.contains(subkey)
	
	
	async def stat(self, key: datastore.Key) -> MD:
		ds, subkey = self._find_mountpoint(key)
		if ds is None:
			raise KeyError(key)
		return await ds.stat(subkey)  # type: ignore[return-value]
	
	
	def mount(self, prefix: datastore.Key, ds: DS) -> None:
		"""Mounts the datastore `ds` at key `prefix`
		
		If a datastore is already mounted at the given key a :exc:`KeyError` is
		raised.
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
		
		If no datastore is mounted at the given key a :exc:`KeyError` is
		raised.
		
		The returned datastore is not closed; it is the callers responsibility
		to ensure this by using ``await m.unmount(key).aclose()`` or similar.
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
