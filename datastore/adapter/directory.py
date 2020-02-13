import typing

import datastore

__all__ = ("ObjectDirectorySupport", "ObjectDatastore")


T_co = typing.TypeVar("T_co", covariant=True)


class ObjectDirectorySupport:
	"""Datastore that allows manual tracking of directory entries.
	
	For example:
	
		>>> ds = DirectoryDatastore(ds)
		>>>
		>>> # initialize directory at /foo
		>>> ds.directory(Key('/foo'))
		>>>
		>>> # adding directory entries
		>>> ds.directory_add(Key('/foo'), Key('/foo/bar'))
		>>> ds.directory_add(Key('/foo'), Key('/foo/baz'))
		>>>
		>>> # value is a generator returning all the keys in this dir
		>>> for key in ds.directory_read(Key('/foo')):
		...   print(key)
		Key('/foo/bar')
		Key('/foo/baz')
		>>>
		>>> # querying for a collection works
		>>> for item in ds.query(Query(Key('/foo'))):
		... 	print(item)
		'bar'
		'baz'
	"""
	
	@typing.no_type_check
	async def directory(self, dir_key: datastore.Key, exist_ok: bool = False) -> bool:
		"""Initializes directory at dir_key.
		
		Returns a boolean of whether a new directory was actually created or
		not."""
		try:
			await (await super().get(dir_key)).aclose()
		except KeyError:
			await super()._put(dir_key, datastore.util.receive_channel_from([]))
			return True
		else:
			if not exist_ok:
				raise KeyError(str(dir_key))
			return False
	
	
	@typing.no_type_check
	async def directory_read(self, dir_key: datastore.Key) -> typing.AsyncIterable[datastore.Key]:
		"""Returns a generator that iterates over all keys in the directory
		referenced by `dir_key`

		Raises `KeyError` if the directory `dir_key` does not exist
		"""
		async with await self.get(dir_key) as dir_items_iter:
			async for dir_item in dir_items_iter:
				yield datastore.Key(dir_item)
	
	
	@typing.no_type_check
	async def directory_add(self, dir_key: datastore.Key, key: datastore.Key,
	                        create: bool = False) -> None:
		"""Adds directory entry `key` to directory at `dir_key`.

		If the directory `dir_key` does not exist, `KeyError` will be raised
		unless `create` is True in which case the directory will be created
		instead.
		"""
		key_str = str(key)
		
		dir_items: typing.List[str] = []
		try:
			dir_items = [item async for item in await super().get(dir_key)]
		except KeyError:
			if not create:
				raise
		
		if key_str not in dir_items:
			dir_items.append(key_str)
			await super()._put(dir_key, datastore.util.receive_channel_from(dir_items))
	
	
	@typing.no_type_check
	async def directory_remove(self, dir_key: datastore.Key, key: datastore.Key,
	                           missing_ok: bool = False) -> None:
		"""Removes directory entry `key` from directory at `dir_key`.

		If either the directory `dir_key` or the directory entry `key` don't
		exist, `KeyError` will be raised unless `missing_ok` is set to `True`.
		"""
		key_str = str(key)
		
		try:
			dir_items = [item async for item in await super().get(dir_key)]
		except KeyError:
			if not missing_ok:
				raise
			return
		
		try:
			dir_items.remove(key_str)
		except ValueError:
			if not missing_ok:
				raise KeyError(f"{key} in {dir_key}") from None
		else:
			await super()._put(dir_key, datastore.util.receive_channel_from(dir_items))



class ObjectDatastore(
		ObjectDirectorySupport,
		datastore.abc.ObjectAdapter[T_co, typing.Union[T_co, str]],
		typing.Generic[T_co]
):
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
	__slots__ = ()
	
	FORWARD_CONTAINS = True
	FORWARD_GET_ALL  = True
	FORWARD_STAT     = True
	
	
	async def _put(self, key: datastore.Key, value: datastore.abc.ReceiveChannel[T_co]) -> None:
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
	
	
	async def delete(self, key: datastore.Key) -> None:
		"""Removes the object named by `key`.
		   DirectoryTreeDatastore removes the directory entry.
		"""
		await super().delete(key)
		
		dir_key = key.parent.instance('directory')
		await super().directory_remove(dir_key, key, missing_ok=True)
	
	
	async def query(self, query: datastore.Query) -> datastore.Cursor:
		"""Returns objects matching criteria expressed in `query`.
		DirectoryTreeDatastore uses directory entries.
		"""
		return query(super().directory_read(query.key))
