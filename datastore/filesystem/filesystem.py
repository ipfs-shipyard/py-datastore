import errno
import io
import os
import pathlib
import typing

import trio

import datastore
import datastore.abc

from .util import statx

# Make default buffer larger to try to compensate for the thread switching overhead
DEFAULT_BUFFER_SIZE = io.DEFAULT_BUFFER_SIZE * 10


stat_result_t = typing.Union[os.stat_result, statx.stat_result]


class FileReader(datastore.abc.ReceiveStream):
	__slots__ = ("_file")
	
	_file: 'trio._file_io.AsyncIOWrapper'
	
	
	def __init__(self, file: 'trio._file_io.AsyncIOWrapper', stat: stat_result_t):
		super().__init__()
		
		self._file = file
		
		self.size  = stat.st_size
		self.atime = stat.st_atime
		self.mtime = stat.st_mtime
		if stat.st_atime_ns:
			self.atime = stat.st_atime_ns / 1_000_000_000
		if stat.st_mtime_ns:
			self.mtime = stat.st_mtime_ns / 1_000_000_000
		
		# Finding btime from stat is tricky and platform dependant
		st_birthtime_ns: typing.Optional[int] = getattr(stat, "st_birthtime_ns", None)
		if st_birthtime_ns:
			# Linux with statx patch exposes this
			self.btime = st_birthtime_ns / 1_000_000_000
		elif hasattr(stat, "st_birthtime") and stat.st_birthtime:
			# FreeBSD/macOS has this field
			self.btime = stat.st_birthtime
		elif os.name == "nt":  # Windows stores btime as ctime
			self.btime = stat.st_ctime
			if stat.st_ctime_ns:
				self.btime = stat.st_ctime_ns / 1_000_000_000
	
	
	async def receive_some(self, max_bytes: typing.Optional[int] = None):
		if max_bytes:
			buf = await self._file.read(max_bytes)
		else:
			buf = await self._file.read(DEFAULT_BUFFER_SIZE)
		
		if len(buf) == 0:
			await self.aclose()
		
		return buf
	
	
	async def aclose(self) -> None:
		await self._file.aclose()
	
	
	@classmethod
	async def from_path(cls, filepath: typing.Union[str, bytes, os.PathLike]):
		# Open file
		file = await trio.open_file(filepath, "rb")
		try:
			# Query file stat data
			stat = await trio.run_sync_in_worker_thread(statx.stat, file.fileno(), cancellable=True)
			
			return cls(file, stat)
		except BaseException:
			await file.aclose()
			raise


class FileSystemDatastore(datastore.abc.BinaryDatastore):
	"""Simple flat-file datastore.

	FileSystemDatastore will store objects in independent files in the host's
	filesystem. The FileSystemDatastore is initialized with a `root` path, under
	which to store all objects. Each object will be stored under its own file:
	`root`/`key`.obj

	The `key` portion also replaces namespace parameter delimiters (:) with
	slashes, creating several nested directories. For example, storing objects
	under `root` path '/data' with the following keys::

		Key('/Comedy:MontyPython/Actor:JohnCleese')
		Key('/Comedy:MontyPython/Sketch:ArgumentClinic')
		Key('/Comedy:MontyPython/Sketch:CheeseShop')
		Key('/Comedy:MontyPython/Sketch:CheeseShop/Character:Mousebender')

	will yield the file structure::

		/data/Comedy/MontyPython/Actor/JohnCleese.obj
		/data/Comedy/MontyPython/Sketch/ArgumentClinic.obj
		/data/Comedy/MontyPython/Sketch/CheeseShop.obj
		/data/Comedy/MontyPython/Sketch/CheeseShop/Character/Mousebender.obj

	Implementation Notes:

		Using the `.obj` extension gets around the ambiguity of having both a
		`CheeseShop` object and directory::

			/data/Comedy/MontyPython/Sketch/CheeseShop.obj
			/data/Comedy/MontyPython/Sketch/CheeseShop/


	Hello World:

		>>> import datastore.filesystem
		>>>
		>>> ds = datastore.filesystem.FileSystemDatastore('/tmp/.test_datastore')
		>>>
		>>> hello = datastore.Key('hello')
		>>> ds.put(hello, 'world')
		>>> ds.contains(hello)
		True
		>>> ds.get(hello)
		'world'
		>>> ds.delete(hello)
		>>> ds.contains(hello)
		False
		>>> ds.get(hello)
		None

	"""

	case_sensitive: bool
	object_extension: str
	remove_empty: bool
	root_path: pathlib.PurePath

	def __init__(self, root: typing.Union[os.PathLike, str], *,
	             case_sensitive: bool = True, remove_empty: bool = True):
		"""Initialize the datastore with given root directory `root`.

		Arguments
		---------
		root
			A path at which to mount this filesystem datastore.
		case_sensitive
			Keep case of all path items (True) or lower case them (False)
			before passing them to the OS. Note that, if the underlying
			file system is case-insensitive this option will not prevent
			conflicts between paths that differ in case only.
		remove_empty
			Attempt to remove empty directories in the underlying file
			system. While this is enabled every successful delete operation
			will be followed by at least one extra context switch to invoke
			the `rmdir` system call.
		"""
		if not root:
			raise ValueError('root path must not be empty (use \'.\' for current directory)')
		
		root = pathlib.Path(root)
		root.mkdir(parents=True, exist_ok=True)

		self.object_extension = '.obj'
		self.root_path = root
		self.case_sensitive = bool(case_sensitive)
		self.remove_empty = bool(remove_empty)
	
	
	# object paths
	
	
	def relative_path(self, key: datastore.Key) -> pathlib.PurePath:
		"""Returns the relative path for given `key`"""
		skey = str(key)  # stringify
		skey = skey.replace(':', '/')  # turn namespace delimiters into slashes
		skey = skey[1:]  # remove first slash (absolute)
		if not self.case_sensitive:
			skey = skey.lower()  # coerce to lowercase
		return pathlib.PurePath(skey)

	def path(self, key: datastore.Key) -> pathlib.PurePath:
		"""Returns the `path` for given `key`"""
		return self.root_path / self.relative_path(key)

	def relative_object_path(self, key: datastore.Key) -> pathlib.PurePath:
		"""Returns the relative path for object pointed by `key`."""
		return self.relative_path(key).with_suffix(self.object_extension)

	def object_path(self, key: datastore.Key):
		"""return the object path for `key`."""
		return self.root_path / self.relative_object_path(key)
	
	
	# Datastore implementation
	
	
	async def get(self, key: datastore.Key) -> datastore.abc.ReceiveStream:
		"""Returns the data named by key, or raises KeyError otherwise.
		
		It is suggested to read larger chunks of the returned stream to reduce
		the overhead for doing a context switch for each system call.
		
		Arguments
		---------
		key
			Key naming the data to retrieve

		Raises
		------
		KeyError
			The given object was not present in this datastore
		RuntimeError
			The given ``key`` names a subtree, not a value
		"""
		path = self.object_path(key)
		try:
			return await FileReader.from_path(path)
		except FileNotFoundError as exc:
			raise KeyError(key) from exc
		except IsADirectoryError as exc:
			# Should hopefully only happen if `object_extension` is `""`
			raise RuntimeError(f"Key '{key}' names a subtree, not a value") from exc
	
	
	async def get_all(self, key: datastore.Key) -> bytes:
		"""Returns all the data named by `key` at once or raises `KeyError`
		   otherwise
		
		This is an optimization over :meth:`get` for smaller files as it entails
		only one context switch to open, read and close the file, rather then
		several.
		
		Arguments
		---------
		key
			Key naming the data to retrieve

		Raises
		------
		KeyError
			The given object was not present in this datastore
		RuntimeError
			The given ``key`` names a subtree, not a value
		"""
		path = trio.Path(self.object_path(key))
		try:
			return await path.read_bytes()
		except FileNotFoundError as exc:
			raise KeyError(key) from exc
		except IsADirectoryError as exc:
			# Should hopefully only happen if `object_extension` is `""`
			raise RuntimeError(f"Key '{key}' names a subtree, not a value") from exc
	
	
	async def _put(self, key: datastore.Key, value: datastore.abc.ReceiveStream) -> None:
		"""Stores or replaces the data named by `key` with `value`
		
		Arguments
		---------
		key
			Key naming the binary data slot to store at
		value
			Some stream yielding the data to store
		
		Raises
		------
		RuntimeError
			The given ``key`` names a subtree, not a value OR the contains a
			value item as part of the key path
		"""
		path = trio.Path(self.object_path(key))
		
		# Ensure containing directory exists
		parent = path.parent
		try:
			await parent.mkdir(parents=True, exist_ok=True)
		except FileExistsError as exc:
			# Should hopefully only happen if `object_extension` is `""`
			raise RuntimeError(f"Key '{key}' requires containing directory "
			                   f"'{parent}' to not be a value") from exc
		
		try:
			async with await trio.open_file(path, "wb") as file:
				chunk = await value.receive_some(DEFAULT_BUFFER_SIZE)
				while chunk:
					await file.write(chunk)
					
					chunk = await value.receive_some(DEFAULT_BUFFER_SIZE)
		except IsADirectoryError as exc:
			# Should only happen if `object_extension` is `""`
			raise RuntimeError(f"Key '{key}' names a subtree, not a value") from exc
	

	async def delete(self, key: datastore.Key):
		"""Removes the data named by `key`
		
		Arguments
		---------
		key
			Key naming the binary data slot to remove
		
		Raises
		------
		KeyError
			The given object was not present in this datastore
		"""
		path = trio.Path(self.object_path(key))
		
		try:
			await path.unlink()
		except FileNotFoundError as exc:
			raise KeyError(key) from exc
		
		# Try to remove parent directories if they are empty
		if not self.remove_empty:
			return
		try:
			parent = path.parent
			# Attempt to remove all parent directories as long as the
			# parent directory is:
			#  * … a sub-directory of `self.root_path` – checking whether
			#    the path of that directory starts with `{self.root_path}/`.
			#  * … not the same directory again – to ensure that pathlib's
			#    special `Path(".").parent == Path(".")` behaviour doesn't
			#    bite us. (This check may be unecessary / overly pendantic…)
			# The loop is stopped when we either reach the root directory
			# or receive an `ENOTEMPTY` error indicating that we tried to
			# remove a directory that wasn't actually empty.
			while str(parent).startswith(str(self.root_path) + os.path.sep) \
			      and parent.parent != parent:
				await parent.rmdir()
				
				parent = parent.parent
		except OSError as exc:
			if exc.errno == errno.ENOTEMPTY:
				return
			raise
