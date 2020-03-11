import copy
import errno
import io
import json
import os
import pathlib
import stat as stat_
import tempfile
import typing

import trio

import datastore
import datastore.abc
import datastore.util

from .util import exchange, rename_noreplace, statx

T = typing.TypeVar("T")
if typing.TYPE_CHECKING:
	os_PathLike_str = os.PathLike[str]
	from typing_extensions import Literal as typing_Literal
	from typing_extensions import TypedDict as typing_TypedDict
	typing_Literal_True = typing_Literal[True]
	typing_Literal_False = typing_Literal[False]
else:
	os_PathLike_str = os.PathLike
	if hasattr(typing, "Literal"):
		from typing import Literal as typing_Literal
		typing_Literal_True = typing.Literal[True]
		typing_Literal_False = typing.Literal[False]
	else:
		from typing import Union as typing_Literal
		typing_Literal_True = typing_Literal_False = bool
	if hasattr(typing, "TypedDict"):
		from typing import TypedDict as typing_TypedDict
	else:
		def typing_TypedDict(*args, **kwargs) -> typing.Type[dict]:
			return dict

# Make default buffer larger to try to compensate for the thread switching overhead
DEFAULT_BUFFER_SIZE = io.DEFAULT_BUFFER_SIZE * 10

DEFAULT_STATS_KEY = datastore.Key("diskUsage.cache")


async def run_blocking_intr(func: typing.Callable[..., T], *args: typing.Any,
                            **kwargs: typing.Any) -> T:
	"""Short form for :func:`trio.run_sync_in_worker_thread`"""
	def callback() -> T:
		return func(*args, **kwargs)
	return typing.cast(T, await trio.to_thread.run_sync(callback, cancellable=True))


async def run_blocking_nointr(func: typing.Callable[..., T], *args: typing.Any,
                              **kwargs: typing.Any) -> T:
	"""Short form for :func:`trio.run_sync_in_worker_thread`"""
	def callback() -> T:
		return func(*args, **kwargs)
	return typing.cast(T, await trio.to_thread.run_sync(callback, cancellable=False))


def move_to_tempfile_sync(
		src: typing.Union[os_PathLike_str, str], *,
		wait_for_src: bool = False,
		suffix: str = "",
		prefix: str = "",
		dir: typing.Optional[typing.Union[os_PathLike_str, str]] = None
) -> pathlib.Path:
	"""Moves file to temporary file location
	
	Similar to the :func:`tempfile.mkstemp` function, but moves an existing
	source file rather then creating a new one."""
	for _ in range(tempfile.TMP_MAX):
		tmp = pathlib.Path(tempfile.mktemp(suffix=suffix, prefix=prefix, dir=dir))
		
		last_exc: BaseException
		try:
			rename_noreplace.rename_noreplace(src, tmp)
			return tmp  # Success
		except FileExistsError:
			pass  # Target file was created in the meantime
		except FileNotFoundError as exc:
			last_exc = exc
			if wait_for_src:
				# Somebody else moved the file out already, give
				# them extra time to move in the replacement
				#
				# If they died, those stats will be gone however…
				os.sched_yield()
			else:
				raise
	
	raise last_exc from last_exc


async def move_to_tempfile(
		src: typing.Union[os_PathLike_str, str], *,
		wait_for_src: bool = False,
		suffix: str = "",
		prefix: str = "",
		dir: typing.Optional[typing.Union[os_PathLike_str, str]] = None
) -> pathlib.Path:
	return await run_blocking_nointr(move_to_tempfile_sync, src, wait_for_src, suffix, prefix, dir)


@datastore.util.awaitable_to_context_manager
async def make_named_tempfile(*args: typing.Any, **kwargs: typing.Any) \
      -> 'trio._file_io.AsyncIOWrapper':
	return typing.cast(
		'trio._file_io.AsyncIOWrapper',
		trio.wrap_file(await run_blocking_nointr(tempfile.NamedTemporaryFile, *args, **kwargs))
	)


stat_result_t = typing.Union[os.stat_result, statx.stat_result]
stat_kwargs_t = typing_TypedDict("stat_kwargs_t", {
	"size":  int,
	"atime": float,
	"mtime": float,
	"btime": typing.Optional[float],
})


class FileReader(datastore.abc.ReceiveStream):
	__slots__ = ("_file")
	
	_file: 'trio._file_io.AsyncIOWrapper'
	
	
	def __init__(self, file: 'trio._file_io.AsyncIOWrapper', **kwargs: typing.Any):
		self._file = file
		
		super().__init__(**kwargs)
	
	
	async def receive_some(self, max_bytes: typing.Optional[int] = None) -> bytes:
		buf: bytes
		if max_bytes:
			buf = await self._file.read(max_bytes)
		else:
			buf = await self._file.read(DEFAULT_BUFFER_SIZE)
		
		if len(buf) == 0:
			await self.aclose()
		
		return buf
	
	
	async def aclose(self) -> None:
		await self._file.aclose()
	
	
	@staticmethod
	def stat_result_to_kwargs(stat: stat_result_t) -> stat_kwargs_t:
		result: stat_kwargs_t = {
			"size":  stat.st_size,
			"atime": stat.st_atime,
			"mtime": stat.st_mtime,
			"btime": None,
		}
		
		if stat.st_atime_ns:
			result["atime"] = stat.st_atime_ns / 1_000_000_000
		if stat.st_mtime_ns:
			result["mtime"] = stat.st_mtime_ns / 1_000_000_000
		
		# Finding btime from stat is tricky and platform dependant
		st_birthtime_ns: typing.Optional[int] = getattr(stat, "st_birthtime_ns", None)
		if st_birthtime_ns:
			# Linux with statx patch exposes this
			result["btime"] = st_birthtime_ns / 1_000_000_000
		elif hasattr(stat, "st_birthtime") and stat.st_birthtime:
			# FreeBSD/macOS has this field
			result["btime"] = stat.st_birthtime
		elif os.name == "nt":  # Windows stores btime as ctime
			result["btime"] = stat.st_ctime
			if stat.st_ctime_ns:
				result["btime"] = stat.st_ctime_ns / 1_000_000_000
		
		return result
	
	
	@classmethod
	async def from_path(cls, filepath: typing.Union[str, bytes, os_PathLike_str]) -> 'FileReader':
		# Open file
		file = await trio.open_file(filepath, "rb")
		try:
			# Query file stat data
			stat = await run_blocking_intr(statx.stat, file.fileno())
			
			return cls(file, **cls.stat_result_to_kwargs(stat))
		except BaseException:
			await file.aclose()
			raise


accuracy_t = typing_Literal["unknown", "initial-exact", "initial-approximate", "initial-timed-out"]


ACCURACY_INTERAL_TO_METADATA: typing.Dict[accuracy_t, datastore.typing.accuracy_t] = {
	"unknown": "unknown",
	"initial-exact": "exact",
	"initial-approximate": "approximate",
	"initial-timed-out": "lower-bound",
}


# work around GH/mypy/mypy#731: no recursive structural types yet
JSONPrimitive = typing.Union[str, int, bool, None]
JSONType = typing.Union[JSONPrimitive, 'JSONList', 'JSONDict']


class JSONList(typing.List[JSONType]):
    pass


class JSONDict(typing.Dict[str, JSONType]):
    pass


stats_json_t = typing_TypedDict("stats_json_t", {
	"diskUsage": int,
	"accuracy":  accuracy_t,
	"canMerge":  bool,
	"mtime":     int,  # This item is optional
}, total=False)


class Stats:
	disk_usage: int = 0
	accuracy: accuracy_t = "unknown"
	can_merge: bool = True
	mtime_ns: int = -1
	
	def __repr__(self) -> str:
		return (f"{self.__class__.__qualname__}(disk_usage={self.disk_usage!r}, "
		        f"accuracy={self.accuracy!r}, can_merge={self.can_merge!r}, "
		        f"mtime_ns={self.mtime_ns!r})")
	
	def copy(self) -> 'Stats':
		return copy.copy(self)
	
	@classmethod
	def from_json(cls, obj: JSONDict) -> 'Stats':
		self = cls()
		self.disk_usage = int(typing.cast(int, obj.pop("diskUsage", self.disk_usage)))
		self.accuracy   = typing.cast(accuracy_t, str(obj.pop("accuracy", self.accuracy)))
		self.can_merge  = bool(typing.cast(bool, obj.pop("canMerge", False)))
		self.mtime_ns   = int(typing.cast(int, obj.pop("mtime", self.mtime_ns)))
		return self
	
	def to_json(self, mtime: bool = False) -> stats_json_t:
		result: stats_json_t = {
			"diskUsage": self.disk_usage,
			"accuracy": self.accuracy,
			"canMerge": self.can_merge,
		}
		if mtime:
			result["mtime"] = self.mtime_ns
		return result
	
	def merge(self, base: 'Stats', remote: 'Stats') -> None:
		self.disk_usage += remote.disk_usage - base.disk_usage
		self.accuracy = remote.accuracy


def check_dir_empty_sync(path: typing.Union[os_PathLike_str, str]) -> bool:
	"""Synchroniously checks whether the given directory is empty."""
	with os.scandir(path) as scanner:
		for dent in scanner:
			if dent.name not in (".", ".."):
				return False
	return True


class DummyLock:
	__slots__ = ()
	
	async def acquire(self) -> None:
		pass
	
	def acquire_nowait(self) -> typing_Literal_True:
		return True
	
	def release(self) -> None:
		pass
	
	async def __aenter__(self) -> 'DummyLock':
		return self
	
	async def __aexit__(self, *args: typing.Any) -> bool:
		return False
	
	def locked(self) -> typing_Literal_False:
		return False


class FileSystemDatastore(datastore.abc.BinaryDatastore):
	"""Simple flat-file datastore.

	FileSystemDatastore will store objects in independent files in the host's
	filesystem. The FileSystemDatastore is initialized with a `root` path, under
	which to store all objects. Each object will be stored under its own file:
	`root`/`key`.data

	The `key` portion also replaces namespace parameter delimiters (:) with
	slashes, creating several nested directories. For example, storing objects
	under `root` path '/data' with the following keys::

		Key('/Comedy:MontyPython/Actor:JohnCleese')
		Key('/Comedy:MontyPython/Sketch:ArgumentClinic')
		Key('/Comedy:MontyPython/Sketch:CheeseShop')
		Key('/Comedy:MontyPython/Sketch:CheeseShop/Character:Mousebender')

	will yield the file structure::

		/data/Comedy/MontyPython/Actor/JohnCleese.data
		/data/Comedy/MontyPython/Sketch/ArgumentClinic.data
		/data/Comedy/MontyPython/Sketch/CheeseShop.data
		/data/Comedy/MontyPython/Sketch/CheeseShop/Character/Mousebender.data

	Implementation Notes:

		Using the `.data` extension gets around the ambiguity of having both a
		`CheeseShop` object and directory::

			/data/Comedy/MontyPython/Sketch/CheeseShop.data
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
	
	_stats: typing.Optional[Stats] = None
	_stats_lock: typing.Union[trio.Lock, DummyLock] = DummyLock()
	_stats_prev: typing.Optional[Stats] = None
	_stats_orig: typing.Optional[Stats] = None
	
	case_sensitive: bool
	object_extension: str = ".data"
	stats_key: datastore.Key
	remove_empty: bool
	root_path: pathlib.PurePath
	
	
	@classmethod
	@datastore.util.awaitable_to_context_manager
	async def create(cls, root: typing.Union[os_PathLike_str, str], *,
	                 case_sensitive: bool = True, remove_empty: bool = True,
	                 stats: bool = False, stats_key: datastore.Key = DEFAULT_STATS_KEY
	) -> 'FileSystemDatastore':
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
		stats
			Track summary statistics about the contents of the datastore,
			currently only the total apparent size of all files on the disk is
			tracked
		stats_key
			The key/filepath at which to persist statistics between runs
		"""
		if not root:
			raise ValueError('root path must not be empty (use \'.\' for current directory)')
		
		# Ensure target directory exists
		await trio.Path(root).mkdir(parents=True, exist_ok=True)
		
		# Create instance
		self = cls(_create_call=True)
		
		# Do the usual constructor stuff
		self.root_path = pathlib.PurePath(root)
		self.case_sensitive = bool(case_sensitive)
		self.remove_empty = bool(remove_empty)
		self.stats_key = datastore.Key(stats_key)
		
		# Enable stats processing
		if stats:
			self._stats_lock = trio.Lock()
			await self._init_stats()
		
		return self
	
	
	def __init__(self, *, _create_call: bool = False):
		assert _create_call, "Use FileSystemDatastore.create(…) for instance creation"
	
	
	@property
	def stats(self) -> bool:
		return bool(self._stats)
	
	
	async def _init_stats(self) -> None:
		# Start fresh
		self._stats = Stats()
		
		# Try to read existing stats file
		path = trio.Path(self.object_path(self.stats_key))
		try:
			async with await path.open() as stats_file:
				self._stats = Stats.from_json(json.loads(await stats_file.read()))
				self._stats.mtime_ns = (await run_blocking_intr(os.fstat, stats_file.fileno())).st_mtime_ns
		except FileNotFoundError:
			# At least set an appropriate accuracy value if there are no files yet
			is_empty = await run_blocking_intr(check_dir_empty_sync, self.root_path)
			if is_empty:
				self._stats.disk_usage = 0
				self._stats.accuracy   = "initial-exact"
			else:
				pass  #XXX: We could call `os.walk` here to get the initial size estimate…
		
		self._stats_prev = self._stats.copy()
		
		# This will synchronize our current state with the one on the disk
		await self._flush_stats(expect_file=False)
	
	
	def _flush_stats_sync(
			self, write_restore_file: bool = False, expect_file: bool = True
	) -> None:
		"""Update the filesystem stats on the disk in a way that is safe for
		concurrent access
		
		This function performs lots of synchronous I/O, call it from an I/O
		thread only and ensure you hold :attr:`_stats_lock` while doing so!
		
		The general proceedure being a follows:
		
		1. Write the new/current stats to a temporary file
		2. Atomically exchange ``diskUsage.cache`` and the temporary file
		
			* *Fallback if atomic exchange is not available on the host OS*:
			  Move ``diskUsage.cache`` to new location and non-replacingly
			  rename temporary file to ``diskUsage.cache`` (set a flag if the
			  target file already exists by the time we try to move our's in)
			* The primary issue with this variant is that the stats file will
			  be gone (moved or unlinked) for a splitsecond before the new one
			  has been moved in. If the program crashes between these two
			  actions, we will loose some stats.
		
		3. Compare mtime of the moved/previous ``diskUsage.cache`` file to the
		   file's value on startup
		   
			* If they do not match update the expected value and mtime from the
			  moved file and retry from step 1
			* In effect this constitutes a 3-way merge with our expected value
			  as the base, the read value as the “remote” (cause somebody else
			  must have written it) and our new value as “local”
		
		4. Check flag set by the exchange fallback code and continue as in
		   step 3 if it is set (using the current ``diskUsage.cache`` file
		   as the “remote”)
		5. Success
		"""
		assert self._stats is not None
		assert self._stats_prev is not None
		assert self._stats_lock.locked()
		
		unlink_paths: typing.List[pathlib.Path] = []
		try:
			path = pathlib.Path(self.object_path(self.stats_key))
			path_restore = pathlib.Path(str(path) + "-restore")
			path_dir     = path.parent
			path_prefix  = f".{path.name}.tmp-"
			
			while True:
				# Read restore file if it exists (works around non-cooperating
				# implementations just overwriting everything blindly)
				try:
					path_restore_tmp = move_to_tempfile_sync(
						path_restore, dir=path_dir, prefix=path_prefix
					)
				except FileNotFoundError:
					pass  # No restore file currently written
				else:
					unlink_paths.append(path_restore_tmp)
					
					restore_data  = json.loads(path_restore_tmp.read_bytes())
					restore_orig  = Stats.from_json(restore_data.get("orig", {}))
					restore_local = Stats.from_json(restore_data.get("local", {}))
					
					try:
						with path.open() as remote_file:
							restore_remote = Stats.from_json(json.loads(remote_file.read()))
							restore_remote.mtime_ns = os.fstat(remote_file.fileno()).st_mtime_ns
						
						# Apply delta from current file to restore data
						if not restore_remote.can_merge:
							restore_local.merge(restore_orig, restore_remote)
							self._stats_orig = restore_remote
						else:
							self._stats_orig = restore_orig
					except FileNotFoundError:
						pass
					
					new_stats = restore_local.copy()
					# Apply delta from our value to restore data
					new_stats.merge(self._stats_prev, self._stats)
					
					self._stats = new_stats
					self._stats_prev = restore_local
				
				
				with tempfile.NamedTemporaryFile(
					mode     = "w",
					encoding = "utf-8",
					dir      = path_dir,
					prefix   = path_prefix,
					delete   = False
				) as new_file:
					# Remember the file's path so that we may unlink it when
					# the time comes
					new_path = pathlib.Path(self.root_path / new_file.name)
					unlink_paths.append(new_path)
					
					# Write data
					new_file.write(json.dumps(self._stats.to_json()))
				
				# Also remember the mtime after writing for later collision detection
				new_path_mtime_ns: int = new_path.stat().st_mtime_ns
				
				need_move_retry: bool = False
				old_path: typing.Optional[pathlib.Path] = None
				try:
					# Atomically exchange temporary and production file
					exchange.exchange(path, new_path)
					#  – The temporary file now contains the previous contents
					old_path = new_path
					#  – The target file has now been updated
					self._stats.mtime_ns = new_path_mtime_ns
				except (FileNotFoundError, AttributeError, NotImplementedError) as exc:
					if not isinstance(exc, FileNotFoundError):
						# Fallback code in case atomic exchange is not available
						#
						# The primary issue with this variant is that the stats file
						# will be gone (moved or unlinked) for a splitsecond before
						# the new one has been moved in. If the program crashes
						# between these two actions, we loose the stats.
						
						# Move target file to temporary location
						try:
							old_path = move_to_tempfile_sync(
								path, wait_for_src=expect_file, dir=path_dir, prefix=path_prefix
							)
						except FileNotFoundError:
							old_path = None
					
					# This code applies both to the case of atomic exchange not being
					# available and the target file being non-existent
					try:
						# Move created temporary file with our data to
						# production file location
						rename_noreplace.rename_noreplace(new_path, path)
						self._stats.mtime_ns = new_path_mtime_ns
					except FileExistsError:
						# Somebody else beat us to it, we'll need to incorporate
						# their changes and retry
						expect_file = True
						need_move_retry = True
				
				if old_path is not None:
					# Compare timestamp of swapped out file to the expected value
					old_path_mtime_ns = old_path.stat().st_mtime_ns
					
					if old_path_mtime_ns != self._stats_prev.mtime_ns:
						old_stats = Stats.from_json(json.loads(old_path.read_bytes()))
						old_stats.mtime_ns = old_path_mtime_ns
						cur_stats = self._stats.copy()
						
						# Apply delta to current stats (aka perform a “merge”)
						if not old_stats.can_merge:
							if self._stats_orig is not None:
								self._stats.merge(self._stats_orig, old_stats)
							self._stats_orig = old_stats
						else:
							self._stats.merge(self._stats_prev, old_stats)
						
						# Expect the stats data we just swapped in from now on
						# and request a retry soon
						self._stats_prev = cur_stats
						self._stats_prev.mtime_ns = new_path_mtime_ns
						continue
				
				if need_move_retry:
					old_stats = Stats.from_json(json.loads(path.read_bytes()))
					cur_stats = self._stats
					
					# Apply delta to current stats (aka perform a “merge”)
					self._stats.merge(self._stats_prev, old_stats)
					
					# Expect the stats data we just swapped in from now on and
					# request a retry soon
					self._stats_prev = cur_stats
					continue
				
				if write_restore_file:
					assert self._stats_prev is not None
					assert self._stats_orig is not None
					
					with tempfile.NamedTemporaryFile(
						mode     = "w",
						encoding = "utf-8",
						dir      = path_dir,
						prefix   = path_prefix,
						delete   = False
					) as new_restore_file:
						# Remember the file's path so that we may unlink it when
						# the time comes
						new_restore_path = pathlib.Path(self.root_path / new_restore_file.name)
						unlink_paths.append(new_restore_path)
						
						# Write data
						new_restore_file.write(json.dumps({
							"local": self._stats.to_json(mtime=True),
							"prev":  self._stats_prev.to_json(mtime=True),
							"orig":  self._stats_orig.to_json(mtime=True),
						}))
					
					try:
						# Move created temporary file with our data to
						# production file location
						rename_noreplace.rename_noreplace(new_restore_path, path_restore)
					except FileExistsError:
						# Somebody else beat us to it, we'll need to incorporate
						# their changes and retry
						continue
				
				# Remember current snapshot as new base and return
				self._stats_prev = self._stats.copy()
				if self._stats_orig is None:
					self._stats_orig = self._stats.copy()
				return
		finally:
			# Clean up all temporary files created during the above
			errors: typing.List[Exception] = []
			for unlink_path in unlink_paths:
				try:
					unlink_path.unlink()
				except FileNotFoundError:
					pass
				except Exception as exc:
					errors.append(exc)
			if errors:
				if len(unlink_paths) == 1:
					raise errors[0]
				raise trio.MultiError(errors)
	
	
	async def _flush_stats(
			self, write_restore_file: bool = False, expect_file: bool = True
	) -> None:
		"""Flush the current stats to disk, handling potential write conflicts
		in the process
		
		See :meth:`_flush_stats_sync` for all nitty gritty details on how
		conflict resolution is performed.
		"""
		if self._stats is None:
			return  # Nothing to do
		
		async with self._stats_lock:  # type: ignore[union-attr]
			await run_blocking_nointr(self._flush_stats_sync, write_restore_file, expect_file)
	
	
	async def flush(self) -> None:
		await self._flush_stats()
	
	
	async def aclose(self) -> None:
		await self._flush_stats(write_restore_file=True)
	
	
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

	def object_path(self, key: datastore.Key) -> pathlib.PurePath:
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
			return typing.cast(bytes, await path.read_bytes())
		except FileNotFoundError as exc:
			raise KeyError(key) from exc
		except IsADirectoryError as exc:
			# Should hopefully only happen if `object_extension` is `""`
			raise RuntimeError(f"Key '{key}' names a subtree, not a value") from exc
	
	
	def _put_replace_sync(
			self,
			source: typing.Union[os_PathLike_str, str],
			target: typing.Union[os_PathLike_str, str]
	) -> None:
		source = pathlib.Path(source)
		target = pathlib.Path(target)
		if self._stats is None:
			source.replace(target)
			return
		
		assert self._stats_lock.locked()
		
		target_dir    = target.parent
		target_prefix = f".{target.name}.tmp-"
		try:
			# Atomically exchange temporary and production file
			# (only works on Linux and macOS, but not *BSD and Windows, unfortunately)
			exchange.exchange(source, target)
			
			# Do bookkeeping of the source file (now the former target file) that
			# we're going to remove
			self._stats.disk_usage -= os.stat(source).st_size
			os.unlink(source)
		except (FileNotFoundError, AttributeError, NotImplementedError):
			# Fallback code in case atomic exchange is not available or the target
			# file was removed in the meantime
			
			while True:
				try:
					# Move target file to temporary location
					temp_path = move_to_tempfile_sync(target, dir=target_dir, prefix=target_prefix)
				except FileNotFoundError:
					pass
				else:
					# Do bookkeeping of this file before removing it
					self._stats.disk_usage -= temp_path.stat().st_size
					temp_path.unlink()
				
				try:
					# Move created temporary file with our data to
					# production file location
					rename_noreplace.rename_noreplace(source, target)
					
					break
				except FileExistsError:
					continue
	
	async def _receive_and_write(self, file: 'trio._file_io.AsyncIOWrapper',
	                             value: datastore.abc.ReceiveStream) -> None:
		chunk = await value.receive_some(DEFAULT_BUFFER_SIZE)
		while chunk:
			# Do bookkeeping
			if self._stats is not None:
				async with self._stats_lock:  # type: ignore[union-attr]
					self._stats.disk_usage += len(chunk)
			
			await file.write(chunk)
			
			chunk = await value.receive_some(DEFAULT_BUFFER_SIZE)
	
	async def _put(self, key: datastore.Key, value: datastore.abc.ReceiveStream, *,
	               create: bool, replace: bool) -> None:
		"""Stores or replaces the data named by `key` with `value`
		
		Arguments
		---------
		key
			Key naming the binary data slot to store at
		value
			Some stream yielding the data to store
		create
			Create the given key if it does not exist?
		replace
			Replace the given key if it does exist?
		
		Raises
		------
		KeyError
			The given `key` doesn't exist and `create` is not ``True``.
		KeyError
			The given `key` already exists and `replace` is not ``True``.
		RuntimeError
			The given `key` names a subtree, not a value OR the contains a
			value item as part of the key path
		"""
		assert create or replace
		
		path = trio.Path(self.object_path(key))
		path_dir    = path.parent
		path_prefix = f".{path.name}.tmp-"
		
		if create:
			# Ensure containing directory exists
			try:
				await path_dir.mkdir(parents=True, exist_ok=True)
			except FileExistsError as exc:
				# Should hopefully only happen if `object_extension` is `""`
				raise RuntimeError(f"Key '{key}' requires containing directory "
				                   f"'{path_dir}' to not be a value") from exc
		else:
			# Remove existing key with accounting and continue as if create
			# were set to `True`
			#
			# The reason we do this, rather then writing the contents to a
			# temporary file and doing a non-replacing rename afterwards, is
			# so that the exception generated by violating this constraint
			# is raised when opening the file, rather then when closing it
			await self.delete(key)
		
		try:
			if self._stats is None:
				# Since accounting doesn't matter in this case, there is no
				# issue with getting the stats wrong (see next section) and
				# we can directly write to the target file if it does not
				# exist; if it does exist however there is the risk of the
				# file being opened twice by two different processes, likely
				# corrupting its contents, so we still need to use a temporary
				# file to avoid this in that case.
				async with await path.open("xb") as file:
					await self._receive_and_write(file, value)
				return
			elif not replace:
				# Create target file if it does not exist, but don't write to
				# it to ensure that we don't end up with broken accounting if
				# a concurrent process deletes/replaces it while we are still
				# writing its contents.
				async with await path.open("xb") as file:
					pass
		except IsADirectoryError as exc:
			# Should only happen if `object_extension` is `""`
			raise RuntimeError(f"Key '{key}' names a subtree, not a value") from exc
		except FileExistsError as exc:
			# Error out if the target file already exists …
			if not replace:
				raise KeyError(key) from exc
		
		# … unless `replace` is True, then write to a temporary file instead
		#   and later move it into place, overriding the previous file
		async with make_named_tempfile(
			mode="wb", dir=path_dir, prefix=path_prefix, delete=False
		) as temp_file:
			await self._receive_and_write(temp_file, value)
		
		async with self._stats_lock:  # type: ignore[union-attr]
			await run_blocking_nointr(self._put_replace_sync, temp_file.name, path)
	
	
	def _delete_sync(self, path: typing.Union[os_PathLike_str, str]) -> None:
		path = pathlib.Path(path)
		if self._stats is None:
			path.unlink()
			return
		
		assert self._stats_lock.locked()
		
		path_dir    = path.parent
		path_prefix = f".{path.name}.tmp-"
		
		try:
			temp_path = move_to_tempfile_sync(path, dir=path_dir, prefix=path_prefix)
		except FileNotFoundError:
			raise  # Let this propagate to signal that the file didn't exist
		else:
			self._stats.disk_usage -= temp_path.stat().st_size
			temp_path.unlink()
	

	async def delete(self, key: datastore.Key) -> None:
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
			async with self._stats_lock:  # type: ignore[union-attr]
				await run_blocking_nointr(self._delete_sync, path)
		except FileNotFoundError as exc:
			raise KeyError(key) from exc
		
		# Try to remove parent directories if they are empty
		if self.remove_empty:
			try:
				parent = path.parent
				# Attempt to remove all parent directories as long as the
				# parent directory is:
				#  * … a sub-directory of `self.root_path` – checking whether
				#    the path of that directory starts with `{self.root_path}/`.
				#  * … not the same directory again – to ensure that pathlib's
				#    special `Path(".").parent == Path(".")` behaviour doesn't
				#    bite us. (This check may be unnecessary / overly pedantic…)
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
	
	
	async def stat(self, key: datastore.Key) -> datastore.util.StreamMetadata:
		"""Returns the metadata of the data named by key, or raises KeyError otherwise
		
		Arguments
		---------
		key
			Key naming the data to retrieve

		Raises
		------
		KeyError
			The requested file data did not exist
		RuntimeError
			The given ``key`` names a subtree, not a value
		"""
		path = self.object_path(key)
		try:
			stat = await run_blocking_intr(statx.stat, path)
			if stat_.S_ISDIR(stat.st_mode):
				# Should hopefully only happen if `object_extension` is `""`
				raise RuntimeError(f"Key '{key}' names a subtree, not a value")
			
			return datastore.util.StreamMetadata(**FileReader.stat_result_to_kwargs(stat))
		except FileNotFoundError as exc:
			raise KeyError(key) from exc
	
	
	def datastore_stats(self, selector: datastore.Key = None, *, _seen: typing.Set[int] = None) \
	    -> datastore.util.DatastoreMetadata:
		"""Returns available metadata of this filesystem
		
		Unless stats are enabled this will not return any useful values.
		
		Arguments
		---------
		selector
			Ignored by backing datastores
		"""
		if self._stats is None:
			return datastore.util.DatastoreMetadata()
		
		return datastore.util.DatastoreMetadata(
			size = self._stats.disk_usage,
			size_accuracy = ACCURACY_INTERAL_TO_METADATA[self._stats.accuracy],
		)
