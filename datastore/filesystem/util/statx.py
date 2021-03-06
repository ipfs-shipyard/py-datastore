"""An extended version of the standard :func:`os.stat` function that can use
the ``statx(2)`` system call on recent Linux to query a file's creation time.

If that system call is not available on the running version of Linux or the
wrapper is run on a different platform it will silently fall back to calling
:func:`os.stat` instead.
"""
import collections
import ctypes
import enum
import errno
import os
import sys
import typing

__all__ = (
	"Mask",
	
	"AT_FDCWD",
	"AT_SYMLINK_NOFOLLOW",
	"AT_REMOVEDIR",
	"AT_NO_AUTOMOUNT",
	"AT_EMPTY_PATH",
	
	"AT_STATX_SYNC_TYPE",
	"AT_STATX_SYNC_AS_STAT",
	"AT_STATX_FORCE_SYNC",
	"AT_STATX_DONT_SYNC",
	
	"struct_statx",
	"statx",
	"stat_result",
	"stat_result_t",
	"stat",
	"lstat",
	"fstat",
)


class Mask(enum.IntFlag):
	# Basic stats (stuff also part of `os.stat()`)
	TYPE        = 0x00000001  # Want/got stx_mode & S_IFMT
	MODE        = 0x00000002  # Want/got stx_mode & ~S_IFMT
	NLINK       = 0x00000004  # Want/got stx_nlink
	UID         = 0x00000008  # Want/got stx_uid
	GID         = 0x00000010  # Want/got stx_gid
	ATIME       = 0x00000020  # Want/got stx_atime
	MTIME       = 0x00000040  # Want/got stx_mtime
	CTIME       = 0x00000080  # Want/got stx_ctime
	INO         = 0x00000100  # Want/got stx_ino
	SIZE        = 0x00000200  # Want/got stx_size
	BLOCKS      = 0x00000400  # Want/got stx_blocks
	BASIC_STATS = 0x000007FF  # The stuff in the normal stat struct
	
	# Extensions
	BTIME       = 0x00000800  # Want/got stx_btime
	ALL         = 0x00000FFF  # All currently supported flags
	_RESERVED   = 0x80000000  # Reserved for future struct statx expansion


# Special FD for value for meaning “no FD”
AT_FDCWD = -100

# Path lookup flags applicable for `statx`
AT_SYMLINK_NOFOLLOW = 0x100  # Do not resolve symbolic links
AT_REMOVEDIR        = 0x200  # Remove directory instead of unlinking file
AT_NO_AUTOMOUNT     = 0x800  # Suppress terminal automount traversal
AT_EMPTY_PATH       = 0x1000  # Allow empty relative pathname

# Accuracy of timestamps required in case of network file systems
AT_STATX_SYNC_TYPE    = 0x6000  # Type of synchronisation required from statx():
AT_STATX_SYNC_AS_STAT = 0x0000  # - Do whatever stat() does
AT_STATX_FORCE_SYNC   = 0x2000  # - Force the attributes to be sync'd with the server
AT_STATX_DONT_SYNC    = 0x4000  # - Don't sync attributes with the server


class struct_statx_timestamp(ctypes.Structure):
	_fields_ = [
		# Base file attributes
		("tv_sec",     ctypes.c_uint64),
		("tv_nsec",    ctypes.c_uint32),
		("__reserved", ctypes.c_uint32),
	]
	
	tv_sec: int
	tv_nsec: int


class struct_statx(ctypes.Structure):
	_fields_ = [
		# Base file attributes
		("stx_mask",       ctypes.c_uint32),  # Python type: Mask
		("stx_blksize",    ctypes.c_uint32),
		("stx_attributes", ctypes.c_uint64),
		("stx_nlink",      ctypes.c_uint32),
		("stx_uid",        ctypes.c_uint32),
		("stx_gid",        ctypes.c_uint32),
		("stx_mode",       ctypes.c_uint16),
		("__spare0",       ctypes.c_uint16 * 1),
		("stx_ino",        ctypes.c_uint64),
		("stx_size",       ctypes.c_uint64),
		("stx_blocks",     ctypes.c_uint64),
		("stx_attributes_mask", ctypes.c_uint64),
		
		# Timestamps
		("stx_atime", struct_statx_timestamp),
		("stx_btime", struct_statx_timestamp),
		("stx_ctime", struct_statx_timestamp),
		("stx_mtime", struct_statx_timestamp),
		
		# Device ID (if device file)
		("stx_rdev_major", ctypes.c_uint32),
		("stx_rdev_minor", ctypes.c_uint32),
		("stx_dev_major",  ctypes.c_uint32),
		("stx_dev_minor",  ctypes.c_uint32),
		
		# Spare space
		("__spare2", ctypes.c_uint64 * 14),
	]
	
	stx_mask: Mask
	stx_blksize: int
	stx_attributes: int
	stx_nlink: int
	stx_uid: int
	stx_gid: int
	stx_mode: int
	stx_ino: int
	stx_size: int
	stx_blocks: int
	stx_attributes_mask: int
	
	# Timestamps
	stx_atime: struct_statx_timestamp
	stx_btime: struct_statx_timestamp
	stx_ctime: struct_statx_timestamp
	stx_mtime: struct_statx_timestamp
	
	# Device ID (if device file)
	stx_rdev_major: int
	stx_rdev_minor: int
	stx_dev_major: int
	stx_dev_minor: int


assert ctypes.sizeof(struct_statx) == 0x100



if sys.platform == "linux":
	_error: typing.Optional[NotImplementedError] = None
	_func: typing.Optional[typing.Any] = None
	
	try:
		# Try glibc first, falling back to any C library returned by `ldconfig`
		try:
			_libc = ctypes.CDLL("libc.so.6", use_errno=True)
		except OSError:
			import ctypes.util
			_libc_name = ctypes.util.find_library("c")
			if _libc_name is not None:
				_libc = ctypes.CDLL(_libc_name, use_errno=True)
			else:
				raise FileNotFoundError()
		
		try:
			_func = _libc.statx
			_func.argtypes = (
				ctypes.c_int,     # dirfd
				ctypes.c_char_p,  # pathname
				ctypes.c_int,     # flags
				ctypes.c_uint,    # mask
				ctypes.POINTER(struct_statx)
			)
		except AttributeError:  # Probably not GLibC 2.28+
			_error = NotImplementedError("statx: C library does not expose symbol 'statx'")
	except OSError:
		_error = NotImplementedError("statx: No C library found at name 'libc.so.6'")
	
	
	def statx(
			dirfd: int      = AT_FDCWD,
			pathname: bytes = b"",
			flags: int      = AT_STATX_SYNC_AS_STAT,
			mask: Mask      = Mask.BASIC_STATS
	) -> struct_statx:
		"""Low-level wrapper around the ``statx(2)`` Linux system call"""
		global _error
		if _error:
			raise _error
		assert _func
		
		statx_data = struct_statx()
		
		result = _func(dirfd, pathname, flags, mask, ctypes.byref(statx_data))
		if result < 0:
			if ctypes.get_errno() == errno.ENOSYS:  # Kernel does not support syscall
				_error = NotImplementedError("statx: System call not supported by this version of Linux")
				raise _error
			raise OSError(ctypes.get_errno(), os.strerror(ctypes.get_errno()))
		
		return statx_data



# We have to define our own `stat_result` here as there is no way to add fields
# to `os.stat_result` unless Python thinks they should be there
_stat_result = collections.namedtuple("stat_result", [
	# Standard attributes
	"st_mode",
	"st_ino",
	"st_dev",
	"st_nlink",
	"st_uid",
	"st_gid",
	"st_size",
	"st_atime",
	"st_mtime",
	"st_ctime",
	
	# Platform-dependant attributes
	"st_blksize",
	"st_blocks",
	"st_rdev",
	"st_flags",
	
	# High-precision timestamps
	"st_atime_ns",
	"st_mtime_ns",
	"st_ctime_ns",
	
	# Birthtime extension (otherwise only available on FreeBSD/macOS)
	"st_birthtime",
	"st_birthtime_ns"
], defaults=[None, None, None, None, None, None, None, None, None])


class stat_result(_stat_result):
	def __repr__(self) -> str:
		return (f"{self.__module__}.{type(self).__qualname__}("
		        f"st_mode={self.st_mode!r}, "
		        f"st_ino={self.st_ino!r}, "
		        f"st_dev={self.st_dev!r}, "
		        f"st_nlink={self.st_nlink!r}, "
		        f"st_uid={self.st_uid!r}, "
		        f"st_gid={self.st_gid!r}, "
		        f"st_size={self.st_size!r}, "
		        f"st_atime={self.st_atime!r}, "
		        f"st_mtime={self.st_mtime!r}, "
		        f"st_ctime={self.st_ctime!r})")


stat_result_t = typing.Union[os.stat_result, stat_result]
if typing.TYPE_CHECKING:
	path_t = typing.Union[int, str, bytes, os.PathLike[str], os.PathLike[bytes]]
else:
	path_t = typing.Union[int, str, bytes, os.PathLike]

_statx_available = "statx" in globals()


def stat(path: path_t, *, dir_fd: int = None, follow_symlinks: bool = True) -> stat_result_t:
	"""High-level wrapper around the ``statx(2)`` system call, that delegates
	to :func:`os.stat` on other platforms, but provides `st_birthtime` on Linux."""
	def ts_to_nstime(ts: struct_statx_timestamp) -> int:
		return ts.tv_sec * 1000_000_000 + ts.tv_nsec
	
	global _statx_available
	if _statx_available:
		try:
			stx_flags = AT_STATX_SYNC_AS_STAT
			
			if isinstance(path, int):
				stx_dirfd  = path
				stx_path   = b""
				stx_flags |= AT_EMPTY_PATH
			else:
				stx_dirfd = dir_fd if dir_fd is not None else AT_FDCWD
				stx_path  = os.fsencode(os.fspath(path))
			
			if not follow_symlinks:
				stx_flags |= AT_SYMLINK_NOFOLLOW
			
			stx_result = statx(stx_dirfd, stx_path, stx_flags, Mask.BASIC_STATS | Mask.BTIME)
			assert (~stx_result.stx_mask & (Mask.BASIC_STATS & ~Mask.BLOCKS)) == 0
			
			st_blocks       = None
			st_birthtime    = None
			st_birthtime_ns = None
			if stx_result.stx_mask & Mask.BLOCKS:
				st_blocks = stx_result.stx_blocks
			if stx_result.stx_mask & Mask.BTIME:
				st_birthtime    = stx_result.stx_btime.tv_sec
				st_birthtime_ns = ts_to_nstime(stx_result.stx_btime)
			
			
			return stat_result(
				# Standard struct data
				stx_result.stx_mode,
				stx_result.stx_ino,
				os.makedev(stx_result.stx_dev_major, stx_result.stx_dev_minor),
				stx_result.stx_nlink,
				stx_result.stx_uid,
				stx_result.stx_gid,
				stx_result.stx_size,
				stx_result.stx_atime.tv_sec,
				stx_result.stx_ctime.tv_sec,
				stx_result.stx_mtime.tv_sec,
				
				# Extended (platform-dependant) attributes
				stx_result.stx_blksize,
				os.makedev(stx_result.stx_rdev_major, stx_result.stx_rdev_minor),
				stx_result.stx_attributes,
				st_blocks,
				
				# High-precision timestamps
				ts_to_nstime(stx_result.stx_atime),
				ts_to_nstime(stx_result.stx_ctime),
				ts_to_nstime(stx_result.stx_mtime),
				
				# Non-standard birth time value
				st_birthtime,
				st_birthtime_ns
			)
		except NotImplementedError:
			_statx_available = False
	
	return os.stat(path, dir_fd=dir_fd, follow_symlinks=follow_symlinks)


def lstat(path: path_t, *, dir_fd: typing.Optional[int] = None) -> stat_result_t:
	"""Alias for ``stat(…, follow_symlinks=False)`."""
	return stat(path, dir_fd=dir_fd, follow_symlinks=False)


def fstat(fd: int) -> stat_result_t:
	"""Alias for ``stat(fd)`."""
	return stat(fd)
