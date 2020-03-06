"""Somewhat cross-platform (Linux 3.15+ & macOS) function to atomically
swap/exchange two files on the file system.

On Linux this may be any two existing file system nodes, on macOS the underlying
``exchangedata(2)`` only allows for regular files and will retain the orignal
values for all file attributes except for mtime (which it will swap instead);
it also will only work on HFS+."""

import ctypes
import enum
import errno
import os
import sys
import typing

# Special FD for value for meaning “no FD”
AT_FDCWD = -100

if sys.platform == "linux":
	class RenameFlags(enum.IntFlag):
		NOREPLACE = 0x00000001  # Do not replace target file
		EXCHANGE  = 0x00000002  # Exchange source and target file
		WHITEOUT  = 0x00000004  # Replace source file with whiteout node
	
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
			_func = _libc.renameat2
			_func.argtypes = (
				ctypes.c_int,     # olddirfd
				ctypes.c_char_p,  # oldpath
				ctypes.c_int,     # newdirfd
				ctypes.c_char_p,  # newpath
				ctypes.c_uint,    # flags
			)
		except AttributeError:  # Probably not GLibC 2.28+
			import functools
			
			# This system call appeared as part of Linux 3.15 in June 2014, but
			# library support was only added in glibc 2.18 in June 2018! So
			# there are many distributions (including Ubuntu 18.04LTS) that have
			# the system call but not the library wrapper.
			_nr_renameat2: int = 276  # Generic Linux
			if os.uname().machine in ("i386", "i686", "x86", "x86_64"):
				if ctypes.sizeof(ctypes.c_void_p) == 4:  # Running as 32-bit
					_nr_renameat2 = 353
				else:  # Running as 64-bit
					_nr_renameat2 = 316
			
			_func = _libc.syscall
			_func.argtypes = (
				ctypes.c_long,    # sys_nr
				ctypes.c_int,     # olddirfd
				ctypes.c_char_p,  # oldpath
				ctypes.c_int,     # newdirfd
				ctypes.c_char_p,  # newpath
				ctypes.c_uint,    # flags
			)
			_func = functools.partial(_func, _nr_renameat2)
	except OSError:
		_error = NotImplementedError("renameat2: Could not locate the C library")
	
	def renameat2(
			olddirfd: int,
			oldpath: bytes,
			newdirfd: int,
			newpath: bytes,
			flags: RenameFlags
	) -> None:
		"""Low-level wrapper around the ``renameat2(2)`` Linux system call"""
		global _error
		if _error:
			raise _error
		assert _func
		
		result = _func(olddirfd, oldpath, newdirfd, newpath, flags)
		if result < 0:
			if ctypes.get_errno() == errno.ENOSYS:  # Kernel does not support syscall
				_error = NotImplementedError("renameat2: System call not supported by this version of Linux")
				raise _error
			if ctypes.get_errno() == errno.EINVAL:  # Filesystem does not support this
				raise NotImplementedError("renameat2: Operation not supported by filesystem (or usage error)")
			raise OSError(ctypes.get_errno(), os.strerror(ctypes.get_errno()))

if sys.platform == "darwin":
	class FsOpt(enum.IntFlag):
		NOFOLLOW          = 0x01
		NOINMEMUPDATE     = 0x02
		REPORT_FULLSIZE   = 0x04
		PACK_INVAL_ATTRS  = 0x08
		ATTR_CMN_EXTENDED = 0x20
	
	_error = None
	_func = None
	
	try:
		import ctypes.util
		_libc_name = ctypes.util.find_library("c")
		if _libc_name is not None:
			_libc = ctypes.CDLL(_libc_name, use_errno=True)
		else:
			raise FileNotFoundError()
		
		try:
			_func = _libc.exchangedata
			_func.argtypes = (
				ctypes.c_char_p,  # path1
				ctypes.c_char_p,  # path2
				ctypes.c_ulong,   # options
			)
		except AttributeError:
			_error = NotImplementedError("exchangedata: C library does not expose symbol 'exchangedata'")
	except OSError:
		_error = NotImplementedError("exchangedata: Could not locate the C library")
	
	
	def exchangedata(path1: bytes, path2: bytes, options: FsOpt) -> None:
		"""Low-level wrapper around the ``exchangedata(2)`` Darwin system call"""
		if _error:
			raise _error
		assert _func
		
		result = _func(path1, path2, options)
		if result < 0:
			if ctypes.get_errno() == errno.ENOTSUP:  # Filesystem does not support this
				raise NotImplementedError("exchangedata: Operation not supported by filesystem")
			raise OSError(ctypes.get_errno(), os.strerror(ctypes.get_errno()))


path_t = typing.Union[str, bytes, os.PathLike]


if "renameat2" in globals() or "exchangedata" in globals():
	def exchange(src: path_t, dst: path_t, *, src_dir_fd: int = None, dst_dir_fd: int = None):
		src = os.fsencode(src)
		dst = os.fsencode(dst)
		
		if "renameat2" in globals():
			src_dir_fd = src_dir_fd if src_dir_fd is not None else AT_FDCWD
			dst_dir_fd = dst_dir_fd if dst_dir_fd is not None else AT_FDCWD
			
			flags = RenameFlags.EXCHANGE
			
			renameat2(src_dir_fd, src, dst_dir_fd, dst, flags)
		elif "exchangedata" in globals():
			if src_dir_fd is not None or dst_dir_fd is not None:
				raise NotImplementedError("exchange: dir_fd not supported on macOS")
			
			# Use NOFOLLOW here to make the macOS behaviour a strict subset of
			# the one available on Linux – only regular files will be able to
			# be exchanged on macOS, while any file node type may be exchanged
			# on Linux. Without this, the behaviour on symlinks would differ
			# between the two platforms instead!
			options = FsOpt.NOFOLLOW  # type: ignore[name-defined]  # noqa: F821
			
			exchangedata(src, dst, options)  # type: ignore[name-defined]  # noqa: F821
		else:
			assert False, "unreachable"


# Discovery mechanism similar to `os.supports_dir_fd`
if "renameat2" in globals():
	supports_dir_fd = {exchange}
else:
	supports_dir_fd = set()
