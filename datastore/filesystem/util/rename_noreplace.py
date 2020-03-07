"""A version of :func:`os.rename` that rejects replacing an existing file on
all platforms – rather than just on Windows."""

import os
import sys
import typing

# The below idea is curtosy of the discussion in a .NetCore issue:
# https://github.com/dotnet/runtime/issues/14885
if sys.platform == "win32":
	rename_noreplace = os.rename  # That already exists… Done!
else:
	import errno
	
	if sys.platform == "linux" or typing.TYPE_CHECKING:
		from . import exchange
	
	def rename_noreplace(src: exchange.path_t, dst: exchange.path_t, *,
	                     src_dir_fd: int = None, dst_dir_fd: int = None) -> None:
		src = os.fsencode(src)
		dst = os.fsencode(dst)
		
		if sys.platform == "linux":
			# Try using `renameat2(…, RENAME_NOREPLACE)` first
			src_dir_fd_lin = src_dir_fd if src_dir_fd is not None else exchange.AT_FDCWD
			dst_dir_fd_lin = dst_dir_fd if dst_dir_fd is not None else exchange.AT_FDCWD
			
			flags = exchange.RenameFlags.NOREPLACE
			try:
				exchange.renameat2(src_dir_fd_lin, src, dst_dir_fd_lin, dst, flags)
			except NotImplementedError:
				pass
			except OSError as exc:
				# Ignore errors likely resulting from lack of filesystem support
				if exc.errno != errno.EINVAL:
					raise
		
		
		# Alias source file to destination
		# This will raise `FileExistsError`, rather then replacing the target
		os.link(dst, src, src_dir_fd=src_dir_fd, dst_dir_fd=dst_dir_fd, follow_symlinks=False)
		
		try:
			# On success of the above, unlink the source file
			os.unlink(src, dir_fd=src_dir_fd)
		except BaseException:
			# Try to remove the target file, rather then leaving behind the
			# hard-link (if this fails as well then both exceptions will be
			# shown which is intended behaviour in this case)
			os.unlink(dst, dir_fd=dst_dir_fd)
			
			# Continue propagating the original error
			raise
