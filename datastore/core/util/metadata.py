import typing


class _MetadataBase:
	"""
	Attributes
	----------
	atime
		Time of the entry's last access (before the current one) in seconds
		since the Unix epoch, or `None` if unkown
	mtime
		Time of the entry's last modification in seconds since the Unix epoch,
		or `None` if unknown
	btime
		Time of entry creation in seconds since the Unix epoch, or `None`
		if unknown
	"""
	__doc__: str
	__slots__ = ("atime", "mtime", "btime")
	
	# The backing record's last access time
	atime: typing.Optional[typing.Union[int, float]]
	# The backing record's last modification time
	mtime: typing.Optional[typing.Union[int, float]]
	# The backing record's creation (“birth”) time
	btime: typing.Optional[typing.Union[int, float]]
	
	def __init__(self, *args, atime = None, mtime = None, btime = None, **kwargs):
		self.atime = atime
		self.mtime = mtime
		self.btime = btime
		super().__init__(*args, **kwargs)


class ChannelMetadata(_MetadataBase):
	__doc__ = _MetadataBase.__doc__[:-1] + """\
	count
		The number of objects that will be returned, or `None` if unavailable
	"""
	__slots__ = ("count",)
	
	# The total length of this stream (if available)
	count: typing.Optional[int]
	
	def __init__(self, *args, count = None, **kwargs):
		super().__init__(*args, **kwargs)
		self.count = count


class StreamMetadata(_MetadataBase):
	__doc__ = _MetadataBase.__doc__[:-1] + """\
	size
		The size of the entire stream data in bytes, or `None` if unavailable
	"""
	__slots__ = ("size",)
	
	# The total length of this stream (if available)
	size: typing.Optional[int]
	
	def __init__(self, *args, size = None, **kwargs):
		super().__init__(*args, **kwargs)
		self.size = size
