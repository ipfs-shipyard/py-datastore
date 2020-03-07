import dataclasses
import typing


@dataclasses.dataclass(frozen=True)
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
	
	# The backing record's last access time
	atime: typing.Optional[typing.Union[int, float]] = None
	# The backing record's last modification time
	mtime: typing.Optional[typing.Union[int, float]] = None
	# The backing record's creation (“birth”) time
	btime: typing.Optional[typing.Union[int, float]] = None


@dataclasses.dataclass(frozen=True)
class ChannelMetadata(_MetadataBase):
	__doc__ = (_MetadataBase.__doc__ or "")[:-1] + """\
	count
		The number of objects that will be returned, or `None` if unavailable
	"""
	
	# The total length of this stream (if available)
	count: typing.Optional[int] = None


@dataclasses.dataclass(frozen=True)
class StreamMetadata(_MetadataBase):
	__doc__ = (_MetadataBase.__doc__ or "")[:-1] + """\
	size
		The size of the entire stream data in bytes, or `None` if unavailable
	"""
	
	# The total length of this stream (if available)
	size: typing.Optional[int] = None


@dataclasses.dataclass(frozen=True)
class DatastoreMetadata:
	"""
	Attributes
	----------
	size
		The size of the entire datastore in bytes, or `None` if unavailable
	"""
	
	size: typing.Optional[int] = None
