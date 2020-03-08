import dataclasses
import typing

if typing.TYPE_CHECKING:
	from typing_extensions import Literal as typing_Literal
elif hasattr(typing, "Literal"):  #PY38+
	from typing import Literal as typing_Literal
else:  #PY37-
	from typing import Union as typing_Literal


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


accuracy_t = typing_Literal["unknown", "lower-bound", "approximate", "exact"]


# This only exists to declare the class attribute `IGNORE` in *mypy* without it
# being considered special by *dataclasses*
class _DatastoreMetadataBase:
	IGNORE: 'DatastoreMetadata'


@dataclasses.dataclass(frozen=True)
class DatastoreMetadata(_DatastoreMetadataBase):
	"""
	.. method:: IGNORE() -> Datastore.Metadata
	   :property:
	   
	   Singleton instance representing a metadata value that should never be counted
	   
	   This is used in conjunction with the ``_seen`` attribute of
	   :meth:`~datastore.BinaryDatastore.datastore_stats` to ensure that each
	   datastore is only counted once.
	
	Attributes
	----------
	size
		The size of the entire datastore in bytes, or `None` if unavailable
	size_accuracy
		The accuracy of the returned size value, may be any of the following:
		
		* ``"unknown"``: We have no idea how reliable the returned accuracy is,
		  the size is a rough estimate at best (this is the only allowed accuracy
		  type if ``size`` is ``None``)
		* ``"lower-bound"``: We know that the size must be at least as large as
		                     the returned value, but it may be higher
		* ``"approximate"``: We know the size should be very similar to the real
		                     value but it may be slightly off
		* ``"exact"``: We know the value to be totally reliable
	"""
	
	size: typing.Optional[int] = None
	size_accuracy: accuracy_t = "unknown"
	
	def __post_init__(self) -> None:
		assert self.size is not None or self.size_accuracy == "unknown"
	
	def __add__(self, other: object) -> 'DatastoreMetadata':
		if isinstance(other, DatastoreMetadata):
			# Do not consider values in the special ignore-instance
			if other is self.IGNORE:
				return self
			if self is self.IGNORE:
				return other
			
			size_incomplete: bool = False
			
			size: typing.Optional[int] = None
			if other.size is None:
				size = self.size
				size_incomplete = True
			elif self.size is None:
				size = other.size
				size_incomplete = True
			else:
				size = self.size + other.size
			
			# Merge the size accuracy value by choosing the weaker of the two
			# possible predications in each case
			size_accuracy: accuracy_t = "unknown"
			if size is None:
				pass  # Use the default `"unknown"` size accuracy value
			elif (self.size_accuracy == "unknown" and self.size is not None) \
			     or (other.size_accuracy == "unknown" and other.size is not None):
				pass  # Use the default `"unknown"` size accuracy value
			elif self.size_accuracy == "lower-bound" \
			     or other.size_accuracy == "lower-bound" \
			     or size_incomplete:
				size_accuracy = "lower-bound"
			elif self.size_accuracy == "approximate" \
			     or other.size_accuracy == "approximate":
				size_accuracy = "approximate"
			elif self.size_accuracy == "exact" \
			     or other.size_accuracy == "exact":
				size_accuracy = "exact"
			
			return DatastoreMetadata(size=size, size_accuracy=size_accuracy)
		return NotImplemented


DatastoreMetadata.IGNORE = DatastoreMetadata()
