import abc
import collections.abc
import io
import typing

import trio.abc


ArbitraryReceiveChannel = typing.Union[
	trio.abc.ReceiveChannel,
	typing.AsyncIterable[object],
	typing.Awaitable[object],
	typing.Iterable[object]
]


ArbitraryReceiveStream = typing.Union[
	trio.abc.ReceiveStream,
	typing.AsyncIterable[bytes],
	typing.Awaitable[bytes],
	typing.Iterable[bytes],
	bytes
]


class ReceiveChannel(trio.abc.ReceiveChannel):
	"""A slightly extended version of `trio`'s standard interface for receiving object streams.
	
	Attributes
	----------
	count
		The number of objects that will be returned, or `None` if unavailable
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
	__slots__ = ("count", "atime", "mtime", "btime")
	
	# The total length of this stream (if available)
	count: typing.Optional[int]
	
	# The backing record's last access time
	atime: typing.Optional[typing.Union[int, float]]
	# The backing record's last modification time
	mtime: typing.Optional[typing.Union[int, float]]
	# The backing record's creation (“birth”) time
	btime: typing.Optional[typing.Union[int, float]]
	
	
	def __init__(self):
		self.count = None
		self.atime = None
		self.mtime = None
		self.btime = None
	
	
	async def collect(self) -> typing.List:
		result: typing.List = []
		async with self:
			async for item in self:
				result.append(item)
		return result


class WrapingReceiveChannel(ReceiveChannel):
	"""Abstracts over various forms of synchronous and asynchronous returning of
	   object streams
	"""
	
	_source: typing.Union[typing.AsyncIterator, typing.Iterator]
	
	def __init__(self, source: ArbitraryReceiveChannel):
		super().__init__()
		
		source_val: typing.Union[typing.AsyncIterable, typing.Iterable]
		
		# Handle special cases, so that we'll end up either with a synchronous
		# or an asynchrous iterable (also tries to calculate the expected total
		# number of objects ahead of time for some known cases)
		if isinstance(source, collections.abc.Awaitable):
			async def await_iter_wrapper(source):
				yield await source
			source_val = await_iter_wrapper(source)
		elif isinstance(source, collections.abc.Sequence):
			self.count = len(source)
			source_val = source
		else:
			source_val = source
		assert isinstance(source_val, (collections.abc.AsyncIterable, collections.abc.Iterable))
		
		if isinstance(source_val, collections.abc.AsyncIterable):
			self._source = source_val.__aiter__()
		else:
			self._source = iter(source_val)
	
	
	async def receive(self) -> object:
		if isinstance(self._source, collections.abc.AsyncIterator):
			try:
				return await self._source.__anext__()
			except StopAsyncIteration as exc:
				await self.aclose()
				raise trio.EndOfChannel() from exc
		else:
			try:
				return next(self._source)
			except StopIteration as exc:
				await self.aclose()
				raise trio.EndOfChannel() from exc
	
	
	async def aclose(self) -> None:
		try:
			if isinstance(self._source, collections.abc.AsyncIterator):
				await self._source.aclose()  # type: ignore  # We catch errors instead
			else:
				self._source.close()  # type: ignore  # We catch errors instead
		except AttributeError:
			pass


def receive_channel_from(channel: ArbitraryReceiveChannel[T_co]) -> ReceiveChannel[T_co]:
	return WrapingReceiveChannel(channel)



class ReceiveStream(trio.abc.ReceiveStream):
	"""A slightly extended version of `trio`'s standard interface for receiving byte streams.
	
	Attributes
	----------
	size
		The size of the entire stream data in bytes, or `None` if unavailable
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
	__slots__ = ("size", "atime", "mtime", "btime")
	
	# The total length of this stream (if available)
	size: typing.Optional[int]
	
	# The backing record's last access time
	atime: typing.Optional[typing.Union[int, float]]
	# The backing record's last modification time
	mtime: typing.Optional[typing.Union[int, float]]
	# The backing record's creation (“birth”) time
	btime: typing.Optional[typing.Union[int, float]]
	
	
	def __init__(self):
		self.size  = None
		self.atime = None
		self.mtime = None
		self.btime = None
	
	
	async def collect(self) -> bytes:
		value = bytearray()
		async with self:
			# Use “size”, if available, to try and read the entire stream's conents
			# in one go
			max_bytes = getattr(self, "size", None)
			
			while True:
				chunk = await self.receive_some(max_bytes)
				if len(chunk) < 1:
					break
				value += chunk
		return bytes(value)



class WrapingReceiveStream(ReceiveStream):
	"""Abstracts over various forms of synchronous and asynchronous returning of
	   byte streams
	"""
	
	_buffer:  bytearray
	_memview: typing.Union[memoryview, None]
	_offset:  int
	
	_source: typing.Union[typing.AsyncIterator[bytes], typing.Iterator[bytes]]
	
	def __init__(self, source):
		super().__init__()
		
		# Handle special cases, so that we'll end up either with a synchronous
		# or an asynchrous iterable (also tries to calculate the expected total
		# stream size ahead of time for some known cases)
		source_val: typing.Union[trio.abc.ReceiveStream,
		                         typing.AsyncIterable[bytes],
		                         typing.Iterable[bytes]]
		if isinstance(source, collections.abc.Awaitable):
			async def await_iter_wrapper(source):
				yield await source
			source_val = await_iter_wrapper(source)
		elif isinstance(source, bytes):
			self.size = len(source)
			source_val = (source,)
		elif isinstance(source, collections.abc.Sequence):
			# Remind mypy that the result of the above test is
			# `Sequence[bytes]`, not `Sequence[Any]` in this case
			source = typing.cast(typing.Sequence[bytes], source)
			
			self.size = sum(len(item) for item in source)
			source_val = source
		elif isinstance(source, io.BytesIO):
			# Ask in-memory stream for its remaining length, restoring its
			# original state afterwards
			pos = source.tell()
			source.seek(0, io.SEEK_END)
			self.size = source.tell() - pos
			source.seek(pos, io.SEEK_SET)
			source_val = source
		else:
			source_val = source
		
		self._buffer  = bytearray()
		self._memview: typing.Optional[memoryview] = None
		self._offset  = 0
		if isinstance(source_val, trio.abc.ReceiveStream):
			self._source = source_val
		elif isinstance(source_val, collections.abc.AsyncIterable):
			self._source = source_val.__aiter__()
		else:
			self._source = iter(source_val)
	
	
	async def receive_some(self, max_bytes=None):
		# Serve chunks from buffer if there is any data that hasn't been
		# delivered yet
		if self._memview:
			if max_bytes is not None:
				end_offset = min(self._offset + max_bytes, len(self._memview))
			else:
				end_offset = len(self._memview)
			
			result = bytes(self._memview[self._offset:end_offset])
			if end_offset >= len(self._memview):
				self._offset = 0
				self._memview.release()
				self._memview = None
				self._buffer.clear()
			return result
		
		
		at_end = False
		while not at_end:
			value = b""
			if isinstance(self._source, trio.abc.ReceiveStream):
				# This branch is just an optimization to pass `max_bytes` along
				# to subordinated ReceiveStreams
				value = await self._source.receive_some(max_bytes)
				at_end = (len(value) < 1)
			elif isinstance(self._source, collections.abc.AsyncIterator):
				try:
					value = await self._source.__anext__()
				except StopAsyncIteration:
					at_end = True
			else:
				try:
					value = next(self._source)
				except StopIteration:
					at_end = True
			
			# Skip empty returned byte strings as they have a special meaning here
			if len(value) < 1:
				continue
			
			# Stash extra bytes that are too large for our receiver
			if max_bytes is not None and max_bytes > len(value):
				self._buffer += value[max_bytes:]
				self._memview = memoryview(self._buffer)
				value = value[:max_bytes]
			
			return value
		
		# We're at the end
		await self.aclose()
		return b""
	
	
	async def aclose(self):
		try:
			if isinstance(self._source, collections.abc.Iterable):
				await self._source.aclose()  # type: ignore  # We catch errors instead
			else:
				self._source.close()  # type: ignore  # We catch errors instead
		except AttributeError:
			pass
