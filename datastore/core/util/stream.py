import abc
import collections.abc
import io
import typing

import trio.abc


T    = typing.TypeVar("T")
T_co = typing.TypeVar("T_co", covariant=True)
U_co = typing.TypeVar("U_co", covariant=True)


ArbitraryReceiveChannel = typing.Union[
	trio.abc.ReceiveChannel[T_co],
	typing.AsyncIterable[T_co],
	typing.Awaitable[T_co],
	typing.Iterable[T_co]
]


ArbitraryReceiveStream = typing.Union[
	trio.abc.ReceiveStream,
	typing.AsyncIterable[bytes],
	typing.Awaitable[bytes],
	typing.Iterable[bytes],
	bytes
]


class _ChannelSharedBase:
	__slots__ = ("lock", "refcount")
	
	lock:     trio.Lock
	refcount: int
	
	def __init__(self):
		self.lock     = trio.Lock()
		self.refcount = 1


class ReceiveChannel(trio.abc.ReceiveChannel[T_co], typing.Generic[T_co]):
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

	
	async def collect(self) -> typing.List[T_co]:
		result: typing.List[T_co] = []
		async with self:
			async for item in self:
				result.append(item)
		return result



class _WrapingTrioReceiveChannel(ReceiveChannel[T_co], typing.Generic[T_co]):
	__slots__ = ("_source",)
	
	_source: trio.abc.ReceiveChannel[T_co]
	
	
	def __init__(self, source: trio.abc.ReceiveChannel[T_co]):
		super().__init__()
		
		self._source = source
	
	
	async def receive(self) -> T_co:
		return await self._source.receive()
	
	
	def receive_nowait(self) -> T_co:
		return self._source.receive_nowait()
	
	
	def clone(self) -> ReceiveChannel[T_co]:
		return self.__class__(self._source.clone())
	
	
	async def aclose(self) -> None:
		await self._source.aclose()



class _WrapingChannelShared(_ChannelSharedBase, typing.Generic[U_co]):
	__slots__ = ("source",)
	
	source: typing.Optional[U_co]
	
	
	def __init__(self, source: U_co):
		super().__init__()
		
		self.source = source



class _WrapingIterReceiveChannelBase(ReceiveChannel[T_co], typing.Generic[T_co, U_co]):
	"""Abstracts over various forms of synchronous and asynchronous returning of
	   object streams
	"""
	__slots__ = ("_closed", "_shared")
	
	_closed: bool
	_shared: _WrapingChannelShared[U_co]
	
	def __init__(self, source: typing.Optional[U_co], *,
	             _shared: typing.Optional[_WrapingChannelShared[U_co]] = None):
		super().__init__()
		
		assert source is not None or _shared is not None
		
		self._closed = False
		if _shared is None:
			assert source is not None
			self._shared = _WrapingChannelShared(source)
		else:
			self._shared = _shared
	
	
	@abc.abstractmethod
	async def _receive(self) -> T_co:
		pass
	
	
	async def receive(self) -> T_co:
		if self._closed:
			raise trio.ClosedResourceError()
		if self._shared.source is None:
			raise trio.EndOfChannel()
		
		try:
			async with self._shared.lock:  # type: ignore[attr-defined]  # upstream type bug
				return await self._receive()
		except trio.BrokenResourceError:
			await self.aclose(_mark_closed=True)
			raise
		except trio.EndOfChannel:
			await self.aclose(_mark_closed=False)
			raise
	
	
	@abc.abstractmethod
	def _receive_nowait(self) -> T_co:
		pass
	
	
	def receive_nowait(self) -> T_co:
		if self._closed:
			raise trio.ClosedResourceError()
		if self._shared.source is None:
			raise trio.EndOfChannel()
		
		self._shared.lock.acquire_nowait()
		try:
			return self._receive_nowait()
		finally:
			self._shared.lock.release()
	
	
	def clone(self) -> ReceiveChannel[T_co]:
		if self._closed:
			raise trio.ClosedResourceError()
		
		try:
			return self.__class__(None, _shared=self._shared)
		except BaseException:
			raise
		else:
			self._shared.refcount += 1
	
	
	@abc.abstractmethod
	async def _close_source(self) -> None:
		pass
	
	
	async def aclose(self, *, _mark_closed: bool = True) -> None:
		if not self._closed and _mark_closed:
			self._closed = True
		
		if self._shared.source is None:
			return
		
		self._shared.refcount -= 1
		if self._shared.refcount != 0:
			return
		
		try:
			await self._close_source()
		except AttributeError:
			pass
		finally:
			self._shared.source = None


class _WrapingAsyncIterReceiveChannel(
		_WrapingIterReceiveChannelBase[T_co, typing.AsyncIterator[T_co]],
		typing.Generic[T_co]
):
	def __init__(self, source: typing.Optional[typing.AsyncIterable[T_co]], **kwargs):
		super().__init__(source.__aiter__() if source is not None else None, **kwargs)
	
	
	async def _receive(self) -> T_co:
		assert self._shared.source is not None
		
		try:
			return await self._shared.source.__anext__()
		except StopAsyncIteration as exc:
			raise trio.EndOfChannel() from exc
	
	
	def _receive_nowait(self) -> T_co:
		# Cannot ask this stream type for a non-blocking value
		raise trio.WouldBlock()
	
	
	async def _close_source(self) -> None:
		try:
			await self._shared.source.aclose()  # type: ignore  # We catch errors instead
		except AttributeError:
			pass



class _WrapingSyncIterReceiveChannel(
		_WrapingIterReceiveChannelBase[T_co, typing.Iterator[T_co]],
		typing.Generic[T_co]
):
	def __init__(self, source: typing.Optional[typing.Iterable[T_co]],
	             count_hint: typing.Optional[int] = None, **kwargs):
		super().__init__(iter(source) if source is not None else None, **kwargs)
		
		self.count = count_hint
	
	
	async def _receive(self) -> T_co:
		assert self._shared.source is not None
		
		try:
			return next(self._shared.source)
		except StopIteration as exc:
			raise trio.EndOfChannel() from exc
	
	
	def _receive_nowait(self) -> T_co:
		assert self._shared.source is not None
		
		try:
			return next(self._shared.source)
		except StopIteration:
			# We cannot handle invoking async close here
			raise trio.WouldBlock() from None
	
	
	async def _close_source(self) -> None:
		try:
			self._shared.source.close()  # type: ignore  # We catch errors instead
		except AttributeError:
			pass



def receive_channel_from(channel: ArbitraryReceiveChannel[T_co]) -> ReceiveChannel[T_co]:
	# Optimization: Reuse given stream object, rather then creating a new
	#               wrapper when it is already of the right interface type
	if isinstance(channel, ReceiveChannel):
		return channel
	
	# Optimization: Wrap the given Trio stream object in a tiny wrapper that
	#               just passes through all calls, but adds our extra fields
	#               and values
	if isinstance(channel, trio.abc.ReceiveChannel):
		return _WrapingTrioReceiveChannel(channel)
	
	# Handle asynchronous iterables
	if isinstance(channel, (collections.abc.AsyncIterable, collections.abc.Awaitable)):
		source1: typing.AsyncIterable[T_co]
		
		if isinstance(channel, collections.abc.Awaitable):
			async def await_iter_wrapper(channel: typing.Awaitable[T_co]) \
					-> typing.AsyncIterable[T_co]:
				yield await channel
			source1 = await_iter_wrapper(channel)
		else:
			source1 = channel
		
		return _WrapingAsyncIterReceiveChannel(source1)
	
	# Handle synchronous iterables (and try to deduce the length in each case possible)
	if isinstance(channel, collections.abc.Iterable):
		count:  typing.Optional[int]  = None
		source2: typing.Iterable[T_co] = channel
		
		if isinstance(source2, collections.abc.Sequence):
			count = len(source2)
		
		return _WrapingSyncIterReceiveChannel(source2, count)
	
	assert False, "Unreachable code"



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




class _WrapingTrioReceiveStream(ReceiveStream):
	"""
	Abstracts over a bare `trio.abc.ReceiveStream` to add our standard fields
	and methods to that stream
	"""
	__slots__ = ("_source",)
	
	_source: trio.abc.ReceiveStream
	
	def __init__(self, source: trio.abc.ReceiveStream):
		super().__init__()
		
		self._source = source
	
	
	async def receive_some(self, max_bytes: typing.Optional[int] = None) -> bytes:
		return await self._source.receive_some(max_bytes)
	
	
	async def aclose(self) -> None:
		await self._source.aclose()



class _WrapingIterReceiveStreamBase(ReceiveStream, typing.Generic[T_co]):
	"""Abstracts over various forms of synchronous and asynchronous returning of
	   byte streams
	"""
	
	__slots__ = ("_buffer", "_memview", "_offset", "_closed", "_source")
	
	_buffer:  bytearray
	_memview: typing.Optional[memoryview]
	_offset:  int
	_closed:  bool
	
	_source: typing.Optional[T_co]
	
	def __init__(self, source: T_co):
		super().__init__()
		
		self._source = source
		
		self._buffer  = bytearray()
		self._memview = None
		self._offset  = 0
		self._closed  = False
	
	
	@abc.abstractmethod
	async def _receive(self, max_bytes: typing.Optional[int]) -> bytes:
		pass
	
	
	async def receive_some(self, max_bytes: typing.Optional[int] = None) -> bytes:
		if self._closed:
			raise trio.ClosedResourceError()
		if self._source is None:
			return b""
		
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
				self._memview.release()  # type: ignore  # Fixed in mypy 0.350
				self._memview = None
				self._buffer.clear()
			return result
		
		
		value = await self._receive(max_bytes)
		
		assert isinstance(value, bytes), \
		       f"Source stream {repr(self._source)} returned non-byte segment"
		
		if len(value) < 1:
			# We're at the end
			await self.aclose(_mark_closed=False)
			return b""
		
		# Stash extra bytes that are too large for our receiver
		if max_bytes is not None and max_bytes > len(value):
			self._buffer += value[max_bytes:]
			self._memview = memoryview(self._buffer)
			value = value[:max_bytes]
		
		return value
	
	
	@abc.abstractmethod
	async def _close_source(self) -> None:
		pass
	
	
	async def aclose(self, *, _mark_closed=True) -> None:
		if not self._closed and _mark_closed:
			self._closed = True
		
		if self._source is None:
			return
		
		try:
			await self._close_source()
		finally:
			self._source = None
			if self._memview is not None:
				self._memview.release()  # type: ignore  # Fixed in mypy 0.450
				self._memview = None
			self._buffer.clear()


class _WrapingAsyncIterReceiveStream(_WrapingIterReceiveStreamBase[typing.AsyncIterator[bytes]]):
	def __init__(self, source: typing.AsyncIterable[bytes]):
		super().__init__(source.__aiter__())
	
	
	async def _receive(self, _: typing.Optional[int]) -> bytes:
		assert self._source is not None
		
		# Skip empty returned byte strings as they have a special meaning here
		value = b""
		while len(value) < 1:
			try:
				value = await self._source.__anext__()
			except StopAsyncIteration:
				return b""
		return value
	
	
	async def _close_source(self) -> None:
		try:
			await self._source.aclose()  # type: ignore[union-attr]  # We catch errors instead
		except AttributeError:
			pass



class _WrapingSyncIterReceiveStream(_WrapingIterReceiveStreamBase[typing.Iterator[bytes]]):
	def __init__(self, source: typing.Iterable[bytes], size_hint: typing.Optional[int]):
		super().__init__(iter(source))
		
		self.size = size_hint
	
	
	async def _receive(self, _: typing.Optional[int]) -> bytes:
		assert self._source is not None
		
		# Skip empty returned byte strings as they have a special meaning here
		value = b""
		while len(value) < 1:
			try:
				value = next(self._source)
			except StopIteration:
				return b""
		return value
	
	
	async def _close_source(self) -> None:
		try:
			self._source.close()  # type: ignore[union-attr]  # We catch errors instead
		except AttributeError:
			pass



def receive_stream_from(stream: ArbitraryReceiveStream) -> ReceiveStream:
	# Optimization: Reuse given stream object, rather then creating a new
	#               wrapper when it is already of the right interface type
	if isinstance(stream, ReceiveStream):
		return stream
	
	# Optimization: Wrap the given Trio stream object in a tiny wrapper that
	#               just passes through all calls, but adds our extra fields
	#               and values
	if isinstance(stream, trio.abc.ReceiveStream):
		return _WrapingTrioReceiveStream(stream)
	
	# Handle asynchronous iterables
	if isinstance(stream, (collections.abc.AsyncIterable, collections.abc.Awaitable)):
		source1: typing.AsyncIterable[bytes]
		
		# Wrap awaitables of bytes in an asynchronous iterable that yields once
		if isinstance(stream, collections.abc.Awaitable):
			async def await_iter_wrapper(stream: typing.Awaitable[bytes]) \
					-> typing.AsyncIterable[bytes]:
				yield await stream
			source1 = await_iter_wrapper(stream)
		else:
			source1 = stream
		
		return _WrapingAsyncIterReceiveStream(source1)
	
	# Handle synchronous iterables (and try to deduce the length in each case possible)
	if isinstance(stream, (bytes, collections.abc.Iterable)):
		size:    typing.Optional[int] = None
		source2: typing.Iterable[bytes]
		
		# Wrap a simple bytes value in a tuple to make it an iterable
		if isinstance(stream, bytes):
			source2 = (stream,)
		else:
			source2 = stream
		
		# Deduce the length of sequences of bytes (including the single bytes sequence above)
		if isinstance(source2, collections.abc.Sequence):
			size = sum(len(item) for item in source2)
		# … and in-memory byte stream files as well
		elif isinstance(source2, io.BytesIO):
			# Asks for just the remaining length, restoring the position afterwards
			pos = source2.tell()
			source2.seek(0, io.SEEK_END)
			size = source2.tell() - pos
			source2.seek(pos, io.SEEK_SET)
		
		return _WrapingSyncIterReceiveStream(source2, size)
	
	assert False, "Unreachable code"
