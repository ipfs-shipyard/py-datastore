import abc
import collections.abc
import io
import typing

import trio.abc


async def collect(self):
	if isinstance(self, collections.abc.Awaitable):
		self = await self
	
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
	
	# This will actually bind the function just like any other method when
	# instantiating this class
	collect = collect



class WrapingReceiveStream(ReceiveStream):
	"""Abstracts over various forms of synchronous and asynchronous returning of
	   byte streams
	"""
	
	
	_ASYNC_TYPE_SYNC = 0
	_ASYNC_TYPE_ITER = 1
	_ASYNC_TYPE_TRIO = 2
	
	def __init__(self, source):
		# Handle special cases, so that we'll end up either with a synchronous
		# or an asynchrous iterable (also tries to calculate the expected total
		# stream size ahead of time for some known cases)
		if isinstance(source, collections.abc.Awaitable):
			async def await_iter_wrapper(source):
					yield await source
			source = await_iter_wrapper(source)
		elif isinstance(source, bytes):
			self.size = len(source)
			source = (source,)
		elif isinstance(source, collections.abc.Sequence):
			self.size = sum(len(item) for item in source)
		elif isinstance(source, io.BytesIO):
			# Ask in-memory stream for its remaining length, restoring its
			# original state afterwards
			pos = source.tell()
			source.seek(0, io.SEEK_END)
			self.size = source.tell() - pos
			source.seek(pos, io.SEEK_SET)
			
		
		self._buffer  = bytearray()
		self._memview = None
		self._offset  = 0
		if isinstance(source, trio.abc.ReceiveStream):
			self._source = source
			self._async  = self._ASYNC_TYPE_TRIO
		elif isinstance(source, collections.abc.AsyncIterable):
			self._source = source.__aiter__()
			self._async  = self._ASYNC_TYPE_ITER
		else:
			self._source = iter(source)
			self._async  = self._ASYNC_TYPE_SYNC
	
	
	
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
			if self._async == self._ASYNC_TYPE_TRIO:
				# This branch is just an optimization to pass `max_bytes` along
				# to subordinated ReceiveStreams
				value = await self._source.receive_some(max_bytes)
				at_end = (len(value) < 1)
			elif self._async:
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
		return b""
	
	
	async def aclose(self):
		if self._async:
			await self._source.aclose()
		else:
			self._source.close()
