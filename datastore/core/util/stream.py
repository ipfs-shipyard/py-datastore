import collections.abc
import typing

import trio.abc


async def collect(self):
	if isinstance(self, collections.abc.Awaitable):
		self = await self
	
	value = bytearray()
	async with self:
		async for chunk in self:
			value += chunk
	return bytes(value)


class ReceiveStream(trio.abc.ReceiveStream):
	"""Abstracts over various forms of synchronous and asynchronous returning of
	   byte streams
	"""
	
	
	_ASYNC_TYPE_SYNC = 0
	_ASYNC_TYPE_ITER = 1
	_ASYNC_TYPE_TRIO = 2
	
	def __init__(self, source):
		if isinstance(source, collections.abc.Awaitable):
			async def await_iter_wrapper(source):
				try:
					yield await source
				except Exception as exc:
					source.athrow(exc)
					raise
			source = await_iter_wrapper(source)
		elif isinstance(source, bytes):
			source = (source,)
		
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
	
	
	# This will bind the function as a normal method when instantiating
	# this class
	collect = collect
	
	
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
