import codecs
import json
import typing

import datastore
import datastore.abc

__all__ = ("Serializer", "PrettySerializer")

T_co = typing.TypeVar("T_co", covariant=True)


#XXX: Switch to [`ijson`](https://pypi.org/project/ijson/) once it supports a
#     non-blocking parsing mode.
#     Upstream issue: https://github.com/ICRAR/ijson/issues/22 (“working on this”)


class Serializer(datastore.abc.Serializer[T_co], typing.Generic[T_co]):
	"""JSON parser that handles concatenated JSON messages and encodes to the
	   most compact representation possible by default."""
	
	__slots__ = ("encoding", "indent", "separators",
	             "_buffer", "_decoder1", "_decoder2", "_lasterror")

	encoding:   str
	indent:     typing.Optional[typing.Union[int, str]]
	separators: typing.Tuple[str, str]
	
	_buffer:    typing.List[str]
	_decoder1:  codecs.IncrementalDecoder
	_decoder2:  json.JSONDecoder
	_lasterror: typing.Optional[Exception]


	def __init__(self, *, encoding: str = "utf-8",
	             indent: typing.Optional[typing.Union[int, str]] = None,
	             separators: typing.Tuple[str, str] = (',', ':')):
		self.encoding   = encoding
		self.indent     = indent
		self.separators = separators
		
		self._buffer    = []
		self._decoder1  = codecs.getincrementaldecoder(encoding)()
		self._decoder2  = json.JSONDecoder()
		self._lasterror = None

	def parse_partial(self, data: bytes) -> typing.Iterator[T_co]:
		"""Incrementally decodes JSON data sets into Python objects.

		Raises
		------
		~datastore.DecodingError
		"""
		try:
			# Python 3 requires all JSON data to be a text string
			lines = self._decoder1.decode(data, False).split("\n")

			# Add first input line to last buffer line, if applicable, to
			# handle cases where the JSON string has been chopped in half
			# at the network level due to streaming
			if len(self._buffer) > 0 and self._buffer[-1] is not None:
				self._buffer[-1] += lines[0]
				self._buffer.extend(lines[1:])
			else:
				self._buffer.extend(lines)
		except UnicodeDecodeError as error:
			raise datastore.ParseError("json", error) from error

		# Process data buffer
		index = 0
		try:
			# Process each line as separate buffer
			#PERF: This way the `.lstrip()` call becomes almost always a NOP
			#      even if it does return a different string it will only
			#      have to allocate a new buffer for the currently processed
			#      line.
			while index < len(self._buffer):
				while self._buffer[index]:
					# Make sure buffer does not start with whitespace
					#PERF: `.lstrip()` does not reallocate if the string does
					#      not actually start with whitespace.
					self._buffer[index] = self._buffer[index].lstrip()

					# Handle case where the remainder of the line contained
					# only whitespace
					if not self._buffer[index]:
						self._buffer[index] = None
						continue

					# Try decoding the partial data buffer and return results
					# from this
					data = self._buffer[index]
					for index2 in range(index, len(self._buffer)):
						# If decoding doesn't succeed with the currently
						# selected buffer (very unlikely with our current
						# class of input data) then retry with appending
						# any other pending pieces of input data
						# This will happen with JSON data that contains
						# arbitrary new-lines: "{1:\n2,\n3:4}"
						if index2 > index:
							data += "\n" + self._buffer[index2]

						try:
							print(repr(data))
							(obj, offset) = self._decoder2.raw_decode(data)
						except ValueError:
							# Treat error as fatal if we have already added
							# the final buffer to the input
							if (index2 + 1) == len(self._buffer):
								raise
						else:
							index = index2
							break

					# Decoding succeeded – yield result and shorten buffer
					yield obj
					
					if (len(data) - offset) > 0:
						buffer_offset = len(self._buffer[index]) - len(data) + offset
						self._buffer[index] = self._buffer[index][buffer_offset:]
					else:
						self._buffer[index] = None
				index += 1
		except ValueError as error:
			# It is unfortunately not possible to reliably detect whether
			# parsing ended because of an error *within* the JSON string, or
			# an unexpected *end* of the JSON string.
			# We therefor have to assume that any error that occurs here
			# *might* be related to the JSON parser hitting EOF and therefor
			# have to postpone error reporting until `parse_finalize` is
			# called.
			self._lasterror = error
		finally:
			# Remove all processed buffers
			del self._buffer[0:index]

	def parse_finalize(self) -> typing.Iterator[T_co]:
		"""Raises errors for incomplete buffered data that could not be parsed
		because the end of the input data has been reached.

		Raises
		------
		~ipfshttpclient.exceptions.DecodingError

		Returns
		-------
			tuple : Always empty
		"""
		try:
			try:
				# Raise exception for remaining bytes in bytes decoder
				self._decoder1.decode(b"", True)
			except UnicodeDecodeError as error:
				raise datastore.ParseError("json", error) from error

			# Late raise errors that looked like they could have been fixed if
			# the caller had provided more data
			if self._buffer:
				raise datastore.ParseError("json", self._lasterror) from self._lasterror
		finally:
			# Reset state
			self._buffer    = []
			self._lasterror = None
			self._decoder1.reset()

		return ()
	
	async def parse(self, source: datastore.abc.ReceiveStream) -> typing.Iterator[T_co]:
		async for chunk in source:
			for obj in self.parse_partial(chunk):
				yield obj
		for obj in self.parse_finalize():
			yield obj

	async def serialize(self, value: datastore.abc.ReceiveChannel[T_co]) \
	      -> typing.Iterator[bytes]:
		"""returns json serialized `value` (pretty-printed)."""
		try:
			async for obj in value:
				# We force `ensure_ascii=False` here to most compact encoding of
				# having non-ascii characters be encoded by the given encoding
				# rather then using JSON string "\uXXXX" notation.
				# We also force `sort_keys=True` to ensure the output is
				# reproducible between interpreter runs.
				yield json.dumps(obj, ensure_ascii=False, sort_keys=True,
				                 indent=self.indent, separators=self.separators
				).encode(self.encoding)
		except (UnicodeEncodeError, TypeError) as error:
			raise datastore.SerializeError("json", error) from error


class PrettySerializer(Serializer[T_co], typing.Generic[T_co]):
	"""json wrapper serializer that pretty-prints.
	Useful for human readable values and versioning.
	"""
	
	def __init__(self, *, encoding: str = "utf-8"):
		super().__init__(encoding=encoding, indent="\t", separators=(", ", ": "))
