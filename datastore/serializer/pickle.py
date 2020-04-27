import io
import os
import pickle
import typing

import datastore.abc

__all__ = ("Serializer",)

T_co = typing.TypeVar("T_co", covariant=True)


#FIXME: This stuff should support streaming data to the maximum extent possible


class Serializer(datastore.abc.Serializer[T_co], typing.Generic[T_co]):
	"""Python object parser/encoder using `pickle`.
	
	WARNING: This serializer does not actually support streaming and will buffer
	         everything in memory before pickling/unpickling the data.
	"""
	__slots__ = ("protocol", "py2_encoding", "py2_errors", "py2_fix_imports")
	
	protocol: typing.Optional[int]
	
	py2_encoding:    str
	py2_errors:      str
	py2_fix_imports: bool
	
	
	# The default protocol version (4) was introduced with Python 3.4 and is
	# the last version compatible with all Python versions we target.
	
	
	def __init__(self, *, protocol: typing.Optional[int] = 4,
	             py2_fix_imports: bool = True, py2_encoding: str = "latin1",
	             py2_errors: str = "strict"):
		self.protocol = protocol
		
		self.py2_encoding    = py2_encoding
		self.py2_errors      = py2_errors
		self.py2_fix_imports = py2_fix_imports
	
	
	async def parse(self, source: datastore.abc.ReceiveStream) -> typing.Iterator[T_co]:
		#XXX: Collect all data into one big binary I/O stream
		bio_stream = io.BytesIO()
		bio_stream.write(await source.collect())
		bio_stream.seek(0, os.SEEK_SET)
		
		unpickler = pickle.Unpickler(bio_stream, fix_imports=self.py2_fix_imports,
		                             encoding=self.py2_encoding, errors=self.py2_errors)
		while True:
			try:
				yield unpickler.load()
			except EOFError:
				break
			except pickle.UnpicklingError as error:
				raise datastore.ParseError("pickle", error) from error
	
	
	async def serialize(self, value: datastore.abc.ReceiveChannel[T_co]) \
	      -> typing.Iterator[bytes]:
		"""returns json serialized `value` (pretty-printed)."""
		try:
			async for obj in value:
				yield pickle.dumps(obj, protocol=self.protocol, fix_imports=self.py2_fix_imports)
		except pickle.PicklingError as error:
			raise datastore.SerializeError("pickle", error) from error
