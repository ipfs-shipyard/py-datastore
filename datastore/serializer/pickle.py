import pickle
import typing

import datastore.abc


T_co = typing.TypeVar("T_co", covariant=True)


#FIXME: This stuff should support streaming data to the maximum extent possible


class Serializer(datastore.abc.Serializer[T_co], typing.Generic[T_co]):
	"""json wrapper serializer that gives the most compact representation possible.
	"""
	
	__slots__ = ("protocol",)
	
	protocol: typing.Optional[int]
	
	
	def __init__(self, *, protocol: typing.Optional[int] = None):
		self.protocol = protocol
	
	
	def loads(self, value: bytes) -> typing.List[T_co]:
		"""returns pickle deserialized `value`."""
		return [typing.cast(T_co, pickle.loads(value))]  #FIXME: Broken if more than one object
	
	
	def dumps(self, value: typing.Iterable[T_co]) -> bytes:
		"""returns pickle serialized `value` (pretty-printed)."""
		result = bytearray()
		for item in value:
			result += pickle.dumps(item, protocol=self.protocol)
		return bytes(result)
