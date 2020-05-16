import json
import typing

import datastore.datastore_abc

__all__ = ("Serializer", "PrettySerializer")

T_co = typing.TypeVar("T_co", covariant=True)


#FIXME: This stuff should support streaming data to the maximum extent possible


class Serializer(datastore.datastore_abc.Serializer[T_co], typing.Generic[T_co]):
	"""json wrapper serializer that gives the most compact representation possible.
	"""
	
	__slots__ = ("encoding", "indent", "separators")
	
	encoding:   str
	indent:     typing.Optional[typing.Union[int, str]]
	separators: typing.Tuple[str, str]
	
	
	def __init__(self, *, encoding: str = "utf-8",
	             indent: typing.Optional[typing.Union[int, str]] = None,
	             separators: typing.Tuple[str, str] = (',', ':')):
		self.encoding   = encoding
		self.indent     = indent
		self.separators = separators
	
	
	def loads(self, value: bytes) -> typing.List[T_co]:
		"""returns json deserialized `value`."""
		return [json.loads(value)]  #FIXME: Broken if more than one object
	
	
	def dumps(self, value: typing.Iterable[T_co]) -> bytes:
		"""returns json serialized `value` (pretty-printed)."""
		result = bytearray()
		for item in value:
			# We force `ensure_ascii=False` here to most compact encoding of
			# having non-ascii characters be encoded by the given encoding
			# rather then using JSON string "\uXXXX" notation.
			# We also force `sort_keys=True` to ensure the output is
			# reproducible between interpreter runs.
			result += json.dumps(item, ensure_ascii=False, sort_keys=True,
			                     indent=self.indent, separators=self.separators
			).encode(self.encoding)
		return bytes(result)



class PrettySerializer(Serializer[T_co], typing.Generic[T_co]):
	"""json wrapper serializer that pretty-prints.
	Useful for human readable values and versioning.
	"""
	
	def __init__(self, *, encoding: str = "utf-8"):
		super().__init__(encoding=encoding, indent="\t", separators=(", ", ": "))
