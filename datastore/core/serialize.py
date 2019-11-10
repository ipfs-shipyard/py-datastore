import abc
import json
import typing

from . import binarystore
from . import objectstore
from . import key
from . import query
class util:  # noqa
	from .util import stream


T_co = typing.TypeVar("T_co", covariant=True)


#FIXME: This stuff should support streaming data to the maximum extent possible


class Serializer(typing.Generic[T_co], metaclass=abc.ABCMeta):
	"""Serializing protocol. Serialized data must be a string."""

	__slots__ = ()
	
		
	@abc.abstractmethod
	def loads(self, value: bytes) -> typing.List[T_co]:
		"""returns deserialized `value`."""
		pass

	
	@abc.abstractmethod
	def dumps(self, value: typing.List[T_co]) -> bytes:
		"""returns serialized `value`."""
		pass


class NonSerializer(Serializer):
	"""Implements serializing protocol but does not serialize at all.
	If only storing strings (or already-serialized values).
	"""

	@classmethod
	def loads(cls, value):
		"""returns `value`."""
		return value

	@classmethod
	def dumps(cls, value):
		"""returns `value`."""
		return value


class PrettyJSON(Serializer[T_co], typing.Generic[T_co]):
	"""json wrapper serializer that pretty-prints.
	Useful for human readable values and versioning.
	"""

	@classmethod
	def loads(cls, value: bytes) -> typing.List[T_co]:
		"""returns json deserialized `value`."""
		return [json.loads(value)]  # XXX: Broken if more than one object

	@classmethod
	def dumps(cls, value: typing.List[T_co], indent: int = 1, encoding: str = "utf-8") -> bytes:
		"""returns json serialized `value` (pretty-printed)."""
		return json.dumps(value, sort_keys=True, indent=indent)

class SerializerAdapter(objectstore.Datastore[T_co]):
	"""Represents a Datastore that serializes and deserializes values.
	
	As data is ``put``, the serializer shim serializes it and ``put``s it into
	the underlying ``child_datastore``. Correspondingly, on the way out (through
	``get`` or ``query``) the data is retrieved from the ``child_datastore`` and
	deserialized.
	
	Arguments
	---------
	datastore
		A child datastore for the ShimDatastore superclass.
	serializer
		A serializer object (responds to loads and dumps).
	"""

	__slots__ = ("serializer", "child_datastore")
	

	# value serializer
	# override this with their own custom serializer on a class-wide or per-
	# instance basis. If you plan to store mostly strings, use NonSerializer.
	serializer: Serializer[T_co]
	
	
	def __init__(self, datastore: binarystore.Datastore,
	             serializer: Serializer[T_co]):
		"""Initializes internals and tests the serializer.
		
		Arguments
		---------
		datastore
			A child datastore for the ShimDatastore superclass.
		serializer
			A serializer object (responds to loads and dumps).
		"""
		super().__init__(datastore)
		self.child_datastore = datastore
		self.serializer      = serializer

	
	async def get(self, key: key_.Key) -> util.stream.ReceiveChannel[T_co]:
		"""Returns the object named by key or raises `KeyError` if it does not exist.
		
		Retrieves the value from the ``child_datastore``, and de-serializes
		it on the way out.
		
		Args:
			key: Key naming the object to retrieve
		
		Returns:
			object or None
		"""
		value = await (await self.child_datastore.get(key)).collect()  #FIXME
		return util.stream.receive_channel_from(self.serializer.loads(value))
	
	
	async def put(self, key: key_.Key, value: util.stream.ArbitraryReceiveChannel[T_co]) -> None:
		"""Stores the object `value` named by `key`.
		Serializes values on the way in, and stores the serialized data into the
		``child_datastore``.
		
		Args:
			key: Key naming `value`
			value: the object to store.
		"""
		value_items = await util.stream.receive_channel_from(value).collect()  #FIXME
		value_bytes = self.serializer.dumps(value_items)
		await self.child_datastore.put(key, value_bytes)

	
	async def query(self, query: query_.Query) -> query_.Cursor:
		"""Returns an iterable of objects matching criteria expressed in `query`
		De-serializes values on the way out, using a :ref:`deserialized_gen` to
		avoid incurring the cost of de-serializing all data at once, or ever, if
		iteration over results does not finish (subject to order generator
		constraint).
		
		Args:
			query: Query object describing the objects to return.
		
		Raturns:
			iterable cursor with all objects matching criteria
		"""

		# run the query on the child datastore
		cursor = await self.child_datastore.query(query)

		# chain the deserializing generator to the cursor's result set iterable
		cursor._iterable = deserialized_gen(self.serializer, cursor._iterable)

		return cursor
	
	
	async def delete(self, key: key_.Key) -> None:
		"""Remove the object named by `key`.
		
		Arguments
		---------
		key
			Key naming `value`
		value
			The object to store.
		"""
		await self.child_datastore.delete(key)
	
	
	async def contains(self, key: key_.Key) -> bool:
		"""Returns whether an object named by `key` exists
		
		The default implementation pays the cost of a get. Some datastore
		implementations may optimize this.
		
		Arguments
		---------
		key
			Key naming the object to check.
		"""
		return await self.child_datastore.contains(key)


"""
Hello World:

	>>> import datastore.core
	>>> import json
	>>>
	>>> ds_child = datastore.DictDatastore()
	>>> ds = datastore.serialize.shim(ds_child, json)
	>>>
	>>> hello = datastore.Key('hello')
	>>> ds.put(hello, 'world')
	>>> ds.contains(hello)
	True
	>>> ds.get(hello)
	'world'
	>>> ds.delete(hello)
	>>> ds.get(hello)
	None
"""
