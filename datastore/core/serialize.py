import abc
import json
import typing

from . import binarystore
from . import objectstore
from . import key
from . import query
class util:  # noqa
	from .util import stream


default_serializer = json


#FIXME: This stuff should support streaming data to the maximum extent possible


class Serializer(metaclass=abc.ABCMeta):
	"""Serializing protocol. Serialized data must be a string."""

	@classmethod
	@abc.abstractmethod
	def loads(cls, value):
		"""returns deserialized `value`."""
		pass

	@classmethod
	@abc.abstractmethod
	def dumps(cls, value):
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


class PrettyJSON(Serializer):
	"""json wrapper serializer that pretty-prints.
	Useful for human readable values and versioning.
	"""

	@classmethod
	def loads(cls, value):
		"""returns json deserialized `value`."""
		return json.loads(value)

	@classmethod
	def dumps(cls, value, indent=1):
		"""returns json serialized `value` (pretty-printed)."""
		return json.dumps(value, sort_keys=True, indent=indent)


class Stack(Serializer, list):
	"""represents a stack of serializers, applying each serializer in sequence."""

	def loads(self, value):
		"""Returns deserialized `value`."""
		for serializer in reversed(self):
			value = serializer.loads(value)
		return value

	def dumps(self, value):
		"""returns serialized `value`."""
		for serializer in self:
			value = serializer.dumps(value)
		return value


def deserialized_gen(serializer, iterable):
	"""Generator that yields deserialized objects from `iterable`."""
	# TODO: Remove this?
	for item in iterable:
		yield serializer.loads(item)


def serialized_gen(serializer, iterable):
	"""Generator that yields serialized objects from `iterable`."""
	# TODO: Remove this?
	for item in iterable:
		yield serializer.dumps(item)


class SerializerAdapter(objectstore.Adapter):
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

	# value serializer
	# override this with their own custom serializer on a class-wide or per-
	# instance basis. If you plan to store mostly strings, use NonSerializer.
	serializer = default_serializer  # type: Serializer
	
	
	def __init__(self, datastore: binarystore.Datastore,
	             serializer: typing.Union[Serializer, None] = None):
		"""Initializes internals and tests the serializer.
		
		Arguments
		---------
		datastore
			A child datastore for the ShimDatastore superclass.
		serializer
			A serializer object (responds to loads and dumps).
		"""
		super().__init__(datastore)
		if serializer:
			self.serializer = serializer

		# ensure serializer works
		test = {'value': repr(self)}
		error_str = 'Serializer error: serialized value does not match original'
		assert self.serializer.loads(self.serializer.dumps(test)) == test, error_str
	
	
	async def get(self, key):
		"""Return the object named by key or raise `KeyError` if it does not exist.
		Retrieves the value from the ``child_datastore``, and de-serializes
		it on the way out.
		
		Args:
			key: Key naming the object to retrieve
		
		Returns:
			object or None
		"""
		value = await (await self.child_datastore.get(key)).collect()  #FIXME
		return self.serializer.loads(value) if value is not None else None
	
	
	async def _put(self, key: key.Key, value: util.stream.ReceiveStream) -> None:
		"""Stores the object `value` named by `key`.
		Serializes values on the way in, and stores the serialized data into the
		``child_datastore``.
		
		Args:
			key: Key naming `value`
			value: the object to store.
		"""
		value_bytes = await value.collect()  #FIXME
		value_bytes = self.serializer.dumps(value_bytes)
		await self.child_datastore.put(key, value_bytes)

	async def query(self, query):
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
