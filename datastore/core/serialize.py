import abc
import typing

from . import binarystore
from . import key as key_
from . import objectstore
from . import query as query_


class util:  # noqa
	from .util import metadata
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
		self.child_datastore = datastore
		self.serializer      = serializer

	
	async def get(self, key: key_.Key) -> util.stream.ReceiveChannel[T_co]:
		"""Returns the object named by key or raises `KeyError` if it does not exist.
		
		Retrieves the value from the ``child_datastore``, and de-serializes
		it on the way out.
		
		Arguments
		---------
		key
			Key naming the object to retrieve
		"""
		value = await (await self.child_datastore.get(key)).collect()  #FIXME
		return util.stream.receive_channel_from(self.serializer.loads(value))
	
	
	async def _put(self, key: key_.Key, value: util.stream.ReceiveChannel[T_co], *,
	               create: bool, replace: bool, **kwargs: typing.Any) -> None:
		"""Stores the objects of the given object stream *value* at name *key*
		
		Serializes values on the way in, and stores the serialized data into the
		``child_datastore``.
		
		Arguments
		---------
		key
			Key naming `value`
		value
			The objects to store
		create
			Create the given key if it does not exist?
		replace
			Replace the given key if it does exist?
		"""
		value_items = await value.collect()  #FIXME
		value_bytes = self.serializer.dumps(value_items)
		value_stream = util.stream.receive_stream_from(value_bytes)
		await self.child_datastore._put(key, value_stream, create=create, replace=replace, **kwargs)
	
	
	async def _put_new(self, prefix: key_.Key, value: util.stream.ReceiveChannel[T_co],
	                   **kwargs: typing.Any) -> key_.Key:
		"""Stores the objects of the given object stream *value* below name *prefix*
		
		Serializes values on the way in, and stores the serialized data into the
		``child_datastore``.
		
		Arguments
		---------
		prefix
			Key below which to create the the new target key
		value
			The objects to store
		"""
		value_items = await value.collect()  #FIXME
		value_bytes = self.serializer.dumps(value_items)
		value_stream = util.stream.receive_stream_from(value_bytes)
		return await self.child_datastore._put_new(prefix, value_stream, **kwargs)
	
	
	async def _put_new_indirect(self, prefix: key_.Key, **kwargs: typing.Any) \
	      -> objectstore.Datastore._PUT_NEW_INDIRECT_RT[T_co]:
		"""Stores the objects of the given object stream *value* below name *prefix*
		
		Serializes values on the way in, and stores the serialized data into the
		``child_datastore``.
		
		Arguments
		---------
		prefix
			Key below which to create the the new target key
		value
			The objects to store
		"""
		key, callback = await self.child_datastore._put_new_indirect(prefix, **kwargs)
		
		async def callback_wrapper(value: util.stream.ReceiveChannel[T_co]) -> None:
			value_items = await value.collect()  #FIXME
			value_bytes = self.serializer.dumps(value_items)
			value_stream = util.stream.receive_stream_from(value_bytes)
			await callback(value_stream)
		return key, callback_wrapper
	
	
	async def query(self, query: query_.Query) -> query_.Cursor:
		"""Returns an iterable of objects matching criteria expressed in `query`
		De-serializes values on the way out, using a :ref:`deserialized_gen` to
		avoid incurring the cost of de-serializing all data at once, or ever, if
		iteration over results does not finish (subject to order generator
		constraint).
		
		Arguments
		---------
		query
			Query object describing the objects to return.
		"""

		# run the query on the child datastore
		#FIXME: Would need to implement this
		cursor = await self.child_datastore.query(query)  # type: ignore

		# chain the deserializing generator to the cursor's result set iterable
		result = cursor._iterable
		cursor._iterable = []
		for field in result:
			cursor._iterable.extend(self.serializer.loads(field))

		return cursor  # type: ignore[no-any-return]
	
	
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
	
	
	async def rename(self, key1: key_.Key, key2: key_.Key, *, replace: bool = True) -> None:
		"""Moves the content at name *key1* to *key2*
		
		Arguments
		---------
		key1
			The key to rename, must exist
		key2
			The new name of the key; if *replace* is ``False``, a key of the
			same name may not already exist
		replace
			Should an existing key at name *key2* be replaced?
		
		Raises
		------
		KeyError
			Key *key1* does not exist in the child datastore
		KeyError
			Key *key2* already exists in the child datastore, but *replace* was not ``True``
		RuntimeError
			An internal error occurred in the child datastore
		NotImplementedError
			The child datastore does not support this operation
		"""
		await self.child_datastore.rename(key1, key2, replace=replace)
	
	
	async def stat(self, key: key_.Key) -> util.metadata.ChannelMetadata:
		"""Returns whether an object named by `key` exists
		
		The default implementation pays the cost of a get. Some datastore
		implementations may optimize this.
		
		Arguments
		---------
		key
			Key naming the object to check.
		"""
		metadata: util.metadata.StreamMetadata = await self.child_datastore.stat(key)
		return util.metadata.ChannelMetadata(
			atime = metadata.atime,
			mtime = metadata.mtime,
			btime = metadata.btime
		)
	
	
	def datastore_stats(self, selector: key_.Key = None, *, _seen: typing.Set[int] = None) \
	    -> util.metadata.DatastoreMetadata:
		"""Returns metadata of the child datastore
		
		Arguments
		---------
		selector
			Used to select the backing store for some datastore adapters (such as
			mount) that have more than one backing store
			
			If this is ``None``, the result will be the sum of all datastores
			attached to this adapter.
		_seen
			Set of Python object IDs of datastores already visited while gathering
			stats from datastore adapters with more than one then one backing store
			
			This is required to ensure that no backing datastore is counted more
			than once if `selector` is ``None``.
		
		Raises
		------
		RuntimeError
			An internal error occurred in the child datastore
		"""
		_seen = _seen if _seen is not None else set()
		
		if id(self.child_datastore) in _seen:
			return util.metadata.DatastoreMetadata.IGNORE
		_seen.add(id(self.child_datastore))
		
		return self.child_datastore.datastore_stats(selector, _seen=_seen)


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
