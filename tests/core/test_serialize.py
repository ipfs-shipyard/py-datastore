import functools

import pytest
import trio.testing

import datastore
import datastore.serializer.json
import datastore.serializer.pickle

SERIALIZERS = [
	datastore.serializer.json.Serializer,
	datastore.serializer.json.PrettySerializer,
	datastore.serializer.pickle.Serializer,
	functools.partial(datastore.serializer.pickle.Serializer, protocol=2),
	functools.partial(datastore.serializer.pickle.Serializer, protocol=0),
]


@pytest.mark.parametrize("Serializer", SERIALIZERS)
@trio.testing.trio_test
async def test_serializer_basic(Serializer, numelems=1000):
	serializer = Serializer()
	
	values_raw = [{"value": i} for i in range(0, numelems)]
	
	values_serial = []
	async for chunk in serializer.serialize(datastore.util.receive_channel_from(values_raw)):
		values_serial.append(chunk)
	
	values_deserial_1 = []
	async for value in serializer.parse(datastore.util.receive_stream_from(values_serial)):
		values_deserial_1.append(value)
	
	values_deserial_2 = []
	async for value in serializer.parse(datastore.util.receive_stream_from(b"".join(values_serial))):
		values_deserial_2.append(value)
	
	assert values_deserial_1 == values_deserial_2 == values_raw


@pytest.mark.parametrize("Serializer", SERIALIZERS)
@trio.testing.trio_test
async def test_serializer_shim(Serializer, numelems=100):
	child = datastore.BinaryDictDatastore()
	shim = datastore.SerializerAdapter(child, serializer=Serializer())
	
	values_raw = [{"value": i} for i in range(0, numelems)]

	for value in values_raw:
		key = datastore.Key(value["value"])
		value_serialized = b""
		async for chunk in shim.serializer.serialize(datastore.util.receive_channel_from([value])):
			value_serialized += chunk
		
		# should not be there yet
		assert not await shim.contains(key)
		with pytest.raises(KeyError):
			assert await shim.get_all(key)
		
		# put (should be there)
		await shim.put(key, [value])
		assert await shim.contains(key)
		assert await shim.get_all(key) == [value]
		
		# make sure underlying DictDatastore is storing the serialized value.
		assert await shim.child_datastore.get_all(key) == value_serialized
		
		# delete (should not be there afterwards)
		await shim.delete(key)
		assert not await shim.contains(key)
		with pytest.raises(KeyError):
			await shim.get_all(key)
		
		# make sure manipulating underlying DictDatastore works equally well.
		await shim.child_datastore.put(key, value_serialized)
		assert await shim.contains(key)
		assert await shim.get_all(key) == [value]
		
		await shim.child_datastore.delete(key)
		assert not await shim.contains(key)
		with pytest.raises(KeyError):
			await shim.get_all(key)
