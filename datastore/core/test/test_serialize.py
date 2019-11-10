import pytest
import trio.testing

import datastore
import datastore.serializer.json
import datastore.serializer.pickle


def implements_serializer_interface(cls):
	return hasattr(cls, 'loads') and callable(cls.loads) \
	       and hasattr(cls, 'dumps') and callable(cls.dumps)


def test_basic(DatastoreTests):
	serializer = datastore.serializer.json.Serializer()
	
	values_orig = [{"value": i} for i in range(0, 1000)]
	values_json = [serializer.dumps([item]) for item in values_orig]
	
	# test generators
	values_serialized = [serializer.dumps([item]) for item in values_orig]
	values_deserialized = [serializer.loads(item)[0] for item in values_serialized]
	assert values_serialized == values_json
	assert values_deserialized == values_orig


async def subtest_serializer_shim(serializer=None, numelems=100):
	child = datastore.BinaryDictDatastore()
	shim = datastore.SerializerAdapter(child, serializer=serializer)
	
	values_raw = [{"value": i} for i in range(0, numelems)]
	
	values_serial = [serializer.dumps([v]) for v in values_raw]
	values_deserial = [serializer.loads(v)[0] for v in values_serial]
	assert values_deserial == values_raw
	
	for value in values_raw:
		key = datastore.Key(value["value"])
		value_serialized = serializer.dumps([value])
		
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


@trio.testing.trio_test
async def test_serializer_shim():
	#XXX: Testing directly with the `json` module is not going to work as that
	#     module expects input strings to be text, rather then binary
	await subtest_serializer_shim(datastore.serializer.json.Serializer())
	await subtest_serializer_shim(datastore.serializer.json.PrettySerializer())
	await subtest_serializer_shim(datastore.serializer.pickle.Serializer())


def test_interface_check_returns_true_for_valid_serializers():
	class S(object):
		def loads(self, foo):
			return foo

		def dumps(self, foo):
			return foo

	assert implements_serializer_interface(S)
	assert implements_serializer_interface(datastore.serializer.json.Serializer())
	assert implements_serializer_interface(datastore.serializer.json.PrettySerializer())
	assert implements_serializer_interface(datastore.serializer.pickle.Serializer())
	assert implements_serializer_interface(datastore.abc.Serializer)


def test_interface_check_returns_false_for_invalid_serializers():
	class S1(object):
		pass

	class S2(object):
		def loads(self, foo):
			return foo

	class S3(object):
		def dumps(self, foo):
			return foo

	class S4(object):
		def dumps(self, foo):
			return foo

	class S5(object):
		loads = 'loads'
		dumps = 'dumps'

	assert not implements_serializer_interface(S1)
	assert not implements_serializer_interface(S2)
	assert not implements_serializer_interface(S3)
	assert not implements_serializer_interface(S4)
	assert not implements_serializer_interface(S5)
