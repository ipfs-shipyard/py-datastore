import pickle
import unittest

import pytest

from datastore.core.key import Key
from datastore.core.basic import DictDatastore
from datastore.core.serialize import *
from datastore.core.test.test_basic import TestDatastore


def implements_serializer_interface(cls):
	return hasattr(cls, 'loads') and callable(cls.loads) \
	       and hasattr(cls, 'dumps') and callable(cls.dumps)


class TestSerialize(TestDatastore):

	def test_basic(self):
		value = 'test_value_%s' % self
		values_raw = [{'value': i} for i in range(0, 1000)]
		values_json = map(json.dumps, values_raw)

		# test protocol
		with pytest.raises(NotImplementedError):
		    Serializer.loads(value)
		with pytest.raises(NotImplementedError):
		    Serializer.dumps(value)

		# test non serializer
		assert NonSerializer.loads(value) == value
		assert NonSerializer.dumps(value) == value
		assert NonSerializer.loads(value) is value
		assert NonSerializer.dumps(value) is value

		# test generators
		values_serialized = list(serialized_gen(json, values_raw))
		values_deserialized = list(deserialized_gen(json, values_serialized))
		assert values_serialized == values_json
		assert values_deserialized == values_raw

		# test stack
		stack = Stack([json, MapSerializer])
		values_serialized = map(stack.dumps, values_raw)
		values_deserialized = map(stack.loads, values_serialized)
		assert values_deserialized == values_raw

	def subtest_serializer_shim(self, serializer, numelems=100):
		child = DictDatastore()
		shim = SerializerShimDatastore(child, serializer=serializer)

		values_raw = [{'value': i} for i in range(0, numelems)]

		values_serial = [serializer.dumps(v) for v in values_raw]
		values_deserial = [serializer.loads(v) for v in values_serial]
		assert values_deserial == values_raw

		for value in values_raw:
			key = Key(value['value'])
			value_serialized = serializer.dumps(value)

			# should not be there yet
			assert not shim.contains(key)
			assert shim.get(key) == None

			# put (should be there)
			shim.put(key, value)
			assert shim.contains(key)
			assert shim.get(key) == value

			# make sure underlying DictDatastore is storing the serialized value.
			assert shim.child_datastore.get(key) == value_serialized

			# delete (should not be there)
			shim.delete(key)
			assert not shim.contains(key)
			assert shim.get(key) == None

			# make sure manipulating underlying DictDatastore works equally well.
			shim.child_datastore.put(key, value_serialized)
			assert shim.contains(key)
			assert shim.get(key) == value

			shim.child_datastore.delete(key)
			assert not shim.contains(key)
			assert shim.get(key) == None

	def test_serializer_shim(self):
		self.subtest_serializer_shim(json)
		self.subtest_serializer_shim(PrettyJSON)
		self.subtest_serializer_shim(pickle)
		self.subtest_serializer_shim(MapSerializer)
		self.subtest_serializer_shim(default_serializer)  # module default

		self.subtest_serializer_shim(Stack([MapSerializer]))
		self.subtest_serializer_shim(Stack([json, MapSerializer]))
		self.subtest_serializer_shim(Stack([json, MapSerializer, pickle]))

	def test_interface_check_returns_true_for_valid_serializers(self):
		class S(object):
			def loads(self, foo):
				return foo

			def dumps(self, foo):
				return foo

		assert implements_serializer_interface(S)
		assert implements_serializer_interface(json)
		assert implements_serializer_interface(pickle)
		assert implements_serializer_interface(Serializer)

	def test_interface_check_returns_false_for_invalid_serializers(self):
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
