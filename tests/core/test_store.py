import pytest
import trio.testing

import datastore
from datastore import (BinaryDictDatastore, BinaryNullDatastore, Key,
                       ObjectDictDatastore, ObjectNullDatastore, Query)


@pytest.mark.parametrize("NullDatastore", [BinaryNullDatastore, ObjectNullDatastore])
@trio.testing.trio_test
async def test_null(NullDatastore):
	s = NullDatastore()

	for c in range(1, 20):
		if NullDatastore is BinaryNullDatastore:
			v = str(c).encode()
		else:
			v = [c]
		k = Key(str(c))
		assert not await s.contains(k)
		with pytest.raises(KeyError):
			await s.get(k)
		
		await s.put(k, v)
		
		assert not await s.contains(k)
		with pytest.raises(KeyError):
			await s.get(k)

	for item in await s.query(Query(Key('/'))):
		raise Exception('Should not have found anything.')



@pytest.mark.parametrize("DictDatastore", [BinaryDictDatastore, ObjectDictDatastore])
@trio.testing.trio_test
async def test_dictionary(DatastoreTests, DictDatastore):
	s1 = DictDatastore()
	s2 = DictDatastore()
	s3 = DictDatastore()
	stores = [s1, s2, s3]
	
	await DatastoreTests(stores).subtest_simple()


@trio.testing.trio_test
async def test_dictionary_size():
	ds = datastore.BinaryDictDatastore()
	assert ds.datastore_stats().size == 0
	assert ds.datastore_stats().size_accuracy == "exact"
	
	await ds.put(datastore.Key("/bla"), b"abcdef")
	assert ds.datastore_stats().size == 6
	
	await ds.put(datastore.Key("/bla"), b"abc")
	assert ds.datastore_stats().size == 3
	
	await ds.put(datastore.Key("/blab"), b"abcdef")
	assert ds.datastore_stats().size == 9
	assert ds.datastore_stats().size_accuracy == "exact"
	
	await ds.delete(datastore.Key("/bla"))
	assert ds.datastore_stats().size == 6
	
	await ds.delete(datastore.Key("/blab"))
	assert ds.datastore_stats().size == 0
	assert ds.datastore_stats().size_accuracy == "exact"
