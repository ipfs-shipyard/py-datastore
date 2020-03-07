import pytest
import trio.testing

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
