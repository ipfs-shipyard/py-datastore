import logging
import typing

import pytest
import trio.testing

import datastore
from tests import conftest
from tests.adapter.conftest import make_datastore_test_params


@pytest.mark.parametrize(*make_datastore_test_params("tiered"))
@trio.testing.trio_test
async def test_tiered_simple(DatastoreTests, Adapter, DictDatastore, encode_fn):
	s1 = DictDatastore()
	s2 = DictDatastore()
	s3 = DictDatastore()
	async with Adapter([s1, s2, s3]) as ts:
		await DatastoreTests([ts]).subtest_simple()


@pytest.mark.parametrize(*make_datastore_test_params("tiered"))
@trio.testing.trio_test
async def test_tiered(DatastoreTests, Adapter, DictDatastore, encode_fn):
	s1 = DictDatastore()
	s2 = DictDatastore()
	s3 = DictDatastore()
	async with Adapter([s1, s2, s3]) as ts:
		k1 = datastore.Key('1')
		k2 = datastore.Key('2')
		k3 = datastore.Key('3')

		await s1.put(k1, encode_fn('1'))
		await s2.put(k2, encode_fn('2'))
		await s3.put(k3, encode_fn('3'))

		assert await s1.contains(k1)
		assert not await s2.contains(k1)
		assert not await s3.contains(k1)
		assert await ts.contains(k1)

		assert await ts.get_all(k1) == encode_fn('1')
		assert await s1.get_all(k1) == encode_fn('1')
		assert not await s2.contains(k1)
		assert not await s3.contains(k1)

		assert not await s1.contains(k2)
		assert await s2.contains(k2)
		assert not await s3.contains(k2)
		assert await ts.contains(k2)

		assert await s2.get_all(k2) == encode_fn('2')
		assert not await s1.contains(k2)
		assert not await s3.contains(k2)

		# Read value from TS (where it will be found in T2) and check whether it was
		# copied into T1 because of this
		assert await ts.get_all(k2) == encode_fn('2')
		assert await s1.get_all(k2) == encode_fn('2')
		assert await s2.get_all(k2) == encode_fn('2')
		assert not await s3.contains(k2)

		assert not await s1.contains(k3)
		assert not await s2.contains(k3)
		assert await s3.contains(k3)
		assert await ts.contains(k3)

		assert await s3.get_all(k3) == encode_fn('3')
		assert not await s1.contains(k3)
		assert not await s2.contains(k3)

		assert await ts.get_all(k3) == encode_fn('3')
		assert await s1.get_all(k3) == encode_fn('3')
		assert await s2.get_all(k3) == encode_fn('3')
		assert await s3.get_all(k3) == encode_fn('3')

		await ts.delete(k1)
		await ts.delete(k2)
		await ts.delete(k3)

		assert not await ts.contains(k1)
		assert not await ts.contains(k2)
		assert not await ts.contains(k3)
		
		# Datastores should still be there
		assert len(ts._stores) == 3
	
	# Datastores should have been cleaned up by aexit
	assert len(ts._stores) == 0


@pytest.mark.parametrize(*make_datastore_test_params("tiered"))
@trio.testing.trio_test
async def test_tiered_half_fail(DatastoreTests, Adapter, DictDatastore, encode_fn):
	s1 = DictDatastore()
	s2 = DictDatastore()
	async with Adapter([s1, s2]) as ts:
		k1 = datastore.Key("/a")
		k2 = datastore.Key("/b")
		
		assert not await s1.contains(k1)
		assert not await s1.contains(k2)
		assert not await s2.contains(k1)
		assert not await s2.contains(k2)
		
		# Write value into only one datastore
		await s1.put(k1, encode_fn("1"))
		await s2.put(k2, encode_fn("1"))
		
		assert await s1.contains(k1)
		assert not await s1.contains(k2)
		assert not await s2.contains(k1)
		assert await s2.contains(k2)
		
		# Write non-replacingly to both
		with conftest.raises(KeyError):
			await ts.put(k1, encode_fn("2"), replace=False)
		with conftest.raises(KeyError):
			await ts.put(k2, encode_fn("2"), replace=False)
		
		assert await s1.contains(k1)
		assert not await s1.contains(k2)
		assert not await s2.contains(k1)
		assert await s2.contains(k2)
		
		# Ensure red back value didn't change
		assert await ts.get_all(k1) == encode_fn("1")
		assert await ts.get_all(k2) == encode_fn("1")
		
		assert await s1.contains(k1)
		assert await s1.contains(k2)
		assert not await s2.contains(k1)
		assert await s2.contains(k2)
		
		# Write new value to both stores normally
		await ts.put(k1, encode_fn("2"))
		await ts.put(k2, encode_fn("2"))
		
		assert await s1.contains(k1)
		assert await s1.contains(k2)
		assert await s2.contains(k1)
		assert await s2.contains(k2)
		
		# Ensure red back is now changed
		assert await ts.get_all(k1) == encode_fn("2")
		assert await ts.get_all(k2) == encode_fn("2")
