import logging
import typing

import pytest
import trio

import datastore
from tests.adapter.conftest import make_datastore_test_params


@pytest.mark.parametrize(*make_datastore_test_params("sharded"))
@trio.testing.trio_test
async def test_sharded_simple(DatastoreTests, Adapter, DictDatastore, encode_fn):
	s1 = DictDatastore()
	s2 = DictDatastore()
	s3 = DictDatastore()
	s4 = DictDatastore()
	s5 = DictDatastore()
	stores = [s1, s2, s3, s4, s5]
	
	async with Adapter(stores) as sharded:
		await DatastoreTests([sharded], test_put_new=False).subtest_simple()



@pytest.mark.parametrize(*make_datastore_test_params("sharded"))
@trio.testing.trio_test
async def test_sharded(DatastoreTests, Adapter, DictDatastore, encode_fn):
	numelems = 100
	
	s1 = DictDatastore()
	s2 = DictDatastore()
	s3 = DictDatastore()
	s4 = DictDatastore()
	s5 = DictDatastore()
	stores = [s1, s2, s3, s4, s5]

	def hash(key):
		return int(key.name) * len(stores) // numelems

	def sumlens(stores):
		return sum(map(lambda s: len(s), stores))

	async def checkFor(key, value, sharded, shard=None):
		correct_shard = sharded._stores[hash(key) % len(sharded._stores)]

		for s in sharded._stores:
			if shard and s == shard:
				assert await s.contains(key)
				assert await s.get_all(key) == encode_fn(value)
			else:
				assert not await s.contains(key)

		if correct_shard == shard:
			assert await sharded.contains(key)
			assert await sharded.get_all(key) == encode_fn(value)
		else:
			assert not await sharded.contains(key)

	async with Adapter(stores, sharding_fn=hash) as sharded:
		assert sumlens(stores) == 0
		# test all correct.
		for value in range(0, numelems):
			key = datastore.Key(f"/fdasfdfdsafdsafdsa/{value}")
			shard = stores[hash(key) % len(stores)]
			await checkFor(key, value, sharded)
			await shard.put(key, encode_fn(value))
			await checkFor(key, value, sharded, shard)
		assert sumlens(stores) == numelems

		# ensure its in the same spots.
		for i in range(0, numelems):
			key = datastore.Key(f"/fdasfdfdsafdsafdsa/{value}")
			shard = stores[hash(key) % len(stores)]
			await checkFor(key, value, sharded, shard)
			await shard.put(key, encode_fn(value))
			await checkFor(key, value, sharded, shard)
		assert sumlens(stores) == numelems

		# ensure its in the same spots.
		for value in range(0, numelems):
			key = datastore.Key(f"/fdasfdfdsafdsafdsa/{value}")
			shard = stores[hash(key) % len(stores)]
			await checkFor(key, value, sharded, shard)
			await sharded.put(key, encode_fn(value))
			await checkFor(key, value, sharded, shard)
		assert sumlens(stores) == numelems

		# ensure its in the same spots.
		for value in range(0, numelems):
			key = datastore.Key(f"/fdasfdfdsafdsafdsa/{value}")
			shard = stores[hash(key) % len(stores)]
			await checkFor(key, value, sharded, shard)
			if value % 2 == 0:
				await shard.delete(key)
			else:
				await sharded.delete(key)
			await checkFor(key, value, sharded)
		assert sumlens(stores) == 0

		# try out adding it to the wrong shards.
		for value in range(0, numelems):
			key = datastore.Key(f"/fdasfdfdsafdsafdsa/{value}")
			incorrect_shard = stores[(hash(key) + 1) % len(stores)]
			await checkFor(key, value, sharded)
			await incorrect_shard.put(key, encode_fn(value))
			await checkFor(key, value, sharded, incorrect_shard)
		assert sumlens(stores) == numelems

		# ensure its in the same spots.
		for value in range(0, numelems):
			key = datastore.Key(f"/fdasfdfdsafdsafdsa/{value}")
			incorrect_shard = stores[(hash(key) + 1) % len(stores)]
			await checkFor(key, value, sharded, incorrect_shard)
			await incorrect_shard.put(key, encode_fn(value))
			await checkFor(key, value, sharded, incorrect_shard)
		assert sumlens(stores) == numelems

		# this wont do anything
		for value in range(0, numelems):
			key = datastore.Key(f"/fdasfdfdsafdsafdsa/{value}")
			incorrect_shard = stores[(hash(key) + 1) % len(stores)]
			await checkFor(key, value, sharded, incorrect_shard)
			with pytest.raises(KeyError):
				await sharded.delete(key)
			await checkFor(key, value, sharded, incorrect_shard)
		assert sumlens(stores) == numelems

		# this will place it correctly.
		for value in range(0, numelems):
			key = datastore.Key(f"/fdasfdfdsafdsafdsa/{value}")
			incorrect_shard = stores[(hash(key) + 1) % len(stores)]
			correct_shard = stores[(hash(key)) % len(stores)]
			await checkFor(key, value, sharded, incorrect_shard)
			await sharded.put(key, encode_fn(value))
			await incorrect_shard.delete(key)
			await checkFor(key, value, sharded, correct_shard)
		assert sumlens(stores) == numelems

		# this will place it correctly.
		for value in range(0, numelems):
			key = datastore.Key(f"/fdasfdfdsafdsafdsa/{value}")
			correct_shard = stores[(hash(key)) % len(stores)]
			await checkFor(key, value, sharded, correct_shard)
			await sharded.delete(key)
			await checkFor(key, value, sharded)
		assert sumlens(stores) == 0
		
		# Datastores should still be there
		assert len(sharded._stores) == len(stores)
	
	# Datastores should have been cleaned up by aexit
	assert len(sharded._stores) == 0
