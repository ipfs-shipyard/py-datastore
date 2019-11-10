import importlib
import logging
import unittest
from typing import Callable, List, Tuple, Type, TypeVar, Union, TYPE_CHECKING

import pytest
import trio.testing

from datastore.core import BinaryDictDatastore, ObjectDictDatastore
from datastore.core import BinaryNullDatastore, ObjectNullDatastore
from datastore.core.key import Key
from datastore.core.query import Query

import datastore.adapter.logging


T_co = TypeVar("T_co", covariant=True)


def get_all_type_args(adapter: str, subname: str = "") \
		-> Tuple[str, Tuple[
			Tuple[
				Type[datastore.abc.BinaryAdapter],
				Type[Union[BinaryDictDatastore, ObjectDictDatastore]],
				Callable[[T_co], bytes]
			],
			Tuple[
				Type[datastore.abc.ObjectAdapter],
				Type[ObjectDictDatastore],
				Callable[[T_co], List[T_co]]
			]
		]]:
	mod = importlib.import_module(f"datastore.adapter.{adapter}")
	
	def encode_bin(value: T_co) -> bytes:
		return str(value).encode()
	
	def encode_obj(value: T_co) -> List[T_co]:
		return [value]
	
	return ("Adapter, DictDatastore, encode_fn", (
		(getattr(mod, f"Binary{subname}Adapter"), BinaryDictDatastore, encode_bin),  # type: ignore
		(getattr(mod, f"Object{subname}Adapter"), ObjectDictDatastore, encode_obj)   # type: ignore
	))


@pytest.mark.parametrize("NullDatastore", [BinaryNullDatastore, ObjectNullDatastore])
@trio.testing.trio_test
async def test_null(NullDatastore):
	s = NullDatastore()

	for c in range(1, 20):
		if NullDatastore is BinaryNullDatastore:
			v = str(c).encode()
		else:
			v = [c]
		k = Key(c)
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



####################
# logging.*Adapter #
####################

_Logger = logging.Logger
if not TYPE_CHECKING:
	_Loger = logging.getLoggerClass()

class NullLogger(_Logger):
		def debug(self, *args, **kwargs): pass
		def info(self, *args, **kwargs): pass
		def warning(self, *args, **kwargs): pass
		def error(self, *args, **kwargs): pass
		def critical(self, *args, **kwargs): pass


@pytest.mark.parametrize(*get_all_type_args("logging"))
@trio.testing.trio_test
async def test_logging_simple(DatastoreTests, Adapter, DictDatastore, encode_fn):
	s1 = Adapter(DictDatastore(), logger=NullLogger('null'))
	s2 = Adapter(DictDatastore())
	await DatastoreTests([s1, s2]).subtest_simple()




#########################
# keytransform.*Adapter #
#########################

@pytest.mark.parametrize(*get_all_type_args("keytransform"))
@trio.testing.trio_test
async def test_keytransform_simple(DatastoreTests, Adapter, DictDatastore, encode_fn):
	s1 = Adapter(DictDatastore())
	s2 = Adapter(DictDatastore())
	s3 = Adapter(DictDatastore())
	stores = [s1, s2, s3]

	await DatastoreTests(stores).subtest_simple()

@pytest.mark.parametrize(*get_all_type_args("keytransform"))
@trio.testing.trio_test
async def test_keytransform_reverse_transform(Adapter, DictDatastore, encode_fn):
	def transform(key):
		return key.reverse

	ds = DictDatastore()
	kt = Adapter(ds, key_transform=transform)

	k1 = Key('/a/b/c')
	k2 = Key('/c/b/a')
	assert not await ds.contains(k1)
	assert not await ds.contains(k2)
	assert not await kt.contains(k1)
	assert not await kt.contains(k2)

	await ds.put(k1, encode_fn('abc'))
	assert await ds.get_all(k1) == encode_fn('abc')
	assert not await ds.contains(k2)
	assert not await kt.contains(k1)
	assert await kt.get_all(k2) == encode_fn('abc')

	await kt.put(k1, encode_fn('abc'))
	assert await ds.get_all(k1) == encode_fn('abc')
	assert await ds.get_all(k2) == encode_fn('abc')
	assert await kt.get_all(k1) == encode_fn('abc')
	assert await kt.get_all(k2) == encode_fn('abc')

	await ds.delete(k1)
	assert not await ds.contains(k1)
	assert await ds.get_all(k2) == encode_fn('abc')
	assert await kt.get_all(k1) == encode_fn('abc')
	assert not await kt.contains(k2)

	await kt.delete(k1)
	assert not await ds.contains(k1)
	assert not await ds.contains(k2)
	assert not await kt.contains(k1)
	assert not await kt.contains(k2)

@pytest.mark.parametrize(*get_all_type_args("keytransform"))
@trio.testing.trio_test
async def test_keytransform_lowercase_transform(Adapter, DictDatastore, encode_fn):
	def transform(key):
		return Key(str(key).lower())
	
	ds = DictDatastore()
	lds = Adapter(ds, key_transform=transform)
	
	k1 = Key('hello')
	k2 = Key('HELLO')
	k3 = Key('HeLlo')
	
	await ds.put(k1, encode_fn('world'))
	await ds.put(k2, encode_fn('WORLD'))
	
	assert await ds.get_all(k1) == encode_fn('world')
	assert await ds.get_all(k2) == encode_fn('WORLD')
	assert not await ds.contains(k3)
	
	assert await lds.get_all(k1) == encode_fn('world')
	assert await lds.get_all(k2) == encode_fn('world')
	assert await lds.get_all(k3) == encode_fn('world')
	
	async def test(key, val):
		await lds.put(key, val)
		assert await lds.get_all(k1) == val
		assert await lds.get_all(k2) == val
		assert await lds.get_all(k3) == val
	
	await test(k1, encode_fn('a'))
	await test(k2, encode_fn('b'))
	await test(k3, encode_fn('c'))



#####################################
# keytransform.*LowercaseKeyAdapter #
#####################################


@pytest.mark.parametrize(*get_all_type_args("keytransform", "LowercaseKey"))
@trio.testing.trio_test
async def test_lowercase_key_simple(DatastoreTests, Adapter, DictDatastore, encode_fn):
	s1 = Adapter(DictDatastore())
	s2 = Adapter(DictDatastore())
	s3 = Adapter(Adapter(DictDatastore()))
	stores = [s1, s2, s3]
	
	await DatastoreTests(stores).subtest_simple()

@pytest.mark.parametrize(*get_all_type_args("keytransform", "LowercaseKey"))
@trio.testing.trio_test
async def test_lowercase_key(Adapter, DictDatastore, encode_fn):
	ds = DictDatastore()
	lds = Adapter(ds)
	
	k1 = Key('hello')
	k2 = Key('HELLO')
	k3 = Key('HeLlo')
	
	await ds.put(k1, encode_fn('world'))
	await ds.put(k2, encode_fn('WORLD'))
	
	assert await ds.get_all(k1) == encode_fn('world')
	assert await ds.get_all(k2) == encode_fn('WORLD')
	assert not await ds.contains(k3)
	
	assert await lds.get_all(k1) == encode_fn('world')
	assert await lds.get_all(k2) == encode_fn('world')
	assert await lds.get_all(k3) == encode_fn('world')
	
	async def test(key, val):
		await lds.put(key, val)
		assert await lds.get_all(k1) == val
		assert await lds.get_all(k2) == val
		assert await lds.get_all(k3) == val
	
	await test(k1, encode_fn('a'))
	await test(k2, encode_fn('b'))
	await test(k3, encode_fn('c'))



##################################
# keytransform.*NamespaceAdapter #
##################################

@pytest.mark.parametrize(*get_all_type_args("keytransform", "Namespace"))
@trio.testing.trio_test
async def test_namespace_simple(DatastoreTests, Adapter, DictDatastore, encode_fn):
	s1 = Adapter(Key('a'), DictDatastore())
	s2 = Adapter(Key('b'), DictDatastore())
	s3 = Adapter(Key('c'), DictDatastore())
	stores = [s1, s2, s3]

	await DatastoreTests(stores).subtest_simple()

@pytest.mark.parametrize(*get_all_type_args("keytransform", "Namespace"))
@trio.testing.trio_test
async def test_namespace(Adapter, DictDatastore, encode_fn):
	k1 = Key('/c/d')
	k2 = Key('/a/b')
	k3 = Key('/a/b/c/d')

	ds = DictDatastore()
	nd = Adapter(k2, ds)

	await ds.put(k1, encode_fn('cd'))
	await ds.put(k3, encode_fn('abcd'))

	assert await ds.get_all(k1) == encode_fn('cd')
	assert not await ds.contains(k2)
	assert await ds.get_all(k3) == encode_fn('abcd')

	assert await nd.get_all(k1) == encode_fn('abcd')
	assert not await nd.contains(k2)
	assert not await nd.contains(k3)

	async def test(key, val):
		await nd.put(key, val)
		assert await nd.get_all(key) == val
		assert not await ds.contains(key)
		assert not await nd.contains(k2.child(key))
		assert await ds.get_all(k2.child(key)) == val

	for i in range(0, 10):
		await test(Key(str(i)), encode_fn(f"val{i}"))



###################################
# keytransform.*NestedPathAdapter #
###################################

async def subtest_nested_path_ds(Adapter, DictDatastore, encode_fn, **kwargs):
	k1 = kwargs.pop('k1')
	k2 = kwargs.pop('k2')
	k3 = kwargs.pop('k3')
	k4 = kwargs.pop('k4')
	
	ds = DictDatastore()
	np = Adapter(ds, **kwargs)
	
	assert not await ds.contains(k1)
	assert not await ds.contains(k2)
	assert not await ds.contains(k3)
	assert not await ds.contains(k4)
	
	assert not await np.contains(k1)
	assert not await np.contains(k2)
	assert not await np.contains(k3)
	assert not await np.contains(k4)
	
	await np.put(k1, encode_fn(k1))
	await np.put(k2, encode_fn(k2))
	
	assert not await ds.contains(k1)
	assert not await ds.contains(k2)
	assert await ds.contains(k3)
	assert await ds.contains(k4)
	
	assert await np.contains(k1)
	assert await np.contains(k2)
	assert not await np.contains(k3)
	assert not await np.contains(k4)
	
	assert await np.get_all(k1) == encode_fn(k1)
	assert await np.get_all(k2) == encode_fn(k2)
	assert await ds.get_all(k3) == encode_fn(k1)
	assert await ds.get_all(k4) == encode_fn(k2)
	
	await np.delete(k1)
	await np.delete(k2)
	
	assert not await ds.contains(k1)
	assert not await ds.contains(k2)
	assert not await ds.contains(k3)
	assert not await ds.contains(k4)
	
	assert not await np.contains(k1)
	assert not await np.contains(k2)
	assert not await np.contains(k3)
	assert not await np.contains(k4)
	
	await ds.put(k3, encode_fn(k1))
	await ds.put(k4, encode_fn(k2))
	
	assert not await ds.contains(k1)
	assert not await ds.contains(k2)
	assert await ds.contains(k3)
	assert await ds.contains(k4)
	
	assert await np.contains(k1)
	assert await np.contains(k2)
	assert not await np.contains(k3)
	assert not await np.contains(k4)
	
	assert await np.get_all(k1) == encode_fn(k1)
	assert await np.get_all(k2) == encode_fn(k2)
	assert await ds.get_all(k3) == encode_fn(k1)
	assert await ds.get_all(k4) == encode_fn(k2)
	
	await ds.delete(k3)
	await ds.delete(k4)
	
	assert not await ds.contains(k1)
	assert not await ds.contains(k2)
	assert not await ds.contains(k3)
	assert not await ds.contains(k4)
	
	assert not await np.contains(k1)
	assert not await np.contains(k2)
	assert not await np.contains(k3)
	assert not await np.contains(k4)

@pytest.mark.parametrize(*get_all_type_args("keytransform", "NestedPath"))
@trio.testing.trio_test
async def test_nested_path_simple(DatastoreTests, Adapter, DictDatastore, encode_fn):
	s1 = Adapter(DictDatastore())
	s2 = Adapter(DictDatastore(), depth=2)
	s3 = Adapter(DictDatastore(), length=2)
	s4 = Adapter(DictDatastore(), length=1, depth=2)
	stores = [s1, s2, s3, s4]

	await DatastoreTests(stores).subtest_simple()

@pytest.mark.parametrize(*get_all_type_args("keytransform", "NestedPath"))
def test_nested_path_gen(Adapter, DictDatastore, encode_fn):
	def test(depth, length, expected):
		nested = Adapter.nested_path('abcdefghijk', depth, length)
		assert nested == expected

	test(3, 2, 'ab/cd/ef')
	test(4, 2, 'ab/cd/ef/gh')
	test(3, 4, 'abcd/efgh/ijk')
	test(1, 4, 'abcd')
	test(3, 10, 'abcdefghij/k')

@pytest.mark.parametrize(*get_all_type_args("keytransform", "NestedPath"))
@trio.testing.trio_test
async def test_nested_path_3_2(Adapter, DictDatastore, encode_fn):
	opts = {}
	opts['k1'] = Key('/abcdefghijk')
	opts['k2'] = Key('/abcdefghijki')
	opts['k3'] = Key('/ab/cd/ef/abcdefghijk')
	opts['k4'] = Key('/ab/cd/ef/abcdefghijki')
	opts['depth'] = 3
	opts['length'] = 2

	await subtest_nested_path_ds(Adapter, DictDatastore, encode_fn, **opts)

@pytest.mark.parametrize(*get_all_type_args("keytransform", "NestedPath"))
@trio.testing.trio_test
async def test_nested_path_5_3(Adapter, DictDatastore, encode_fn):
	opts = {}
	opts['k1'] = Key('/abcdefghijk')
	opts['k2'] = Key('/abcdefghijki')
	opts['k3'] = Key('/abc/def/ghi/jka/bcd/abcdefghijk')
	opts['k4'] = Key('/abc/def/ghi/jki/abc/abcdefghijki')
	opts['depth'] = 5
	opts['length'] = 3

	await subtest_nested_path_ds(Adapter, DictDatastore, encode_fn, **opts)

@pytest.mark.parametrize(*get_all_type_args("keytransform", "NestedPath"))
@trio.testing.trio_test
async def test_nested_path_keyfn(Adapter, DictDatastore, encode_fn):
	opts = {}
	opts['k1'] = Key('/abcdefghijk')
	opts['k2'] = Key('/abcdefghijki')
	opts['k3'] = Key('/kj/ih/gf/abcdefghijk')
	opts['k4'] = Key('/ik/ji/hg/abcdefghijki')
	opts['depth'] = 3
	opts['length'] = 2
	opts['key_fn'] = lambda key: key.name[::-1]

	await subtest_nested_path_ds(Adapter, DictDatastore, encode_fn, **opts)



####################################
# directory.ObjectDirectorySupport #
####################################

import datastore.adapter.directory


class ObjectDirectoryDictDatastore(
		datastore.adapter.directory.ObjectDirectorySupport,
		ObjectDictDatastore
):
	pass


@trio.testing.trio_test
async def test_directory_simple(DatastoreTests):
	s1 = ObjectDirectoryDictDatastore()
	s2 = ObjectDirectoryDictDatastore()
	await DatastoreTests([s1, s2]).subtest_simple()

@trio.testing.trio_test
async def test_directory_init():
	ds = ObjectDirectoryDictDatastore()

	# initialize directory at /foo
	dir_key = Key('/foo')
	await ds.directory(dir_key)
	assert await ds.get_all(dir_key) == []

	# can add to dir
	bar_key = Key('/foo/bar')
	await ds.directory_add(dir_key, bar_key)
	assert await ds.get_all(dir_key) == [str(bar_key)]

	# re-init does not wipe out directory at /foo
	dir_key = Key('/foo')
	with pytest.raises(KeyError):
		await ds.directory(dir_key, exist_ok=False)
	await ds.directory(dir_key, exist_ok=True)
	assert await ds.get_all(dir_key) == [str(bar_key)]

@trio.testing.trio_test
async def test_directory_basic():
	ds = ObjectDirectoryDictDatastore()

	# initialize directory at /foo
	dir_key = Key('/foo')
	await ds.directory(dir_key)

	# adding directory entries
	bar_key = Key('/foo/bar')
	baz_key = Key('/foo/baz')
	await ds.directory_add(dir_key, bar_key)
	await ds.directory_add(dir_key, baz_key)
	keys = [key async for key in ds.directory_read(dir_key)]
	assert keys == [bar_key, baz_key]

	# removing directory entries
	await ds.directory_remove(dir_key, bar_key)
	keys = [key async for key in ds.directory_read(dir_key)]
	assert keys == [baz_key]

	await ds.directory_remove(dir_key, baz_key)
	keys = [key async for key in ds.directory_read(dir_key)]
	assert keys == []

	# generator
	with pytest.raises(StopAsyncIteration):
		gen = ds.directory_read(dir_key).__aiter__()
		await gen.__anext__()

@trio.testing.trio_test
async def test_directory_double_add():
	ds = ObjectDirectoryDictDatastore()

	# initialize directory at /foo
	dir_key = Key('/foo')
	await ds.directory(dir_key)

	# adding directory entries
	bar_key = Key('/foo/bar')
	baz_key = Key('/foo/baz')
	await ds.directory_add(dir_key, bar_key)
	await ds.directory_add(dir_key, baz_key)
	await ds.directory_add(dir_key, bar_key)
	await ds.directory_add(dir_key, baz_key)
	await ds.directory_add(dir_key, baz_key)
	await ds.directory_add(dir_key, bar_key)

	keys = [key async for key in ds.directory_read(dir_key)]
	assert keys == [bar_key, baz_key]

@trio.testing.trio_test
async def test_directory_remove():
	ds = ObjectDirectoryDictDatastore()

	# initialize directory at /foo
	dir_key = Key('/foo')
	await ds.directory(dir_key)

	# adding directory entries
	bar_key = Key('/foo/bar')
	baz_key = Key('/foo/baz')
	await ds.directory_add(dir_key, bar_key)
	await ds.directory_add(dir_key, baz_key)
	keys = [key async for key in ds.directory_read(dir_key)]
	assert keys == [bar_key, baz_key]

	# removing directory entries
	await ds.directory_remove(dir_key, bar_key)
	await ds.directory_remove(dir_key, bar_key, missing_ok=True)
	with pytest.raises(KeyError):
		await ds.directory_remove(dir_key, bar_key, missing_ok=False)
	keys = [key async for key in ds.directory_read(dir_key)]
	assert keys == [baz_key]



#############################
# directory.ObjectDatastore #
#############################

import datastore.adapter.directory

@trio.testing.trio_test
async def test_dir_simple(DatastoreTests):
	s1 = datastore.adapter.directory.ObjectDatastore(ObjectDictDatastore())
	s2 = datastore.adapter.directory.ObjectDatastore(ObjectDictDatastore())
	await DatastoreTests([s1, s2]).subtest_simple()


@pytest.mark.parametrize(*get_all_type_args("tiered"))
@trio.testing.trio_test
async def test_tiered(DatastoreTests, Adapter, DictDatastore, encode_fn):
	s1 = DictDatastore()
	s2 = DictDatastore()
	s3 = DictDatastore()
	ts = Adapter([s1, s2, s3])

	k1 = Key('1')
	k2 = Key('2')
	k3 = Key('3')

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

	await DatastoreTests([ts]).subtest_simple()


@pytest.mark.parametrize(*get_all_type_args("sharded"))
@trio.testing.trio_test
async def test_sharded(DatastoreTests, Adapter, DictDatastore, encode_fn):
	numelems = 100
	
	s1 = DictDatastore()
	s2 = DictDatastore()
	s3 = DictDatastore()
	s4 = DictDatastore()
	s5 = DictDatastore()
	stores = [s1, s2, s3, s4, s5]
	hash = lambda key: int(key.name) * len(stores) // numelems
	sharded = Adapter(stores, sharding_fn=hash)
	sumlens = lambda stores: sum(map(lambda s: len(s), stores))

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

	assert sumlens(stores) == 0
	# test all correct.
	for value in range(0, numelems):
		key = Key(f"/fdasfdfdsafdsafdsa/{value}")
		shard = stores[hash(key) % len(stores)]
		await checkFor(key, value, sharded)
		await shard.put(key, encode_fn(value))
		await checkFor(key, value, sharded, shard)
	assert sumlens(stores) == numelems

	# ensure its in the same spots.
	for i in range(0, numelems):
		key = Key(f"/fdasfdfdsafdsafdsa/{value}")
		shard = stores[hash(key) % len(stores)]
		await checkFor(key, value, sharded, shard)
		await shard.put(key, encode_fn(value))
		await checkFor(key, value, sharded, shard)
	assert sumlens(stores) == numelems

	# ensure its in the same spots.
	for value in range(0, numelems):
		key = Key(f"/fdasfdfdsafdsafdsa/{value}")
		shard = stores[hash(key) % len(stores)]
		await checkFor(key, value, sharded, shard)
		await sharded.put(key, encode_fn(value))
		await checkFor(key, value, sharded, shard)
	assert sumlens(stores) == numelems

	# ensure its in the same spots.
	for value in range(0, numelems):
		key = Key(f"/fdasfdfdsafdsafdsa/{value}")
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
		key = Key(f"/fdasfdfdsafdsafdsa/{value}")
		incorrect_shard = stores[(hash(key) + 1) % len(stores)]
		await checkFor(key, value, sharded)
		await incorrect_shard.put(key, encode_fn(value))
		await checkFor(key, value, sharded, incorrect_shard)
	assert sumlens(stores) == numelems

	# ensure its in the same spots.
	for value in range(0, numelems):
		key = Key(f"/fdasfdfdsafdsafdsa/{value}")
		incorrect_shard = stores[(hash(key) + 1) % len(stores)]
		await checkFor(key, value, sharded, incorrect_shard)
		await incorrect_shard.put(key, encode_fn(value))
		await checkFor(key, value, sharded, incorrect_shard)
	assert sumlens(stores) == numelems

	# this wont do anything
	for value in range(0, numelems):
		key = Key(f"/fdasfdfdsafdsafdsa/{value}")
		incorrect_shard = stores[(hash(key) + 1) % len(stores)]
		await checkFor(key, value, sharded, incorrect_shard)
		with pytest.raises(KeyError):
			await sharded.delete(key)
		await checkFor(key, value, sharded, incorrect_shard)
	assert sumlens(stores) == numelems

	# this will place it correctly.
	for value in range(0, numelems):
		key = Key(f"/fdasfdfdsafdsafdsa/{value}")
		incorrect_shard = stores[(hash(key) + 1) % len(stores)]
		correct_shard = stores[(hash(key)) % len(stores)]
		await checkFor(key, value, sharded, incorrect_shard)
		await sharded.put(key, encode_fn(value))
		await incorrect_shard.delete(key)
		await checkFor(key, value, sharded, correct_shard)
	assert sumlens(stores) == numelems

	# this will place it correctly.
	for value in range(0, numelems):
		key = Key(f"/fdasfdfdsafdsafdsa/{value}")
		correct_shard = stores[(hash(key)) % len(stores)]
		await checkFor(key, value, sharded, correct_shard)
		await sharded.delete(key)
		await checkFor(key, value, sharded)
	assert sumlens(stores) == 0

	await DatastoreTests([sharded]).subtest_simple()
