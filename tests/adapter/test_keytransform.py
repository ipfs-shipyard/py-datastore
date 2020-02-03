import logging
import typing

import pytest
import trio.testing

import datastore
from tests.adapter.conftest import make_datastore_test_params


@pytest.mark.parametrize(*make_datastore_test_params("keytransform"))
@trio.testing.trio_test
async def test_keytransform_simple(DatastoreTests, Adapter, DictDatastore, encode_fn):
	s1 = Adapter(DictDatastore())
	s2 = Adapter(DictDatastore())
	s3 = Adapter(DictDatastore())
	stores = [s1, s2, s3]

	await DatastoreTests(stores).subtest_simple()


@pytest.mark.parametrize(*make_datastore_test_params("keytransform"))
@trio.testing.trio_test
async def test_keytransform_reverse_transform(Adapter, DictDatastore, encode_fn):
	def transform(key):
		return key.reverse

	ds = DictDatastore()
	kt = Adapter(ds, key_transform=transform)

	k1 = datastore.Key('/a/b/c')
	k2 = datastore.Key('/c/b/a')
	assert not await ds.contains(k1)
	assert not await ds.contains(k2)
	assert not await kt.contains(k1)
	assert not await kt.contains(k2)

	await ds.put(k1, encode_fn('abc'))
	assert await ds.get_all(k1) == encode_fn('abc')
	assert not await ds.contains(k2)
	assert not await kt.contains(k1)
	with pytest.raises(KeyError):
		await (await kt.get(k1)).aclose()
	assert await (await kt.get(k2)).collect() == encode_fn('abc')
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


@pytest.mark.parametrize(*make_datastore_test_params("keytransform"))
@trio.testing.trio_test
async def test_keytransform_lowercase_transform(Adapter, DictDatastore, encode_fn):
	def transform(key):
		return datastore.Key(str(key).lower())
	
	ds = DictDatastore()
	lds = Adapter(ds, key_transform=transform)
	
	k1 = datastore.Key('hello')
	k2 = datastore.Key('HELLO')
	k3 = datastore.Key('HeLlo')
	
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


#######################
# LowercaseKeyAdapter #
#######################


@pytest.mark.parametrize(*make_datastore_test_params("keytransform", "LowercaseKey"))
@trio.testing.trio_test
async def test_lowercase_key_simple(DatastoreTests, Adapter, DictDatastore, encode_fn):
	s1 = Adapter(DictDatastore())
	s2 = Adapter(DictDatastore())
	s3 = Adapter(Adapter(DictDatastore()))
	stores = [s1, s2, s3]
	
	await DatastoreTests(stores).subtest_simple()


@pytest.mark.parametrize(*make_datastore_test_params("keytransform", "LowercaseKey"))
@trio.testing.trio_test
async def test_lowercase_key(Adapter, DictDatastore, encode_fn):
	ds = DictDatastore()
	lds = Adapter(ds)
	
	k1 = datastore.Key('hello')
	k2 = datastore.Key('HELLO')
	k3 = datastore.Key('HeLlo')
	
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


####################
# NamespaceAdapter #
####################


@pytest.mark.parametrize(*make_datastore_test_params("keytransform", "Namespace"))
@trio.testing.trio_test
async def test_namespace_simple(DatastoreTests, Adapter, DictDatastore, encode_fn):
	s1 = Adapter(datastore.Key('a'), DictDatastore())
	s2 = Adapter(datastore.Key('b'), DictDatastore())
	s3 = Adapter(datastore.Key('c'), DictDatastore())
	stores = [s1, s2, s3]

	await DatastoreTests(stores).subtest_simple()


@pytest.mark.parametrize(*make_datastore_test_params("keytransform", "Namespace"))
@trio.testing.trio_test
async def test_namespace(Adapter, DictDatastore, encode_fn):
	k1 = datastore.Key('/c/d')
	k2 = datastore.Key('/a/b')
	k3 = datastore.Key('/a/b/c/d')

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
		await test(datastore.Key(str(i)), encode_fn(f"val{i}"))


#####################
# NestedPathAdapter #
#####################


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


@pytest.mark.parametrize(*make_datastore_test_params("keytransform", "NestedPath"))
@trio.testing.trio_test
async def test_nested_path_simple(DatastoreTests, Adapter, DictDatastore, encode_fn):
	s1 = Adapter(DictDatastore())
	s2 = Adapter(DictDatastore(), depth=2)
	s3 = Adapter(DictDatastore(), length=2)
	s4 = Adapter(DictDatastore(), length=1, depth=2)
	stores = [s1, s2, s3, s4]

	await DatastoreTests(stores).subtest_simple()


@pytest.mark.parametrize(*make_datastore_test_params("keytransform", "NestedPath"))
def test_nested_path_gen(Adapter, DictDatastore, encode_fn):
	def test(depth, length, expected):
		nested = Adapter.nested_path('abcdefghijk', depth, length)
		assert nested == expected

	test(3, 2, 'ab/cd/ef')
	test(4, 2, 'ab/cd/ef/gh')
	test(3, 4, 'abcd/efgh/ijk')
	test(1, 4, 'abcd')
	test(3, 10, 'abcdefghij/k')


@pytest.mark.parametrize(*make_datastore_test_params("keytransform", "NestedPath"))
@trio.testing.trio_test
async def test_nested_path_3_2(Adapter, DictDatastore, encode_fn):
	opts = {}
	opts['k1'] = datastore.Key('/abcdefghijk')
	opts['k2'] = datastore.Key('/abcdefghijki')
	opts['k3'] = datastore.Key('/ab/cd/ef/abcdefghijk')
	opts['k4'] = datastore.Key('/ab/cd/ef/abcdefghijki')
	opts['depth'] = 3
	opts['length'] = 2

	await subtest_nested_path_ds(Adapter, DictDatastore, encode_fn, **opts)


@pytest.mark.parametrize(*make_datastore_test_params("keytransform", "NestedPath"))
@trio.testing.trio_test
async def test_nested_path_5_3(Adapter, DictDatastore, encode_fn):
	opts = {}
	opts['k1'] = datastore.Key('/abcdefghijk')
	opts['k2'] = datastore.Key('/abcdefghijki')
	opts['k3'] = datastore.Key('/abc/def/ghi/jka/bcd/abcdefghijk')
	opts['k4'] = datastore.Key('/abc/def/ghi/jki/abc/abcdefghijki')
	opts['depth'] = 5
	opts['length'] = 3

	await subtest_nested_path_ds(Adapter, DictDatastore, encode_fn, **opts)


@pytest.mark.parametrize(*make_datastore_test_params("keytransform", "NestedPath"))
@trio.testing.trio_test
async def test_nested_path_keyfn(Adapter, DictDatastore, encode_fn):
	opts = {}
	opts['k1'] = datastore.Key('/abcdefghijk')
	opts['k2'] = datastore.Key('/abcdefghijki')
	opts['k3'] = datastore.Key('/kj/ih/gf/abcdefghijk')
	opts['k4'] = datastore.Key('/ik/ji/hg/abcdefghijki')
	opts['depth'] = 3
	opts['length'] = 2
	opts['key_fn'] = lambda key: key.name[::-1]

	await subtest_nested_path_ds(Adapter, DictDatastore, encode_fn, **opts)