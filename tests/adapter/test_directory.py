import logging
import typing

import pytest
import trio.testing

import datastore
import datastore.adapter.directory


@trio.testing.trio_test
async def test_dir_simple(DatastoreTests):
	s1 = datastore.adapter.directory.ObjectDatastore(datastore.ObjectDictDatastore())
	s2 = datastore.adapter.directory.ObjectDatastore(datastore.ObjectDictDatastore())
	await DatastoreTests([s1, s2]).subtest_simple()


##########################
# ObjectDirectorySupport #
##########################


class ObjectDirectoryDictDatastore(
		datastore.adapter.directory.ObjectDirectorySupport,
		datastore.ObjectDictDatastore
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
	dir_key = datastore.Key('/foo')
	await ds.directory(dir_key)
	assert await ds.get_all(dir_key) == []

	# can add to dir
	bar_key = datastore.Key('/foo/bar')
	await ds.directory_add(dir_key, bar_key)
	assert await ds.get_all(dir_key) == [str(bar_key)]

	# re-init does not wipe out directory at /foo
	dir_key = datastore.Key('/foo')
	with pytest.raises(KeyError):
		await ds.directory(dir_key, exist_ok=False)
	await ds.directory(dir_key, exist_ok=True)
	assert await ds.get_all(dir_key) == [str(bar_key)]


@trio.testing.trio_test
async def test_directory_basic():
	ds = ObjectDirectoryDictDatastore()

	# initialize directory at /foo
	dir_key = datastore.Key('/foo')
	await ds.directory(dir_key)

	# adding directory entries
	bar_key = datastore.Key('/foo/bar')
	baz_key = datastore.Key('/foo/baz')
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
	dir_key = datastore.Key('/foo')
	await ds.directory(dir_key)

	# adding directory entries
	bar_key = datastore.Key('/foo/bar')
	baz_key = datastore.Key('/foo/baz')
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
	dir_key = datastore.Key('/foo')
	await ds.directory(dir_key)

	# adding directory entries
	bar_key = datastore.Key('/foo/bar')
	baz_key = datastore.Key('/foo/baz')
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
