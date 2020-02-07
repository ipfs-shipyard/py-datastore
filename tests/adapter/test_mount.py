import contextlib
import typing

import pytest
import trio.testing

import datastore
from tests.adapter.conftest import make_datastore_test_params


@pytest.mark.parametrize(*make_datastore_test_params("mount"))
@trio.testing.trio_test
async def test_mount(DatastoreTests, Adapter, DictDatastore, encode_fn):
	d1 = DictDatastore()
	d2 = DictDatastore()
	
	async with Adapter() as ds:
		ds.mount(datastore.Key("/a"), d1)
		ds.mount(datastore.Key("/a/b/c"), d2)
		
		# Test error when writing to unmounted area
		with pytest.raises(RuntimeError):
			await ds.put(datastore.Key("/data"), encode_fn("value"))
		
		assert not await ds.contains(datastore.Key("/data"))
		assert not await ds.contains(datastore.Key("/a/a"))
		assert not await ds.contains(datastore.Key("/a/b/c/a"))
		with pytest.raises(KeyError):
			await ds.get_all(datastore.Key("/a/a"))
		
		# Write value to key and read through backend
		await ds.put(datastore.Key("/a/a"), encode_fn("value"))
		assert await ds.contains(datastore.Key("/a/a"))
		assert await d1.get_all(datastore.Key("/a")) == encode_fn("value")
		assert not await ds.contains(datastore.Key("/a/b/c/a"))
		
		# Mount backend at different location and read value back
		ds.mount(datastore.Key("/z"), d1)
		assert await ds.get_all(datastore.Key("/z/a")) == encode_fn("value")
		ds.unmount(datastore.Key("/z"))
		
		# Write value to backend key and read through mount ds
		await d2.put(datastore.Key("/b"), encode_fn("value2"))
		assert await (await ds.get(datastore.Key("/a/b/c/b"))).collect() == encode_fn("value2")
