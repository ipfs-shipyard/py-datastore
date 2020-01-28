import typing

import pytest

import datastore


DS = typing.TypeVar("DS", datastore.abc.BinaryDatastore, datastore.abc.ObjectDatastore)
Query = typing.Any


@pytest.fixture(name="DatastoreTests")
def return_datastore_tests():
	return DatastoreTests


class DatastoreTests(typing.Generic[DS]):
	pkey: datastore.Key = datastore.Key('/dfadasfdsafdas/')
	stores: typing.List[DS]
	numelems: int
	is_binary: bool
	
	#FIXME: For some reason `numelems` increases test runtime with at least n²
	def __init__(self, stores: typing.List[DS], numelems: int = 10):  # 1000):
		self.stores = stores
		self.numelems = numelems
		
		self.is_binary = isinstance(stores[0], datastore.abc.BinaryDatastore)
	
	
	def encode(self, value):
		if self.is_binary:
			return str(value).encode()
		else:
			return [value]
	
	
	def check_length(self, length: int) -> None:
		for sn in self.stores:
			try:
				assert len(sn) == length  # type: ignore
			except TypeError:
				pass
	
	
	async def subtest_remove_nonexistent(self) -> None:
		assert len(self.stores) > 0
		self.check_length(0)

		# ensure removing non-existent keys is ok.
		for value in range(0, self.numelems):
			key = self.pkey.child(value)
			for sn in self.stores:
				assert not await sn.contains(key)
				with pytest.raises(KeyError):
					await sn.delete(key)
				assert not await sn.contains(key)

		self.check_length(0)
	
	
	async def subtest_insert_elems(self) -> None:
		sn: DS
		key: datastore.Key
		value: int
		
		# insert numelems elems
		for value in range(0, self.numelems):
			key = self.pkey.child(value)
			for sn in self.stores:
				assert not await sn.contains(key)
				await sn.put(key, self.encode(value))
				assert await sn.contains(key)
				assert await sn.get_all(key) == self.encode(value)

		# reassure they're all there.
		self.check_length(self.numelems)

		for value in range(0, self.numelems):
			key = self.pkey.child(value)
			for sn in self.stores:
				assert await sn.contains(key)
				assert await sn.get_all(key) == self.encode(value)

		self.check_length(self.numelems)
	
	
	@typing.no_type_check  #FIXME: This method is broken
	async def check_query(self, query, total, slice) -> datastore.Cursor:
		assert not self.is_binary  # Queries are only supported for object stores
		
		allitems: typing.List[int] = list(range(0, total))
		sn: datastore.abc.ObjectDatastore
		resultset: datastore.Cursor

		for sn in self.stores:
			try:
				contents = list(await sn.query(Query(self.pkey)))
				expected = contents[slice]
				resultset = await sn.query(query)
				result = list(resultset)

				# make sure everything is there.
				assert sorted(contents) == sorted(allitems)
				assert sorted(result) == sorted(expected)

				# TODO: should order be preserved?
				#assert result == expected

			except NotImplementedError:
				print('WARNING: %s does not implement query.' % sn)

		return resultset
	
	
	@typing.no_type_check  #FIXME: This method is broken
	async def subtest_queries(self) -> None:
		if self.is_binary:
			return  # Not supported on binary datastores
		
		sn: datastore.abc.ObjectDatastore
		value: int
		
		for value in range(0, self.numelems):
			key: datastore.Key = self.pkey.child(value)
			for sn in self.stores:
				await sn.put(key, value)

		k: datastore.Key = self.pkey
		n: int = int(self.numelems)

		await self.check_query(Query(k), n, slice(0, n))
		await self.check_query(Query(k, limit=n), n, slice(0, n))
		await self.check_query(Query(k, limit=n // 2), n, slice(0, n // 2))
		await self.check_query(Query(k, offset=n // 2), n, slice(n // 2, n))
		await self.check_query(Query(k, offset=n // 3, limit=n // 3), n, slice(n // 3, 2 * (n // 3)))
		del k
		del n
	
	
	async def subtest_update(self) -> None:
		sn: DS
		value: int
		
		# change numelems elems
		for value in range(0, self.numelems):
			key: datastore.Key = self.pkey.child(value)
			for sn in self.stores:
				assert await sn.contains(key)
				await sn.put(key, self.encode(value + 1))
				assert await sn.contains(key)
				assert self.encode(value) != await sn.get_all(key)
				assert self.encode(value + 1) == await sn.get_all(key)

		self.check_length(self.numelems)
	
	
	async def subtest_remove(self) -> None:
		sn: DS
		value: int
		
		# remove numelems elems
		for value in range(0, self.numelems):
			key: datastore.Key = self.pkey.child(value)
			for sn in self.stores:
				assert await sn.contains(key)
				await sn.delete(key)
				assert not await sn.contains(key)

		self.check_length(0)
	
	
	async def subtest_simple(self) -> None:
		await self.subtest_remove_nonexistent()
		await self.subtest_insert_elems()
		#await self.subtest_queries()  #FIXME: Query is broken
		await self.subtest_update()
		await self.subtest_remove()
