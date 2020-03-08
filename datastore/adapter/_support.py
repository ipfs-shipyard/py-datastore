import typing

import trio

import datastore

T_co = typing.TypeVar("T_co", covariant=True)

#XXX: Maybe retry describing this cooperative multiple inheritance scheme if these are ever added:
#
#  * https://github.com/python/mypy/issues/7790 (Associated types)
#  * https://github.com/python/mypy/issues/7791 (Types of generic classes)
DS = typing.TypeVar("DS", datastore.abc.BinaryDatastore,
                    datastore.abc.ObjectDatastore[T_co])  # type: ignore[valid-type]
DA = typing.TypeVar("DA", datastore.abc.BinaryAdapter,
                    datastore.abc.ObjectAdapter[T_co, T_co])  # type: ignore[valid-type]
MD = typing.TypeVar("MD", datastore.util.StreamMetadata,
                    datastore.util.ChannelMetadata)
RT = typing.TypeVar("RT", datastore.abc.ReceiveStream,
                    datastore.abc.ReceiveChannel[T_co])  # type: ignore[valid-type]
RV = typing.TypeVar("RV", bytes, typing.List[T_co])  # type: ignore[valid-type]


# Workaround for https://github.com/python/mypy/issues/708
#
# Source: https://github.com/python/mypy/issues/708#issuecomment-405812141
T = typing.TypeVar("T")


class FunctionProperty(typing.Generic[T]):
    def __get__(self, oself: typing.Any, owner: typing.Any) -> T:
        ...

    def __set__(self, oself: typing.Any, value: T) -> None:
        ...


class DatastoreCollectionMixin(typing.Generic[DS]):
	"""Represents a collection of datastores."""
	__slots__ = ()
	
	_stores: typing.List[DS]
	
	def __init__(self, stores: typing.Collection[DS] = []):
		"""Initialize the datastore with any provided datastores."""
		if not isinstance(stores, list):
			stores = list(stores)
		self._stores = stores

	def get_datastore_at(self, index: int) -> DS:
		"""Returns the datastore at `index`."""
		return self._stores[index]

	def append_datastore(self, store: DS) -> None:
		"""Appends datastore `store` to this collection."""
		self._stores.append(store)

	def remove_datastore(self, store: DS) -> None:
		"""Removes datastore `store` from this collection."""
		self._stores.remove(store)

	def insert_datastore(self, index: int, store: DS) -> None:
		"""Inserts datastore `store` into this collection at `index`."""
		self._stores.insert(index, store)
	
	def datastore_stats(self, selector: datastore.Key = None, *, _seen: typing.Set[int] = None) \
	    -> datastore.util.DatastoreMetadata:
		"""Returns the metadata sum of all children
		
		Arguments
		---------
		selector
			Passed down to queried child datastores but otherwise ignored, as this
			datastore always queries all children
		
		Raises
		------
		RuntimeError
			An internal error occurred in one of the child datastores
		"""
		_seen = _seen if _seen is not None else set()
		
		metadata = datastore.util.DatastoreMetadata.IGNORE
		for store in self._stores:
			if id(store) in _seen:
				continue
			
			_seen.add(id(store))
			metadata += store.datastore_stats(selector, _seen=_seen)
		return metadata
	
	
	async def _stores_cleanup(self) -> None:
		"""Closes and removes all added datastores"""
		errors: typing.List[Exception] = []
		
		while len(self._stores):
			store = self._stores.pop()
			
			try:
				await store.aclose()
			except trio.Cancelled:
				pass  # We check for cancellation later on
			except Exception as error:
				errors.append(error)
		
		# Ensure error propagation
		if errors:
			raise trio.MultiError(errors)
		
		# Ensure cancellation is propagated
		await trio.sleep(0)
