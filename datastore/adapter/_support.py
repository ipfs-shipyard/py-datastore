import typing

import datastore


#XXX: Maybe retry describing this cooperative multiple inheritance scheme if these are ever added:
#
#  * https://github.com/python/mypy/issues/7191 (Mixin classes in general)
#  * https://github.com/python/mypy/issues/7790 (Associated types)
#  * https://github.com/python/mypy/issues/7791 (Types of generic classes)
DS = typing.TypeVar("DS", datastore.abc.BinaryDatastore, datastore.abc.ObjectDatastore)
DA = typing.TypeVar("DA", datastore.abc.BinaryAdapter,   datastore.abc.ObjectAdapter)
RT = typing.TypeVar("RT", datastore.abc.ReceiveStream,   datastore.abc.ReceiveChannel)


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
