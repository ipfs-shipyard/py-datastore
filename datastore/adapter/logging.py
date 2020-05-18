import logging
import typing

import datastore

from ._support import DS, MD, RT, RV, T_co

__all__ = [
	"BinaryAdapter",
	"ObjectAdapter"
]



ROOT_LOGGER: logging.Logger = logging.getLogger()



class _Adapter(typing.Generic[DS, MD, RT, RV]):
	"""Wraps a datastore with a logging shim."""
	__slots__ = ()
	
	logger: logging.Logger
	
	
	def __init__(self, *args: typing.Any, logger: logging.Logger = ROOT_LOGGER,
	             **kwargs: typing.Any):
		self.logger = logger
		super().__init__(*args, **kwargs)  # type: ignore[call-arg]
	
	
	async def get(self, key: datastore.Key) -> RT:
		"""Return the object named by key or None if it does not exist.
		   LoggingDatastore logs the access.
		"""
		self.logger.info('%s: get %s', self, key)
		value: RT = await super().get(key)  # type: ignore[misc]
		self.logger.debug('%s: %s', self, value)
		return value
	
	
	async def get_all(self, key: datastore.Key) -> RV:
		"""Return the object named by key or None if it does not exist.
		   LoggingDatastore logs the access.
		"""
		self.logger.info('%s: get %s', self, key)
		value: RV = await super().get_all(key)  # type: ignore[misc]
		self.logger.debug('%s: %s', self, value)
		return value
	
	
	async def _put(self, key: datastore.Key, value: RT) -> None:
		"""Stores the object `value` named by `key`self.
		   LoggingDatastore logs the access.
		"""
		self.logger.info('%s: put %s', self, key)
		self.logger.debug('%s: %s', self, value)
		await super()._put(key, value)  # type: ignore[misc]
	
	
	async def delete(self, key: datastore.Key) -> None:
		"""Removes the object named by `key`.
		   LoggingDatastore logs the access.
		"""
		self.logger.info('%s: delete %s', self, key)
		await super().delete(key)  # type: ignore[misc]
	
	
	async def contains(self, key: datastore.Key) -> bool:
		"""Returns whether the object named by `key` exists.
		   LoggingDatastore logs the access.
		"""
		self.logger.info('%s: contains %s', self, key)
		return await super().contains(key)  # type: ignore[misc, no-any-return]
	
	
	async def stat(self, key: datastore.Key) -> MD:
		"""Returns the metadata of the object named by `key`.
		   LoggingDatastore logs the access.
		"""
		self.logger.info('%s: stat %s', self, key)
		metadata: MD = await super().stat(key)  # type: ignore[misc]
		self.logger.debug('%s: %s', self, metadata)
		return metadata
	
	
	async def query(self, query: datastore.Query) -> datastore.Cursor:
		"""Returns an iterable of objects matching criteria expressed in `query`.
		   LoggingDatastore logs the access.
		"""
		self.logger.info('%s: query %s', self, query)
		return await super().query(query)  # type: ignore[misc, no-any-return]


class BinaryAdapter(
		_Adapter[
			datastore.abc.BinaryDatastore,
			datastore.util.StreamMetadata,
			datastore.abc.ReceiveStream,
			bytes
		],
		datastore.abc.BinaryAdapter
):
	__slots__ = ("logger",)


class ObjectAdapter(
		typing.Generic[T_co],
		_Adapter[
			datastore.abc.ObjectDatastore[T_co],
			datastore.util.ChannelMetadata,
			datastore.abc.ReceiveChannel[T_co],
			typing.List[T_co]
		],
		datastore.abc.ObjectAdapter[T_co, T_co]
):
	__slots__ = ("logger",)
