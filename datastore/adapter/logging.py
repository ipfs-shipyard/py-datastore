import logging
import typing

import datastore

from ._support import DS, RT

__all__ = [
	"BinaryAdapter",
	"ObjectAdapter"
]



ROOT_LOGGER: logging.Logger = logging.getLogger()



class _Adapter(typing.Generic[DS]):
	"""Wraps a datastore with a logging shim."""
	
	logger: logging.Logger
	
	
	@typing.no_type_check
	def __init__(self, *args, logger: logging.Logger = ROOT_LOGGER, **kwargs):
		self.logger = logger
		super().__init__(*args, **kwargs)
	
	
	@typing.no_type_check
	async def get(self, key: datastore.Key) -> RT:
		"""Return the object named by key or None if it does not exist.
		   LoggingDatastore logs the access.
		"""
		self.logger.info('%s: get %s' % (self, key))
		value = await super().get(key)
		self.logger.debug('%s: %s' % (self, value))
		return value
	
	
	@typing.no_type_check
	async def _put(self, key: datastore.Key, value: RT) -> None:
		"""Stores the object `value` named by `key`self.
		   LoggingDatastore logs the access.
		"""
		self.logger.info('%s: put %s' % (self, key))
		self.logger.debug('%s: %s' % (self, value))
		await super()._put(key, value)
	
	
	@typing.no_type_check
	async def delete(self, key: datastore.Key) -> None:
		"""Removes the object named by `key`.
		   LoggingDatastore logs the access.
		"""
		self.logger.info('%s: delete %s' % (self, key))
		await super().delete(key)
	
	
	@typing.no_type_check
	async def contains(self, key: datastore.Key) -> bool:
		"""Returns whether the object named by `key` exists.
		   LoggingDatastore logs the access.
		"""
		self.logger.info('%s: contains %s' % (self, key))
		return await super().contains(key)
	
	
	@typing.no_type_check
	async def query(self, query: datastore.Query) -> datastore.Cursor:
		"""Returns an iterable of objects matching criteria expressed in `query`.
		   LoggingDatastore logs the access.
		"""
		self.logger.info('%s: query %s' % (self, query))
		return await super().query(query)


class BinaryAdapter(_Adapter[datastore.abc.BinaryDatastore], datastore.abc.BinaryAdapter):
	...


class ObjectAdapter(_Adapter[datastore.abc.ObjectDatastore], datastore.abc.ObjectAdapter):
	...
