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
	"""Wraps a datastore with a logging shim"""
	__slots__ = ()
	
	FORWARD_CONTAINS = True
	FORWARD_GET_ALL  = True
	FORWARD_PUT_NEW  = True
	FORWARD_STAT     = True
	
	logger: logging.Logger
	
	
	def __init__(self, *args: typing.Any, logger: logging.Logger = ROOT_LOGGER,
	             **kwargs: typing.Any):
		self.logger = logger
		super().__init__(*args, **kwargs)  # type: ignore[call-arg]
	
	
	async def get(self, key: datastore.Key) -> RT:
		"""Returns an iterable of all data named by *key* or raises if none exists
		
		LoggingDatastore logs the access.
		"""
		self.logger.info('%s: get %s', self, key)
		value: RT = await super().get(key)  # type: ignore[misc]
		self.logger.debug('%s: %s', self, value)
		return value
	
	
	async def get_all(self, key: datastore.Key) -> RV:
		"""Returns all data named by *key* or raises if none exists
		
		LoggingDatastore logs the access.
		"""
		self.logger.info('%s: get %s', self, key)
		value: RV = await super().get_all(key)  # type: ignore[misc]
		self.logger.debug('%s: %s', self, value)
		return value
	
	
	async def _put(self, key: datastore.Key, value: RT, *,
	               create: bool, replace: bool, **kwargs: typing.Any) -> None:
		"""Stores *value* at name *key*
		
		LoggingDatastore logs the access.
		"""
		self.logger.info('%s: put %s (create=%s, replace=%s)', self, key, create, replace)
		self.logger.debug('%s: %s', self, value)
		await super()._put(key, value, create=create, replace=replace, **kwargs)  # type: ignore[misc]
	
	
	async def _put_new(self, prefix: datastore.Key, value: RT, **kwargs: typing.Any) -> datastore.Key:
		"""Stores *value* below name *prefix*
		
		LoggingDatastore logs the access.
		"""
		self.logger.info('%s: put_new[D] %s', self, prefix)
		self.logger.debug('%s: %s', self, value)
		key: datastore.Key = await super()._put_new(prefix, value, **kwargs)  # type: ignore[misc]
		return key
	
	
	async def _put_new_indirect(self, prefix: datastore.Key, **kwargs: typing.Any) \
	      -> typing.Tuple[datastore.Key, typing.Callable[[RT], typing.Awaitable[None]]]:
		"""Stores the value passed to the returned callback below name *prefix*
		
		LoggingDatastore logs the access.
		"""
		self.logger.info('%s: put_new[I] %s', self, prefix)
		return await super()._put_new_indirect(prefix, **kwargs)  # type: ignore[misc, no-any-return]
	
	
	async def delete(self, key: datastore.Key) -> None:
		"""Removes any data at name *key*
		
		LoggingDatastore logs the access.
		"""
		self.logger.info('%s: delete %s', self, key)
		await super().delete(key)  # type: ignore[misc]
	
	
	async def contains(self, key: datastore.Key) -> bool:
		"""Returns whether any data at name *key* exists
		
		LoggingDatastore logs the access.
		"""
		self.logger.info('%s: contains %s', self, key)
		return await super().contains(key)  # type: ignore[misc, no-any-return]
	
	
	async def stat(self, key: datastore.Key) -> MD:
		"""Returns metadata about things stored at name *key*
		
		LoggingDatastore logs the access.
		"""
		self.logger.info('%s: stat %s', self, key)
		metadata: MD = await super().stat(key)  # type: ignore[misc]
		self.logger.debug('%s: %s', self, metadata)
		return metadata
	
	def datastore_stats(self, selector: datastore.Key = None, *, _seen: typing.Set[int] = None) \
	    -> datastore.util.DatastoreMetadata:
		"""Returns metadata of the child datastore
		
		LoggingDatastore logs the access.
		"""
		self.logger.info('%s: datastore_stats %s', self, selector)
		return super().datastore_stats(selector, _seen=_seen)  # type: ignore[misc, no-any-return]
	
	
	async def query(self, query: datastore.Query) -> datastore.Cursor:
		"""Returns an iterable of objects matching criteria expressed in *query*
		
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
