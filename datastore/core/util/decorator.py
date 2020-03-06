import functools
import typing

T = typing.TypeVar("T")


class AwaitableWrapper(
		typing.AsyncContextManager[T],
		typing.Awaitable[typing.AsyncContextManager[T]],
):
	_awaitable: typing.Awaitable[typing.AsyncContextManager[T]]
	_value: typing.AsyncContextManager[T]
	
	def __init__(self, awaitable: typing.Awaitable[typing.AsyncContextManager[T]]):
		self._awaitable = awaitable
	
	def __await__(self) -> typing.Generator[typing.AsyncContextManager[T], typing.Any, typing.Any]:
		return self._awaitable.__await__()
	
	async def __aenter__(self) -> T:
		self._value = await self._awaitable
		return await self._value.__aenter__()
	
	async def __aexit__(self, *args, **kwargs) -> typing.Optional[bool]:
		return await self._value.__aexit__(*args, **kwargs)


def awaitable_to_context_manager(func: typing.Callable[..., typing.Awaitable[T]]) \
    -> typing.Callable[..., AwaitableWrapper[T]]:
	"""Wraps a async function returning an async context manager object in an
	extra async context manager object that lazily awaits the given functon and
	enters the result
	
	This way code such as the following may be simplified by dropping the
	``await`` keyword after the ``async with``::
	
		async with await Datastore.create(...) as ds:
			...
	
	… therefore becomes::
	
		async with Datastore.create(...) as ds:
			...
	"""
	@functools.wraps(func)
	def wrapper(*args, **kwargs):
		return AwaitableWrapper(func(*args, **kwargs))
	
	return wrapper
