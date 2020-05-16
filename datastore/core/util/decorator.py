import functools
import typing

T_co = typing.TypeVar("T_co", covariant=True)


class AwaitableWrapper(
		typing.AsyncContextManager[T_co],
		typing.Awaitable[T_co],
):
	_awaitable: typing.Awaitable[typing.AsyncContextManager[T_co]]
	_value: typing.AsyncContextManager[T_co]
	
	def __init__(self, awaitable: typing.Awaitable[typing.AsyncContextManager[T_co]]):
		self._awaitable = awaitable
	
	def __await__(self) -> typing.Generator[typing.Any, typing.Any, T_co]:
		return typing.cast(
			typing.Generator[typing.Any, typing.Any, T_co],
			self._awaitable.__await__()
		)
	
	async def __aenter__(self) -> T_co:
		self._value = await self._awaitable
		return await self._value.__aenter__()
	
	async def __aexit__(self, *args: typing.Any, **kwargs: typing.Any) -> typing.Optional[bool]:
		return await self._value.__aexit__(*args, **kwargs)


def awaitable_to_context_manager(
		func: typing.Callable[..., typing.Awaitable[typing.AsyncContextManager[T_co]]]
) -> typing.Callable[..., AwaitableWrapper[T_co]]:
	"""Wraps a async function returning an async context manager object in an
	extra async context manager object that lazily awaits the given functon and
	enters the result
	
	This way code such as the following may be simplified by dropping the extra
	``await`` keyword after the ``async with``::
	
		async with await Datastore.create(...) as ds:
			...
	
	… therefore becomes::
	
		async with Datastore.create(...) as ds:
			...
	"""
	@functools.wraps(func)
	def wrapper(*args: typing.Any, **kwargs: typing.Any) -> AwaitableWrapper[T_co]:
		return AwaitableWrapper(func(*args, **kwargs))
	
	return wrapper
