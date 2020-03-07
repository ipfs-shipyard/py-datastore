import typing
import uuid
from functools import total_ordering

from datastore.core.util.fasthash import fast_hash


class Namespace(str):
	"""A Key Namespace is a string identifier.
	
	A namespace can optionally include a type (delimited by ':')
	
	Example namespaces::
	
		Namespace('Bruces')
		Namespace('Song:PhilosopherSong')
	"""
	namespace_delimiter = ':'

	def __repr__(self) -> str:
		return "Namespace('%s')" % self

	@property
	def type(self) -> str:
		"""returns the `type` part of this namespace, if any."""
		if Namespace.namespace_delimiter in self:
			return self.split(Namespace.namespace_delimiter)[0]
		return ''

	@property
	def name(self) -> str:
		"""returns the `name` part of this namespace."""
		return self.split(Namespace.namespace_delimiter)[-1]


@total_ordering
class Key:
	"""A Key represents the unique identifier of an datastore object
	
	Our Key scheme is inspired by file systems and the Google App Engine key
	model.
	
	Keys are meant to be unique across a system. Keys are hierarchical,
	incorporating more and more specific namespaces. Thus keys can be deemed
	'children' or 'ancestors' of other keys::
	
		Key('/Comedy')
		Key('/Comedy/MontyPython')
	
	Also, every namespace can be parametrized to embed relevant object
	information. For example, the Key `name` (most specific namespace) could
	include the object type::
	
		Key('/Comedy/MontyPython/Actor:JohnCleese')
		Key('/Comedy/MontyPython/Sketch:CheeseShop')
		Key('/Comedy/MontyPython/Sketch:CheeseShop/Character:Mousebender')
	"""

	__slots__ = ('_string', '_list')
	_string: str
	_list: typing.Optional[typing.List[Namespace]]

	def __init__(self, key: typing.Union[typing.Sequence[Namespace], str, 'Key']):
		if not isinstance(key, (str, Key)):
			key = '/'.join(key)

		self._string = self.remove_duplicate_slashes(str(key))
		self._list = None

	def __str__(self) -> str:
		"""Returns the string representation of this Key."""
		return self._string

	def __repr__(self) -> str:
		"""Returns the repr of this Key."""
		return "Key('%s')" % self._string

	@property
	def list(self) -> typing.List[Namespace]:
		"""Returns the `list` representation of this Key.
		
		Note that this method assumes the key is immutable.
		"""
		if not self._list:
			self._list = list(map(Namespace, self._string.split('/')))
		return self._list

	@property
	def reverse(self) -> 'Key':
		"""Returns the reverse of this Key.
		
			>>> Key('/Comedy/MontyPython/Actor:JohnCleese').reverse
			Key('/Actor:JohnCleese/MontyPython/Comedy')
		"""
		return Key(self.list[::-1])

	@property
	def namespaces(self) -> typing.List[Namespace]:
		"""Returns the list of namespaces of this Key."""
		return self.list

	@property
	def name(self) -> str:
		"""Returns the name of this Key, the name of the last namespace."""
		return Namespace(self.list[-1]).name

	@property
	def type(self) -> str:
		"""Returns the type of this Key, the type of the last namespace."""
		return Namespace(self.list[-1]).type

	def instance(self, other: str) -> 'Key':
		"""Returns an instance Key, by appending a name to the namespace."""
		assert '/' not in other
		return Key(str(self) + ':' + str(other))

	@property
	def path(self) -> 'Key':
		"""Returns the path of this Key, the parent and the type."""
		return Key(str(self.parent) + '/' + self.type)

	@property
	def parent(self) -> 'Key':
		"""Returns the parent Key (all namespaces except the last).
		
			>>> Key('/Comedy/MontyPython/Actor:JohnCleese').parent
			Key('/Comedy/MontyPython')
		"""
		if '/' in self._string:
			return Key(self.list[:-1])
		raise ValueError(f"{repr(self)} is a root key (it has no parent)")

	def child(self, other: typing.Union[str, 'Key']) -> 'Key':
		"""Returns the child Key by appending namespace `other`.
		
			>>> Key('/Comedy/MontyPython').child('Actor:JohnCleese')
			Key('/Comedy/MontyPython/Actor:JohnCleese')
		"""
		return Key('%s/%s' % (self._string, str(other)))
	
	def __div__(self, other: typing.Union[str, 'Key']) -> 'Key':
		return self.child(other)
	
	def is_ancestor_of(self, other: 'Key') -> bool:
		"""Returns whether this Key is an ancestor of `other`.
		
			>>> john = Key('/Comedy/MontyPython/Actor:JohnCleese')
			>>> Key('/Comedy').is_ancestor_of(john)
			True
		"""
		return other._string.startswith(self._string + '/')

	def is_descendant_of(self, other: 'Key') -> bool:
		"""Returns whether this Key is a descendant of `other`.
		
			>>> Key('/Comedy/MontyPython').is_descendant_of(Key('/Comedy'))
			True
		"""
		return other.is_ancestor_of(self)

	def is_top_level(self) -> bool:
		"""Returns whether this Key is top-level (one namespace)."""
		return len(self.list) == 1

	def __hash__(self) -> int:
		"""Returns the hash of this Key.
		
		Note that for the purposes of this Key (that is, to use it and its hash
		values as unique identifiers across systems and platforms), the hash(.)
		builtin is not adequate (as it is not guaranteed to return the same hash
		value for two different interpreter runs, let alone different machines).
		
		For our purposes, then, we are using a perhaps more expensive hash function
		that guarantees equal hash values given the same input.
		"""
		return fast_hash(self)

	def __iter__(self) -> typing.Iterable[Namespace]:
		return iter(self.list)

	def __len__(self) -> int:
		return len(self.list)

	def __lt__(self, other: object) -> bool:
		if isinstance(other, Key):
			return self.list < other.list
		return NotImplemented

	def __eq__(self, other: object) -> bool:
		if isinstance(other, Key):
			return self._string == other._string
		return NotImplemented

	@classmethod
	def random_key(cls) -> 'Key':
		"""Returns a random Key"""
		return Key(uuid.uuid4().hex)

	@classmethod
	def remove_duplicate_slashes(cls, path: str) -> str:
		"""Returns the path string `path` without duplicate slashes."""
		return '/' + '/'.join(filter(lambda p: p != '', path.split('/')))
