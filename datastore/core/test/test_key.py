import unittest
import random

import pytest

from datastore.core.key import Key


def random_string():
	string = ''
	length = random.randint(5, 50)
	for i in range(0, length):
		string += chr(random.randint(ord('0'), ord('Z')))
	return string


def random_key():
	return Key('/herp/' + random_string() + '/derp')


class KeyTests(unittest.TestCase):

	def __subtest_basic(self, string):
		fixed_string = Key.remove_duplicate_slashes(string)
		last_namespace = fixed_string.rsplit('/')[-1].split(':')
		k_type = last_namespace[0] if len(last_namespace) > 1 else ''
		name = last_namespace[-1]
		path = fixed_string.rsplit('/', 1)[0] + '/' + k_type
		instance = fixed_string + ':' + 'c'

		assert Key(string)._string == fixed_string
		assert Key(string) == Key(string)
		assert str(Key(string)) == fixed_string
		assert repr(Key(string)) == "Key('%s')" % fixed_string
		assert Key(string).name == name
		assert Key(string).type == k_type
		assert Key(string).instance('c') == Key(instance)
		assert Key(string).path == Key(path)
		assert Key(string) == eval(repr(Key(string)))

		assert Key(string).child('a') > Key(string)
		assert Key(string).child('a') < Key(string).child('b')
		assert Key(string) == Key(string)

		split_string = fixed_string.split('/')
		if len(split_string) > 1:
			assert Key('/'.join(split_string[:-1])) == Key(string).parent
		else:
			with pytest.raises(ValueError):
			    Key(string).parent

		namespace = split_string[-1].split(':')
		if len(namespace) > 1:
			assert namespace[0] == Key(string).type
		else:
			assert '' == Key(string).type

	def test_basic(self):
		self.__subtest_basic('')
		self.__subtest_basic('abcde')
		self.__subtest_basic('disahfidsalfhduisaufidsail')
		self.__subtest_basic('/fdisahfodisa/fdsa/fdsafdsafdsafdsa/fdsafdsa/')
		self.__subtest_basic(u'4215432143214321432143214321')
		self.__subtest_basic('/fdisaha////fdsa////fdsafdsafdsafdsa/fdsafdsa/')
		self.__subtest_basic('abcde:fdsfd')
		self.__subtest_basic('disahfidsalfhduisaufidsail:fdsa')
		self.__subtest_basic('/fdisahfodisa/fdsa/fdsafdsafdsafdsa/fdsafdsa/:')
		self.__subtest_basic(u'4215432143214321432143214321:')
		self.__subtest_basic('/fdisaha////fdsa////fdsafdsafdsafdsa/fdsafdsa/f:fdaf')

	def test_ancestry(self):
		k1 = Key('/A/B/C')
		k2 = Key('/A/B/C/D')

		assert k1._string == '/A/B/C'
		assert k2._string == '/A/B/C/D'
		assert k1.is_ancestor_of(k2)
		assert k2.is_descendant_of(k1)
		assert Key('/A').is_ancestor_of(k2)
		assert Key('/A').is_ancestor_of(k1)
		assert not Key('/A').is_descendant_of(k2)
		assert not Key('/A').is_descendant_of(k1)
		assert k2.is_descendant_of(Key('/A'))
		assert k1.is_descendant_of(Key('/A'))
		assert not k2.is_ancestor_of(Key('/A'))
		assert not k1.is_ancestor_of(Key('/A'))
		assert not k2.is_ancestor_of(k2)
		assert not k1.is_ancestor_of(k1)
		assert k1.child('D') == k2
		assert k1 == k2.parent
		assert k1.path == k2.parent.path

	def test_type(self):
		k1 = Key('/A/B/C:c')
		k2 = Key('/A/B/C:c/D:d')

		with pytest.raises(TypeError):
		    k1.is_ancestor_of(str(k2))
		assert k1.is_ancestor_of(k2)
		assert k2.is_descendant_of(k1)
		assert k1.type == 'C'
		assert k2.type == 'D'
		assert k1.type == k2.parent.type

	def test_hashing(self):
		keys = {}

		for i in range(0, 200):
			key = random_key()
			while key in keys.values():
				key = random_key()

			hstr = str(hash(key))
			assert not (hstr in keys)
			keys[hstr] = key

		for key in keys.values():
			hstr = str(hash(key))
			assert hstr in keys
			assert key == keys[hstr]

#XXX: do we need this?
#	def test_random(self):
#		keys = set()
#		for i in range(0, 1000):
#			rand = random_key()
#			self.assertFalse(rand in keys)
#			keys.add(rand)
#		self.assertEqual(len(keys), 1000)
