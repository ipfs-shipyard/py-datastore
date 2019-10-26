import unittest
import hashlib
import time

import pytest

from datastore.core.key import Key
from datastore.core.query import Filter, Order, Query, Cursor


def version_objects():
	sr1 = {}
	sr1['key'] = '/ABCD'
	sr1['hash'] = hashlib.sha1('herp').hexdigest()
	sr1['parent'] = '0000000000000000000000000000000000000000'
	sr1['created'] = time.time_ns()
	sr1['committed'] = time.time_ns()
	sr1['attributes'] = {'str': {'value': 'herp'}}
	sr1['type'] = 'Hurr'

	sr2 = {}
	sr2['key'] = '/ABCD'
	sr2['hash'] = hashlib.sha1('derp').hexdigest()
	sr2['parent'] = hashlib.sha1('herp').hexdigest()
	sr2['created'] = time.time_ns()
	sr2['committed'] = time.time_ns()
	sr2['attributes'] = {'str': {'value': 'derp'}}
	sr2['type'] = 'Hurr'

	sr3 = {}
	sr3['key'] = '/ABCD'
	sr3['hash'] = hashlib.sha1('lerp').hexdigest()
	sr3['parent'] = hashlib.sha1('derp').hexdigest()
	sr3['created'] = time.time_ns()
	sr3['committed'] = time.time_ns()
	sr3['attributes'] = {'str': {'value': 'lerp'}}
	sr3['type'] = 'Hurr'

	return sr1, sr2, sr3


class TestFilter(unittest.TestCase):

	def assertFilter(self, filter, objects, match):
		result = [o for o in Filter.filter(filter, objects)]
		assert result == match

	def test_basic(self):
		v1, v2, v3 = version_objects()
		vs = [v1, v2, v3]

		t1 = v1['committed']
		t2 = v2['committed']
		t3 = v3['committed']

		fkgtA = Filter('key', '>', '/A')

		assert fkgtA(v1)
		assert fkgtA(v2)
		assert fkgtA(v3)

		assert fkgtA.value_passes('/BCDEG')
		assert fkgtA.value_passes('/ZCDEFDSA/fdsafdsa/fdsafdsaf')
		assert not fkgtA.value_passes('/6353456346543')
		assert not fkgtA.value_passes('.')
		assert fkgtA.value_passes('afsdafdsa')

		self.assertFilter(fkgtA, vs, vs)

		fkltA = Filter('key', '<', '/A')

		assert not fkltA(v1)
		assert not fkltA(v2)
		assert not fkltA(v3)

		assert not fkltA.value_passes('/BCDEG')
		assert not fkltA.value_passes('/ZCDEFDSA/fdsafdsa/fdsafdsaf')
		assert fkltA.value_passes('/6353456346543')
		assert fkltA.value_passes('.')
		assert not fkltA.value_passes('A')
		assert not fkltA.value_passes('afsdafdsa')

		self.assertFilter(fkltA, vs, [])

		fkeqA = Filter('key', '=', '/ABCD')

		assert fkeqA(v1)
		assert fkeqA(v2)
		assert fkeqA(v3)

		assert not fkeqA.value_passes('/BCDEG')
		assert not fkeqA.value_passes('/ZCDEFDSA/fdsafdsa/fdsafdsaf')
		assert not fkeqA.value_passes('/6353456346543')
		assert not fkeqA.value_passes('A')
		assert not fkeqA.value_passes('.')
		assert not fkeqA.value_passes('afsdafdsa')
		assert fkeqA.value_passes('/ABCD')

		self.assertFilter(fkeqA, vs, vs)
		self.assertFilter([fkeqA, fkltA], vs, [])
		self.assertFilter([fkeqA, fkeqA], vs, vs)

		fkgtB = Filter('key', '>', '/B')

		assert not fkgtB(v1)
		assert not fkgtB(v2)
		assert not fkgtB(v3)

		assert not fkgtB.value_passes('/A')
		assert fkgtB.value_passes('/BCDEG')
		assert fkgtB.value_passes('/ZCDEFDSA/fdsafdsa/fdsafdsaf')
		assert not fkgtB.value_passes('/6353456346543')
		assert not fkgtB.value_passes('.')
		assert fkgtB.value_passes('A')
		assert fkgtB.value_passes('afsdafdsa')

		self.assertFilter(fkgtB, vs, [])
		self.assertFilter([fkgtB, fkgtA], vs, [])
		self.assertFilter([fkgtB, fkgtB], vs, [])

		fkltB = Filter('key', '<', '/B')

		assert fkltB(v1)
		assert fkltB(v2)
		assert fkltB(v3)

		assert fkltB.value_passes('/A')
		assert not fkltB.value_passes('/BCDEG')
		assert not fkltB.value_passes('/ZCDEFDSA/fdsafdsa/fdsafdsaf')
		assert fkltB.value_passes('/6353456346543')
		assert fkltB.value_passes('.')
		assert not fkltB.value_passes('A')
		assert not fkltB.value_passes('afsdafdsa')

		self.assertFilter(fkltB, vs, vs)

		fkgtAB = Filter('key', '>', '/AB')

		assert fkgtAB(v1)
		assert fkgtAB(v2)
		assert fkgtAB(v3)

		assert not fkgtAB.value_passes('/A')
		assert fkgtAB.value_passes('/BCDEG')
		assert fkgtAB.value_passes('/ZCDEFDSA/fdsafdsa/fdsafdsaf')
		assert not fkgtAB.value_passes('/6353456346543')
		assert not fkgtAB.value_passes('.')
		assert fkgtAB.value_passes('A')
		assert fkgtAB.value_passes('afsdafdsa')

		self.assertFilter(fkgtAB, vs, vs)
		self.assertFilter([fkgtAB, fkltB], vs, vs)
		self.assertFilter([fkltB, fkgtAB], vs, vs)

		fgtet1 = Filter('committed', '>=', t1)
		fgtet2 = Filter('committed', '>=', t2)
		fgtet3 = Filter('committed', '>=', t3)

		assert fgtet1(v1)
		assert fgtet1(v2)
		assert fgtet1(v3)

		assert not fgtet2(v1)
		assert fgtet2(v2)
		assert fgtet2(v3)

		assert not fgtet3(v1)
		assert not fgtet3(v2)
		assert fgtet3(v3)

		self.assertFilter(fgtet1, vs, vs)
		self.assertFilter(fgtet2, vs, [v2, v3])
		self.assertFilter(fgtet3, vs, [v3])

		fltet1 = Filter('committed', '<=', t1)
		fltet2 = Filter('committed', '<=', t2)
		fltet3 = Filter('committed', '<=', t3)

		assert fltet1(v1)
		assert not fltet1(v2)
		assert not fltet1(v3)

		assert fltet2(v1)
		assert fltet2(v2)
		assert not fltet2(v3)

		assert fltet3(v1)
		assert fltet3(v2)
		assert fltet3(v3)

		self.assertFilter(fltet1, vs, [v1])
		self.assertFilter(fltet2, vs, [v1, v2])
		self.assertFilter(fltet3, vs, vs)

		self.assertFilter([fgtet2, fltet2], vs, [v2])
		self.assertFilter([fgtet1, fltet3], vs, vs)
		self.assertFilter([fgtet3, fltet1], vs, [])

		feqt1 = Filter('committed', '=', t1)
		feqt2 = Filter('committed', '=', t2)
		feqt3 = Filter('committed', '=', t3)

		assert feqt1(v1)
		assert not feqt1(v2)
		assert not feqt1(v3)

		assert not feqt2(v1)
		assert feqt2(v2)
		assert not feqt2(v3)

		assert not feqt3(v1)
		assert not feqt3(v2)
		assert feqt3(v3)

		self.assertFilter(feqt1, vs, [v1])
		self.assertFilter(feqt2, vs, [v2])
		self.assertFilter(feqt3, vs, [v3])

	def test_none(self):
		# test query against None
		feqnone = Filter('val', '=', None)
		vs = [{'val': None}, {'val': 'something'}]
		self.assertFilter(feqnone, vs, vs[0:1])

		feqzero = Filter('val', '=', 0)
		vs = [{'val': 0}, {'val': None}]
		self.assertFilter(feqzero, vs, vs[0:1])

	def test_object(self):
		t1 = time.time_ns()
		t2 = time.time_ns()

		f1 = Filter('key', '>', '/A')
		f2 = Filter('key', '<', '/A')
		f3 = Filter('committed', '=', t1)
		f4 = Filter('committed', '>=', t2)

		assert f1 == eval(repr(f1))
		assert f2 == eval(repr(f2))
		assert f3 == eval(repr(f3))
		assert f4 == eval(repr(f4))

		assert str(f1) == 'key > /A'
		assert str(f2) == 'key < /A'
		assert str(f3) == 'committed = %s' % t1
		assert str(f4) == 'committed >= %s' % t2

		assert f1 == Filter('key', '>', '/A')
		assert f2 == Filter('key', '<', '/A')
		assert f3 == Filter('committed', '=', t1)
		assert f4 == Filter('committed', '>=', t2)

		assert f2 != Filter('key', '>', '/A')
		assert f1 != Filter('key', '<', '/A')
		assert f4 != Filter('committed', '=', t1)
		assert f3 != Filter('committed', '>=', t2)

		assert hash(f1) == hash(Filter('key', '>', '/A'))
		assert hash(f2) == hash(Filter('key', '<', '/A'))
		assert hash(f3) == hash(Filter('committed', '=', t1))
		assert hash(f4) == hash(Filter('committed', '>=', t2))

		assert hash(f2) != hash(Filter('key', '>', '/A'))
		assert hash(f1) != hash(Filter('key', '<', '/A'))
		assert hash(f4) != hash(Filter('committed', '=', t1))
		assert hash(f3) != hash(Filter('committed', '>=', t2))


class TestOrder(unittest.TestCase):

	def test_basic(self):
		o1 = Order('key')
		o2 = Order('+committed')
		o3 = Order('-created')

		v1, v2, v3 = version_objects()

		# test  is_ascending
		assert o1.is_ascending()
		assert o2.is_ascending()
		assert not o3.is_ascending()

		# test key_fn
		assert o1.key_fn(v1) == (v1['key'])
		assert o1.key_fn(v2) == (v2['key'])
		assert o1.key_fn(v3) == (v3['key'])
		assert o1.key_fn(v1) == (v2['key'])
		assert o1.key_fn(v1) == (v3['key'])

		assert o2.key_fn(v1) == (v1['committed'])
		assert o2.key_fn(v2) == (v2['committed'])
		assert o2.key_fn(v3) == (v3['committed'])
		assert o2.key_fn(v1) != (v2['committed'])
		assert o2.key_fn(v1) != (v3['committed'])

		assert o3.key_fn(v1) == (v1['created'])
		assert o3.key_fn(v2) == (v2['created'])
		assert o3.key_fn(v3) == (v3['created'])
		assert o3.key_fn(v1) != (v2['created'])
		assert o3.key_fn(v1) != (v3['created'])

		# test sorted
		assert Order.sorted([v3, v2, v1], [o1]) == [v3, v2, v1]
		assert Order.sorted([v3, v2, v1], [o1, o2]) == [v1, v2, v3]
		assert Order.sorted([v1, v3, v2], [o1, o3]) == [v3, v2, v1]
		assert Order.sorted([v3, v2, v1], [o1, o2, o3]) == [v1, v2, v3]
		assert Order.sorted([v1, v3, v2], [o1, o3, o2]) == [v3, v2, v1]

		assert Order.sorted([v3, v2, v1], [o2]) == [v1, v2, v3]
		assert Order.sorted([v3, v2, v1], [o2, o1]) == [v1, v2, v3]
		assert Order.sorted([v3, v2, v1], [o2, o3]) == [v1, v2, v3]
		assert Order.sorted([v3, v2, v1], [o2, o1, o3]) == [v1, v2, v3]
		assert Order.sorted([v3, v2, v1], [o2, o3, o1]) == [v1, v2, v3]

		assert Order.sorted([v1, v2, v3], [o3]) == [v3, v2, v1]
		assert Order.sorted([v1, v2, v3], [o3, o2]) == [v3, v2, v1]
		assert Order.sorted([v1, v2, v3], [o3, o1]) == [v3, v2, v1]
		assert Order.sorted([v1, v2, v3], [o3, o2, o1]) == [v3, v2, v1]
		assert Order.sorted([v1, v2, v3], [o3, o1, o2]) == [v3, v2, v1]

	def test_object(self):
		assert Order('key') == eval(repr(Order('key')))
		assert Order('+committed') == eval(repr(Order('+committed')))
		assert Order('-created') == eval(repr(Order('-created')))

		assert str(Order('key')) == '+key'
		assert str(Order('+committed')) == '+committed'
		assert str(Order('-created')) == '-created'

		assert Order('key') == Order('+key')
		assert Order('-key') == Order('-key')
		assert Order('+committed') == Order('+committed')

		assert Order('key') != Order('-key')
		assert Order('+key') != Order('-key')
		assert Order('+committed') != Order('+key')

		assert hash(Order('+key')) == hash(Order('+key'))
		assert hash(Order('-key')) == hash(Order('-key'))
		assert hash(Order('+key')) != hash(Order('-key'))
		assert hash(Order('+committed')) == hash(Order('+committed'))
		assert hash(Order('+committed')) != hash(Order('+key'))


class TestQuery(unittest.TestCase):

	def test_basic(self):

		now = time.time_ns()

		q1 = Query(Key('/'), limit=100)
		q2 = Query(Key('/'), offset=200)
		q3 = Query(Key('/'), object_getattr=getattr)

		q1.offset = 300
		q3.limit = 1

		q1.filter('key', '>', '/ABC')
		q1.filter('created', '>', now)

		q2.order('key')
		q2.order('-created')

		q1d = {'key': '/', 'limit': 100, 'offset': 300,
		       'filter': [['key', '>', '/ABC'], ['created', '>', now]]}

		q2d = {'key': '/', 'offset': 200, 'order': ['+key', '-created']}

		q3d = {'key': '/', 'limit': 1}

		assert q1.dict() == q1d
		assert q2.dict() == q2d
		assert q3.dict() == q3d

		assert q1 == Query.from_dict(q1d)
		assert q2 == Query.from_dict(q2d)
		assert q3 == Query.from_dict(q3d)

		assert q1 == eval(repr(q1))
		assert q2 == eval(repr(q2))
		assert q3 == eval(repr(q3))

		assert q1 == q1.copy()
		assert q2 == q2.copy()
		assert q3 == q3.copy()

	def test_cursor(self):

		k = Key('/')

		with pytest.raises(ValueError):
		    Cursor(None, None)
		with pytest.raises(ValueError):
		    Cursor(Query(Key('/')), None)
		with pytest.raises(ValueError):
		    Cursor(None, [1])
		c = Cursor(Query(k), [1, 2, 3, 4, 5])  # should not raise

		assert c.skipped == 0
		assert c.returned == 0
		assert c._iterable == [1, 2, 3, 4, 5]

		c.skipped = 1
		c.returned = 2
		assert c.skipped == 1
		assert c.returned == 2

		c._skipped_inc(None)
		c._skipped_inc(None)
		assert c.skipped == 3

		c._returned_inc(None)
		c._returned_inc(None)
		c._returned_inc(None)
		assert c.returned == 5

		self.subtest_cursor(Query(k), [5, 4, 3, 2, 1], [5, 4, 3, 2, 1])
		self.subtest_cursor(Query(k, limit=3), [5, 4, 3, 2, 1], [5, 4, 3])
		self.subtest_cursor(Query(k, limit=0), [5, 4, 3, 2, 1], [])
		self.subtest_cursor(Query(k, offset=2), [5, 4, 3, 2, 1], [3, 2, 1])
		self.subtest_cursor(Query(k, offset=5), [5, 4, 3, 2, 1], [])
		self.subtest_cursor(Query(k, limit=2, offset=2), [5, 4, 3, 2, 1], [3, 2])

		v1, v2, v3 = version_objects()
		vs = [v1, v2, v3]

		t1 = v1['committed']
		t2 = v2['committed']
		t3 = v3['committed']

		self.subtest_cursor(Query(k), vs, vs)
		self.subtest_cursor(Query(k, limit=2), vs, [v1, v2])
		self.subtest_cursor(Query(k, offset=1), vs, [v2, v3])
		self.subtest_cursor(Query(k, offset=1, limit=1), vs, [v2])

		self.subtest_cursor(Query(k).filter('committed', '>=', t2), vs, [v2, v3])
		self.subtest_cursor(Query(k).filter('committed', '<=', t1), vs, [v1])

		self.subtest_cursor(Query(k).order('+committed'), vs, [v1, v2, v3])
		self.subtest_cursor(Query(k).order('-created'), vs, [v3, v2, v1])

	def subtest_cursor(self, query, iterable, expected_results):

		with pytest.raises(ValueError):
		    Cursor(None, None)
		with pytest.raises(ValueError):
		    Cursor(query, None)
		with pytest.raises(ValueError):
		    Cursor(None, iterable)
		cursor = Cursor(query, iterable)
		assert cursor.skipped == 0
		assert cursor.returned == 0

		cursor._ensure_modification_is_safe()
		cursor.apply_filter()
		cursor.apply_order()
		cursor.apply_offset()
		cursor.apply_limit()

		cursor_results = []
		for i in cursor:
			with pytest.raises(AssertionError):
			    cursor._ensure_modification_is_safe()
			with pytest.raises(AssertionError):
			    cursor.apply_filter()
			with pytest.raises(AssertionError):
			    cursor.apply_order()
			with pytest.raises(AssertionError):
			    cursor.apply_offset()
			with pytest.raises(AssertionError):
			    cursor.apply_limit()
			cursor_results.append(i)

		# ensure iteration happens only once.
		with pytest.raises(RuntimeError):
		    iter(cursor)

		assert cursor_results == expected_results
		assert cursor.returned == len(expected_results)
		assert cursor.skipped == query.offset
		if query.limit:
			assert cursor.returned <= query.limit
