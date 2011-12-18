
import unittest

import datastore
from datastore import Key
from datastore import Query


class TestDatastore(unittest.TestCase):

  def test_simple(self, stores=[], numelems=100):

    def checkLength(len):
      try:
        for sn in stores:
          self.assertEqual(len(sn), numelems)
      except TypeError, e:
        pass

    if len(stores) == 0:
      s1 = datastore.DictDatastore()
      s2 = datastore.DictDatastore()
      s3 = datastore.DictDatastore()
      stores = [s1, s2, s3]

    pkey = Key('/dfadasfdsafdas/')

    checkLength(0)

    # ensure removing non-existent keys is ok.
    for value in range(0, numelems):
      key = pkey.child(value)
      for sn in stores:
        self.assertFalse(sn.contains(key))
        sn.delete(key)
        self.assertFalse(sn.contains(key))

    checkLength(0)

    # insert numelems elems
    for value in range(0, numelems):
      key = pkey.child(value)
      for sn in stores:
        self.assertFalse(sn.contains(key))
        sn.put(key, value)
        self.assertTrue(sn.contains(key))
        self.assertEqual(sn.get(key), value)

    # reassure they're all there.
    checkLength(numelems)

    # reversed for LRUCache's benefit...
    for value in reversed(range(0, numelems)):
      key = pkey.child(value)
      for sn in stores:
        self.assertTrue(sn.contains(key))
        self.assertEqual(sn.get(key), value)

    checkLength(numelems)

    def test_query(query, expected):
      for sn in stores:
        try:
          result = list(sn.query(query))
          self.assertTrue(len(result) == len(expected))
          self.assertTrue(all([val in result for val in expected]))
          self.assertEqual(result, expected)

        except NotImplementedError:
          pass

    for sn in stores:
      dsresults = list(stores[0].query(Query()))

    n = int(numelems)
    test_query(Query(), list(range(0, n))) # make sure everything is there.
    test_query(Query(), dsresults[:n])
    test_query(Query(limit=n), dsresults[:n])
    test_query(Query(limit=n/2), dsresults[:n/2])
    test_query(Query(offset=n/2), dsresults[n/2:])
    test_query(Query(offset=n/3, limit=n/3), dsresults[n/3: 2*n/3])
    del n

    # change numelems elems
    for value in range(0, numelems):
      key = pkey.child(value)
      for sn in stores:
        self.assertTrue(sn.contains(key))
        sn.put(key, value + 1)
        self.assertTrue(sn.contains(key))
        self.assertNotEqual(value, sn.get(key))
        self.assertEqual(value + 1, sn.get(key))

    checkLength(numelems)

    # remove numelems elems
    for value in range(0, numelems):
      key = pkey.child(value)
      for sn in stores:
        self.assertTrue(sn.contains(key))
        sn.delete(key)
        self.assertFalse(sn.contains(key))

    checkLength(0)


class TestDatastoreCollection(TestDatastore):

  def test_tiered(self):

    s1 = datastore.DictDatastore()
    s2 = datastore.DictDatastore()
    s3 = datastore.DictDatastore()
    ts = datastore.TieredDatastore([s1, s2, s3])

    k1 = Key('1')
    k2 = Key('2')
    k3 = Key('3')

    s1.put(k1, '1')
    s2.put(k2, '2')
    s3.put(k3, '3')

    self.assertTrue(s1.contains(k1))
    self.assertFalse(s2.contains(k1))
    self.assertFalse(s3.contains(k1))
    self.assertTrue(ts.contains(k1))

    self.assertEqual(ts.get(k1), '1')
    self.assertEqual(s1.get(k1), '1')
    self.assertFalse(s2.contains(k1))
    self.assertFalse(s3.contains(k1))

    self.assertFalse(s1.contains(k2))
    self.assertTrue(s2.contains(k2))
    self.assertFalse(s3.contains(k2))
    self.assertTrue(ts.contains(k2))

    self.assertEqual(s2.get(k2), '2')
    self.assertFalse(s1.contains(k2))
    self.assertFalse(s3.contains(k2))

    self.assertEqual(ts.get(k2), '2')
    self.assertEqual(s1.get(k2), '2')
    self.assertEqual(s2.get(k2), '2')
    self.assertFalse(s3.contains(k2))

    self.assertFalse(s1.contains(k3))
    self.assertFalse(s2.contains(k3))
    self.assertTrue(s3.contains(k3))
    self.assertTrue(ts.contains(k3))

    self.assertEqual(s3.get(k3), '3')
    self.assertFalse(s1.contains(k3))
    self.assertFalse(s2.contains(k3))

    self.assertEqual(ts.get(k3), '3')
    self.assertEqual(s1.get(k3), '3')
    self.assertEqual(s2.get(k3), '3')
    self.assertEqual(s3.get(k3), '3')

    ts.delete(k1)
    ts.delete(k2)
    ts.delete(k3)

    self.assertFalse(ts.contains(k1))
    self.assertFalse(ts.contains(k2))
    self.assertFalse(ts.contains(k3))

    self.test_simple([ts])

  def test_sharded(self, numelems=1000):

    s1 = datastore.DictDatastore()
    s2 = datastore.DictDatastore()
    s3 = datastore.DictDatastore()
    s4 = datastore.DictDatastore()
    s5 = datastore.DictDatastore()
    stores = [s1, s2, s3, s4, s5]
    hash = lambda key: int(key.name) * len(stores) / numelems
    sharded = datastore.ShardedDatastore(stores, shardingfn=hash)
    sumlens = lambda stores: sum(map(lambda s: len(s), stores))

    def checkFor(key, value, sharded, shard=None):
      correct_shard = sharded._stores[hash(key) % len(sharded._stores)]

      for s in sharded._stores:
        if shard and s == shard:
          self.assertTrue(s.contains(key))
          self.assertEqual(s.get(key), value)
        else:
          self.assertFalse(s.contains(key))

      if correct_shard == shard:
        self.assertTrue(sharded.contains(key))
        self.assertEqual(sharded.get(key), value)
      else:
        self.assertFalse(sharded.contains(key))

    self.assertEqual(sumlens(stores), 0)
    # test all correct.
    for value in range(0, numelems):
      key = Key('/fdasfdfdsafdsafdsa/%d' % value)
      shard = stores[hash(key) % len(stores)]
      checkFor(key, value, sharded)
      shard.put(key, value)
      checkFor(key, value, sharded, shard)
    self.assertEqual(sumlens(stores), numelems)

    # ensure its in the same spots.
    for i in range(0, numelems):
      key = Key('/fdasfdfdsafdsafdsa/%d' % value)
      shard = stores[hash(key) % len(stores)]
      checkFor(key, value, sharded, shard)
      shard.put(key, value)
      checkFor(key, value, sharded, shard)
    self.assertEqual(sumlens(stores), numelems)

    # ensure its in the same spots.
    for value in range(0, numelems):
      key = Key('/fdasfdfdsafdsafdsa/%d' % value)
      shard = stores[hash(key) % len(stores)]
      checkFor(key, value, sharded, shard)
      sharded.put(key, value)
      checkFor(key, value, sharded, shard)
    self.assertEqual(sumlens(stores), numelems)

    # ensure its in the same spots.
    for value in range(0, numelems):
      key = Key('/fdasfdfdsafdsafdsa/%d' % value)
      shard = stores[hash(key) % len(stores)]
      checkFor(key, value, sharded, shard)
      if value % 2 == 0:
        shard.delete(key)
      else:
        sharded.delete(key)
      checkFor(key, value, sharded)
    self.assertEqual(sumlens(stores), 0)

    # try out adding it to the wrong shards.
    for value in range(0, numelems):
      key = Key('/fdasfdfdsafdsafdsa/%d' % value)
      incorrect_shard = stores[(hash(key) + 1) % len(stores)]
      checkFor(key, value, sharded)
      incorrect_shard.put(key, value)
      checkFor(key, value, sharded, incorrect_shard)
    self.assertEqual(sumlens(stores), numelems)

    # ensure its in the same spots.
    for value in range(0, numelems):
      key = Key('/fdasfdfdsafdsafdsa/%d' % value)
      incorrect_shard = stores[(hash(key) + 1) % len(stores)]
      checkFor(key, value, sharded, incorrect_shard)
      incorrect_shard.put(key, value)
      checkFor(key, value, sharded, incorrect_shard)
    self.assertEqual(sumlens(stores), numelems)

    # this wont do anything
    for value in range(0, numelems):
      key = Key('/fdasfdfdsafdsafdsa/%d' % value)
      incorrect_shard = stores[(hash(key) + 1) % len(stores)]
      checkFor(key, value, sharded, incorrect_shard)
      sharded.delete(key)
      checkFor(key, value, sharded, incorrect_shard)
    self.assertEqual(sumlens(stores), numelems)

    # this will place it correctly.
    for value in range(0, numelems):
      key = Key('/fdasfdfdsafdsafdsa/%d' % value)
      incorrect_shard = stores[(hash(key) + 1) % len(stores)]
      correct_shard = stores[(hash(key)) % len(stores)]
      checkFor(key, value, sharded, incorrect_shard)
      sharded.put(key, value)
      incorrect_shard.delete(key)
      checkFor(key, value, sharded, correct_shard)
    self.assertEqual(sumlens(stores), numelems)

    # this will place it correctly.
    for value in range(0, numelems):
      key = Key('/fdasfdfdsafdsafdsa/%d' % value)
      correct_shard = stores[(hash(key)) % len(stores)]
      checkFor(key, value, sharded, correct_shard)
      sharded.delete(key)
      checkFor(key, value, sharded)
    self.assertEqual(sumlens(stores), 0)

    self.test_simple([sharded])


if __name__ == '__main__':
  unittest.main()