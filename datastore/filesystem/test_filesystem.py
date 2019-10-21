import os
import shutil

from datastore.core import serialize
from datastore.core.test.test_basic import TestDatastore

from datastore.filesystem import FileSystemDatastore


class TestFileSystemDatastore(TestDatastore):
    tmp = os.path.normpath('/tmp/datastore.test.fs')

    def setUp(self):
        if os.path.exists(self.tmp):
            shutil.rmtree(self.tmp)

    def tearDown(self):
        if os.path.exists(self.tmp):
            shutil.rmtree(self.tmp)

    def test_datastore(self):
        dirs = map(str, range(0, 4))
        dirs = map(lambda d: os.path.join(self.tmp, d), dirs)
        fses = map(FileSystemDatastore, dirs)
        dses = list(map(serialize.shim, fses))
        self.subtest_simple(dses, numelems=500)

