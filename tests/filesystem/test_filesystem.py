import tempfile
import os.path

import pytest
import trio.testing

from datastore.filesystem import FileSystemDatastore
from tests.conftest import DatastoreTests


@pytest.fixture
def temp_path():
	with tempfile.TemporaryDirectory() as temp_path:
		yield temp_path


@trio.testing.trio_test
async def test_datastore(temp_path):
	dirs = map(str, range(0, 4))
	dirs = map(lambda d: os.path.join(temp_path, d), dirs)
	fses = list(map(FileSystemDatastore, dirs))
	await DatastoreTests(fses).subtest_simple()
	
	# Check that all items were cleaned up
	for fs in fses:
		assert os.listdir(fs.root_path) == []
