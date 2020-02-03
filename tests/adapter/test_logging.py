import logging
import typing

import pytest
import trio.testing

from tests.adapter.conftest import make_datastore_test_params


_Logger = logging.Logger
if not typing.TYPE_CHECKING:
	_Logger = logging.getLoggerClass()


class NullLogger(_Logger):
	def debug(self, *args, **kwargs):
		pass

	def info(self, *args, **kwargs):
		pass

	def warning(self, *args, **kwargs):
		pass

	def error(self, *args, **kwargs):
		pass

	def critical(self, *args, **kwargs):
		pass


@pytest.mark.parametrize(*make_datastore_test_params("logging"))
@trio.testing.trio_test
async def test_logging_simple(DatastoreTests, Adapter, DictDatastore, encode_fn):
	s1 = Adapter(DictDatastore(), logger=NullLogger('null'))
	s2 = Adapter(DictDatastore())
	await DatastoreTests([s1, s2]).subtest_simple()
