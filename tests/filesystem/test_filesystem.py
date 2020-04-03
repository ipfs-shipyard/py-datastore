import contextlib
import json
import os.path
import queue as queue_
import tempfile
import traceback

import pytest
import trio
import trio.testing

import datastore
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
	async with contextlib.AsyncExitStack() as stack:
		fses = [
			stack.push_async_exit(await FileSystemDatastore.create(next(dirs))),
			stack.push_async_exit(await FileSystemDatastore.create(next(dirs), stats=True)),
			stack.push_async_exit(await FileSystemDatastore.create(next(dirs), case_sensitive=False)),
			stack.push_async_exit(await FileSystemDatastore.create(next(dirs), stats=True, case_sensitive=False)),
		]
		
		await DatastoreTests(fses).subtest_simple()
		
		# Check that all items were cleaned up
		for fs in fses:
			if fs.stats:
				if fs.case_sensitive:
					assert os.listdir(fs.root_path) == ["diskUsage.data"]
				else:
					assert os.listdir(fs.root_path) == ["diskusage.data"]
			else:
				assert os.listdir(fs.root_path) == []


async def concurrent_datastore(temp_path, queue):
	# Mark "start" task as done
	queue.get()
	queue.task_done()
	
	async with FileSystemDatastore.create(temp_path, stats=True) as fs:
		op = None
		while op != "quit":
			op, key, data = queue.get()
			try:
				if op == "put":
					await fs.put(key, data)
				elif op == "put_new":
					await fs.put_new(key, data)
				elif op == "flush":
					await fs.flush()
				else:
					assert False
			except BaseException:
				traceback.print_exc()
			finally:
				queue.task_done()


@pytest.mark.repeat(10)
@trio.testing.trio_test
async def test_stats_concurrent(temp_path):
	async with trio.open_nursery() as nursery:
		queue = queue_.Queue()
		async with FileSystemDatastore.create(temp_path, stats=True) as fs:
			assert fs.datastore_stats().size == 0
			assert fs.datastore_stats().size_accuracy == "exact"
			
			await fs.put(datastore.Key("/a"), b"1234")
			
			assert fs.datastore_stats().size == 4
			await fs.flush()
			assert fs.datastore_stats().size == 4
			
			# Start concurrent datastore on separate thread
			queue.put(("start", None, None))
			nursery.start_soon(
				trio.to_thread.run_sync, trio.run, concurrent_datastore, temp_path, queue
			)
			try:
				# Wait for thread to read dummy value from queue
				while not queue.empty():
					await trio.sleep(0)
				
				# Perform concurrent write
				queue.put(("put", datastore.Key("/b"), b"Hallo Welt"))
				queue.put(("put_new", datastore.Key("/"), b"Hallo Welt"))
				queue.join()
				
				# Our datastore wouldn't know about it yet
				assert fs.datastore_stats().size == 4
				
				# Flush the remote datastore
				queue.put(("flush", None, None))
				queue.join()
				
				# Our datastore still wouldn't know about it yet
				assert fs.datastore_stats().size == 4
				
				# Write something more into our datastore
				await fs.put(datastore.Key("/c"), b"1234")
				key_new = await fs.put_new(datastore.Key("/"), b"1234")
				
				# Our datastore still will still only know about its changes
				assert fs.datastore_stats().size == 12
				
				# Let's replace "/c"
				await fs.rename(key_new, datastore.Key("/c"))
				
				# Our datastore still will still only know about its changes
				assert fs.datastore_stats().size == 8
				
				# Flush our datastore after theirs
				await fs.flush()
				
				# Now we see their change
				assert fs.datastore_stats().size == 28
			finally:
				queue.put(("quit", None, None))


@trio.testing.trio_test
async def test_stats_tracking(temp_path):
	async with FileSystemDatastore.create(temp_path, stats=True) as fs:
		assert fs.datastore_stats().size == 0
		assert fs.datastore_stats().size_accuracy == "exact"
		
		# Write to datastore key
		await fs.put(datastore.Key("/a"), b"1234")
		
		assert fs.datastore_stats().size == 4
		
		# Overwrite datastore key
		await fs.put(datastore.Key("/a"), b"Hallo Welt")
		
		assert fs.datastore_stats().size == 10
		
		# Create new datastore key
		key_new = await fs.put_new(datastore.Key("/"), b"TC")
		
		assert fs.datastore_stats().size == 12
		
		# Remove datastore keys
		await fs.delete(datastore.Key("/a"))
		
		assert fs.datastore_stats().size == 2
		
		await fs.delete(key_new)
		
		assert fs.datastore_stats().size == 0


@trio.testing.trio_test
async def test_stats_restore(temp_path):
	async with FileSystemDatastore.create(temp_path, stats=True) as fs:
		assert fs.datastore_stats().size == 0
		assert fs.datastore_stats().size_accuracy == "exact"
		
		await fs.put(datastore.Key("/a"), b"1234")
		
		assert fs.datastore_stats().size == 4
	
	# Re-open datastore and check that the stats are still there
	async with FileSystemDatastore.create(temp_path, stats=True) as fs:
		assert fs.datastore_stats().size == 4
		assert fs.datastore_stats().size_accuracy == "exact"
	
	# Replace content with stuff written by a non-cooperating datastore user
	await (trio.Path(temp_path) / "diskUsage.data").write_text(
		json.dumps({"diskUsage": 3, "accuracy": "initial-exact"}), encoding="utf-8"
	)
	
	# Re-open datastore and check that the non-cooperating stats data is
	# properly merged
	async with FileSystemDatastore.create(temp_path, stats=True) as fs:
		assert fs.datastore_stats().size == 7
		assert fs.datastore_stats().size_accuracy == "exact"
