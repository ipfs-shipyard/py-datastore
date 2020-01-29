# Python Asynchronous Datastore Abstraction (_py-datastore_)

[![Build Status](https://img.shields.io/travis/com/ipfs/py-datastore?style=flat-square)](https://travis-ci.com/ipfs/py-datastore)
[![Made by the IPFS Community](https://img.shields.io/badge/made%20by-IPFS%20Community-blue.svg?style=flat-square)](https://docs.ipfs.io/community/)
[![](https://img.shields.io/badge/project-IPFS-blue.svg?style=flat-square)](http://ipfs.io/)
[![IRC #py-ipfs on chat.freenode.net](https://img.shields.io/badge/freenode%20IRC-%23py--ipfs-blue.svg?style=flat-square)](http://webchat.freenode.net/?channels=%23py-ipfs)
[![Matrix #py-ipfs:ninetailed.ninja](https://img.shields.io/matrix/py-ipfs:ninetailed.ninja?color=blue&label=matrix+chat&style=flat-square)](https://matrix.to/#/#py-ipfs:ninetailed.ninja?via=ninetailed.ninja&via=librem.one)
[![Standard README Compliant](https://img.shields.io/badge/standard--readme-OK-green.svg?style=flat-square)](https://github.com/RichardLitt/standard-readme)

A simple, unified Python API for accessing multiple data stores like databases or file system directories asynchronously

py-datastore is a generic layer of abstraction for data store and database access.
It is a **simple** API with the aim to enable application development in a
datastore-agnostic way, allowing datastores to be swapped seamlessly without
changing application code. Thus, one can leverage different datastores with
different strengths without committing the application to one datastore
throughout its lifetime. It looks like this:

	+---------------+
	|  application  |    <--- No cumbersome SQL or Mongo specific queries!
	+---------------+
	        |            <--- simple datastore API calls
	+---------------+
	|   datastore   |    <--- datastore implementation for underlying db
	+---------------+
	        |            <--- database specific calls
	+---------------+
	|  various dbs  |    <--- MySQL, Redis, MongoDB, FS, …
	+---------------+

In addition, datastore adapters and collections significantly simplify creating
interesting data access patterns such as caching, sharding and tiering.

## Table of Contents

  * [Install](#install)
  * [Usage](#usage)
  * [Documentation](#documentation)
  * [API](#api)
  * [Maintainers](#maintainers)
  * [Thanks](#thanks)
  * [Contributing](#contributing)
     * [Other Datastore Implementations](#other-datastore-implementations)
  * [License](#license)

## Install

From pypi (WARNING – This is currently outdated!):

	pip install datastore

From source:

	git clone https://github.com/ipfs/py-datastore.git
	cd datastore
	flit install

### Datastore Implementations

Only the filesystem and the in-memory `DictDatastore` are included in this project.
For other known implementations please see the links below.

NOTE: Struck through entries are not compatible with the latest py-datastore releases!

  * ~~[datastore.aws](https://github.com/datastore/datastore.aws) - aws s3 implementation~~
  * ~~[datastore.git](https://github.com/datastore/datastore-git) - git implementation~~
  * ~~[datastore.mongo](https://github.com/datastore/datastore.mongo) - monogdb implementation~~
  * ~~[datastore.memcached](https://github.com/datastore/datastore.memcached) - memcached implementation~~
  * ~~[datastore.pylru](https://github.com/datastore/datastore.pylru) - pylru cache implementation~~
  * ~~[datastore.redis](https://github.com/datastore/datastore.redis) - redis implementation~~
  * ~~[datastore.leveldb](https://github.com/datastore/datastore.leveldb) - leveldb implementation~~

## Usage

TBD! Some **outdated** examples from the previous iteration of this library:

### Hello World

	>>> import datastore.core
	>>> ds = datastore.DictDatastore()
	>>>
	>>> hello = datastore.Key('hello')
	>>> ds.put(hello, 'world')
	>>> ds.contains(hello)
	True
	>>> ds.get(hello)
	'world'
	>>> ds.delete(hello)
	>>> ds.get(hello)
	None

#### Hello filesystem

	>>> import datastore.filesystem
	>>>
	>>> ds = datastore.filesystem.FileSystemDatastore('/tmp/.test_datastore')
	>>>
	>>> hello = datastore.Key('hello')
	>>> ds.put(hello, 'world')
	>>> ds.contains(hello)
	True
	>>> ds.get(hello)
	'world'
	>>> ds.delete(hello)
	>>> ds.get(hello)
	None


#### Hello Tiered Access


	>>> import pymongo
	>>> import datastore.core
	>>>
	>>> from datastore.mongo import MongoDatastore
	>>> from datastore.pylru import LRUCacheDatastore
	>>> from datastore.filesystem import FileSystemDatastore
	>>>
	>>> conn = pymongo.Connection()
	>>> mongo = MongoDatastore(conn.test_db)
	>>>
	>>> cache = LRUCacheDatastore(1000)
	>>> fs = FileSystemDatastore('/tmp/.test_db')
	>>>
	>>> ds = datastore.TieredDatastore([cache, mongo, fs])
	>>>
	>>> hello = datastore.Key('hello')
	>>> ds.put(hello, 'world')
	>>> ds.contains(hello)
	True
	>>> ds.get(hello)
	'world'
	>>> ds.delete(hello)
	>>> ds.get(hello)
	None


#### Hello Sharding

	>>> import datastore.core
	>>>
	>>> shards = [datastore.DictDatastore() for i in range(0, 10)]
	>>>
	>>> ds = datastore.ShardedDatastore(shards)
	>>>
	>>> hello = datastore.Key('hello')
	>>> ds.put(hello, 'world')
	>>> ds.contains(hello)
	True
	>>> ds.get(hello)
	'world'
	>>> ds.delete(hello)
	>>> ds.get(hello)
	None

## Documentation

The documentation can be found at (WARNING – This is currently outdated!):
http://datastore.readthedocs.org/en/latest/

## API

The datastore API places an emphasis on **simplicity** and elegance. Only four
core methods must be implemented (get, put, delete, query). There are two
variants of this API: object datastores and binary datastores. Object datastores
store streams of rich objects (like database rows or JSON objects), binary
datastores just store abitrary blobs of binary data (like filesystem files).

Object datastores may also impose additional limitations on the types and shape
of objects that may be stored. It is also not an error if the “object stream”
may only ever consist of a single object value.

### `get(key)`

Return the binary or object data stream named by key or raise `KeyError` if there
is no such value.

	Arguments
	---------
	key
		Key naming the binary or object data to retrieve

	Raises
	------
	KeyError
		The given key was not present in this datastore
	RuntimeError
		An internal error occurred

### `put(key, value)`

Store the object or binary data (stream) `value` into `key`. If the `key` does
not yet exist it will be created.

	Arguments
	---------
	key
		Key naming the binary or object data slot to store at
	value
		A list or stream of objects or bytes to store

	Raises
	------
	RuntimeError
		An internal error occurred

### `delete(key)`

Remove the object or binary data named by `key`. If it did not exist `KeyError`
will be raised.

	Arguments
	---------
	key
		Key naming the object or binary data slot to remove

	Raises
	------
	KeyError
		The given key was not present in this datastore
	RuntimeError
		An internal error occurred

### `query(query)` (object stores only)

Return an iterable of objects matching criteria expressed in `query`.

Only object datastores that are able to perform optimized queries
**should** implement this method. If a query is considered too complex
for your datastore implementation, raise `NotImplementedError` rather
then falling back to linear search.

	Arguments
	---------
	query
		Object describing which objects to match and return

	Raises
	------
	RuntimeError
		An internal error occurred


### Specialized Features

Datastore implementors are free to implement specialized features, pertinent
only to a subset of datastores, with the understanding that these should aim
for generality and will most likely not be implemented across other datastores.

When implementing such features, please remember the goal of this project:
A simple, unified API for multiple data stores. When making heavy use of a
particular library's specific functionality, perhaps one should not use
datastore and should directly use that library.

### Key

A Key represents the unique identifier of an object.

Our Key scheme is inspired by classic Unix file systems.

Keys are meant to be unique across a system. Keys are hierarchical,
incorporating increasingly specific namespaces. Thus keys can be deemed
*children* or *ancestors* of other keys.

	Key('/Comedy')
	Key('/Comedy/MontyPython')

## Maintainers

Currently active maintainers:

  * [Alexander Schlarb](https://ninetailed.ninja) ([@alexander255](https://github.com/alexander255)]
  * [@AliabbasMerchant](https://github.com/AliabbasMerchant)

Past maintainers:

  * [Juan Batiz-Benet](https://juan.benet.ai/) ([@jbenet](https://github.com/jbenet))

## Thanks

We'd like to thank the following contributers for their help (in order of appearance):

[@Seenivasanseeni](https://github.com/Seenivasanseeni), [@Swixx](https://github.com/Swixx)

## Contributing

### This Repository

For bug reports, feature requests and other issues please visit the [GitHub issue tracker](https://github.com/ipfs/py-datastore/issues).

Code contributions are primarily accepted through GitHub PRs. The general flow for submitting contributions is:

  1. Sign in to GitHub.
  2. Fork the repository by clicking on the “Fork” button on the top-right of [the repository page](https://github.com/ipfs/py-datastore) (you will have to switch to desktop mode on mobile).
  3. In your forked repository page click on the big green “Clone or download” button on the right-hand side and copy the SSH URL.
  4. On your system run: `git clone <COPIED_SSH_URL>` to download the repository.
  5. Make changes to your repository locally and commit them using `git add <CHANGED_FILES>`, then `git commit`. (If you have several related changes in mind please make one commit for each of them.)
      * Please also observe the next paragrah on things you should check *before* commiting!
  6. Push them to your repository using `git push`.
  7. In your repository page click on the “New pull request” button.
  8. Review the proposed changes and continue with “Create Pull Request”.
  9. Enter a suitable title and description for your changes and confirm by pressing “Create Pull Request” again.

When commiting code please run the tests first and code checkers first and try to fix all problems you observe:

  * Run tests using: `tox -e py3X` where `X` is your Python 3 minor version (ie: `7` for Python 3.7)
  * Check compliance with the code style: `tox -e styleck`
  * Check for static typing problems: `tox -e typeck`
  * Also note that all code should be documented!

Code in this repository follows the [PEP 8](http://www.python.org/dev/peps/pep-0008/) conventions for styling Python code with the exception of using tabs for indentation rather then spaces. In general, code that passes the style checker will be considered acceptable in this regard, so try to fix all issues reported by the style checker first before proposing a code change.

Please also be aware that all interactions related to IPFS, libp2p and Multiformats are subject to the IPFS [Code of Conduct](https://github.com/ipfs/community/blob/master/code-of-conduct.md).

### Other Datastore Implementations

Please write and contribute implementations for other data stores. This project
can only be complete with lots of help.

## License

py-datastore released under the MIT License.

py-datastore was orignally written by [Juan Batiz-Benet](https://juan.benet.ai/).
It was originally part of [py-dronestore](https://github.com/jbenet/py-dronestore)
before being rewritten in 2011.
