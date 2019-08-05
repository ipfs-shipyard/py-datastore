#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re
from setuptools import setup, find_packages

pkgname = 'datastore'

# gather the package information
main_py = open('datastore/core/__init__.py').read()
metadata = dict(re.findall("__([a-z]+)__ = '([^']+)'", main_py))

setup(
  name=pkgname,
  version=metadata['version'],
  description='simple, unified API for multiple data stores',
  long_description_markdown_filename='README.md',
  author=metadata['author'],
  author_email=metadata['email'],
  url='http://github.com/ipfs/py-datastore',
  keywords=[
    'datastore',
    'unified api',
    'database',
  ],
  packages=find_packages(include='datastore'),
  namespace_packages=['datastore'],
  # test_suite='datastore.test',
  license='MIT License',
  classifiers=[
    'Topic :: Database',
    'Topic :: Database :: Front-Ends',
  ],
  include_package_data=True,
  py_modules=['datastore'],
)
