__version__ = '1.1'
__author__ = 'Juan Batiz-Benet <juan@benet.ai>'
__doc__ = """
filesystem datastore implementation.

Tested with:
  * Journaled HFS+ (Mac OS X 10.7.2)

"""

from .filesystem import FileSystemDatastore
