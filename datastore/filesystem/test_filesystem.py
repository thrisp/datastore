import os

import shutil
import unittest

from datastore.core import serialize
from datastore.tests import TestDatastore

from . import FileSystemDatastore


class TestFileSystemDatastore(TestDatastore):
    tmp = os.path.normpath('/tmp/datastore.test.fs')

    def setUp(self):
        if os.path.exists(self.tmp):
            shutil.rmtree(self.tmp)

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def test_datastore(self):
        dirs = list(map(str, range(0, 4)))
        dirs = list(map(lambda d: os.path.join(self.tmp, d), dirs))
        fses = list(map(FileSystemDatastore, dirs))
        dses = list(map(serialize.shim, fses))
        self.subtest_simple(dses, numelems=49)


if __name__ == '__main__':
  unittest.main()
