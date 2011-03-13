#!/usr/bin/python2

import unittest

class TestSequenceFunctions(unittest.TestCase):

  def setUp(self):
    self.seq = range(10)
  def tearDown(self):
    pass


if __name__ == '__main__':
    unittest.main()
