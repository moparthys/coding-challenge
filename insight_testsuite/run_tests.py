#!/usr/bin/python
'''
This module runs all the unit tests defined in tests.py.
'''

__version__ = '0.0'
__author__ = 'Sriram Moparthy'
__email__ = 'moparthy26@gmail.com'

from os import path
import sys
sys.path.append(path.abspath('../src'))

import unittest
import json

from __main__ import *
from tests import *

if __name__ == '__main__':
	test = SixtySecondWindowTest()
	test.runTest()
	test = UpdateGraphTest()
	test.runTest()
	test = ComputeMedianTest()
	test.runTest()
	test = ChallengeTest()
	test.runTest()
