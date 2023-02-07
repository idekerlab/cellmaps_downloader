#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `cellmaps_downloader` package."""


import unittest
from cellmaps_downloader.runner import CellmapsdownloaderRunner


class TestCellmapsdownloaderrunner(unittest.TestCase):
    """Tests for `cellmaps_downloader` package."""

    def setUp(self):
        """Set up test fixtures, if any."""

    def tearDown(self):
        """Tear down test fixtures, if any."""

    def test_constructor(self):
        """Tests constructor"""
        myobj = CellmapsdownloaderRunner(0)

        self.assertIsNotNone(myobj)

    def test_run(self):
        """ Tests run()"""
        myobj = CellmapsdownloaderRunner(4)
        self.assertEqual(4, myobj.run())
