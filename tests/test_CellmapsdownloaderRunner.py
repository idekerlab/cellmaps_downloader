#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `cellmaps_downloader` package."""

import os
import unittest
import tempfile
import shutil
import requests_mock

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

    def test_download_file(self):
        temp_dir = tempfile.mkdtemp()

        try:
            mockurl = 'http://fakey.fake.com/ha.txt'
            runner = CellmapsdownloaderRunner(0)
            with requests_mock.mock() as m:
                m.get(mockurl, status_code=200,
                      text='somedata')
                a_dest_file = os.path.join(temp_dir, 'downloadedfile.txt')
                runner._download_file(mockurl, a_dest_file)
            self.assertTrue(os.path.isfile(a_dest_file))
            with open(a_dest_file, 'r') as f:
                data = f.read()
                self.assertEqual('somedata', data)
        finally:
            shutil.rmtree(temp_dir)
