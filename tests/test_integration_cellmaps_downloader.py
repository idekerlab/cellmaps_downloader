#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Integration Tests for `cellmaps_downloader` package."""

import os

import unittest
from cellmaps_downloader import cellmaps_downloadercmd

SKIP_REASON = 'CELLMAPS_DOWNLOADER_INTEGRATION_TEST ' \
              'environment variable not set, cannot run integration ' \
              'tests'

@unittest.skipUnless(os.getenv('CELLMAPS_DOWNLOADER_INTEGRATION_TEST') is not None, SKIP_REASON)
class TestIntegrationCellmaps_downloader(unittest.TestCase):
    """Tests for `cellmaps_downloader` package."""

    def setUp(self):
        """Set up test fixtures, if any."""

    def tearDown(self):
        """Tear down test fixtures, if any."""

    def test_something(self):
        """Tests parse arguments"""
        self.assertEqual(1, 1)
