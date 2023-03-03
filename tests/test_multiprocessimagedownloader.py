#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `cellmaps_downloader` package."""

import os
import unittest
import tempfile
import shutil
import requests_mock
import json
import cellmaps_downloader
from cellmaps_downloader.exceptions import CellMapsDownloaderError
from cellmaps_downloader.runner import CellmapsdownloaderRunner
from cellmaps_downloader.runner import ImageDownloader
from cellmaps_downloader import runner


class TestMultiprocessImageDownloader(unittest.TestCase):
    """Tests for `cellmaps_downloader` package."""

    def setUp(self):
        """Set up test fixtures, if any."""

    def tearDown(self):
        """Tear down test fixtures, if any."""

    def test_image_downloader(self):
        dloader = ImageDownloader()
        try:
            dloader.download_images()
            self.fail('Expected Exception')
        except CellMapsDownloaderError as ce:
            self.assertEqual('Subclasses should implement this', str(ce))
