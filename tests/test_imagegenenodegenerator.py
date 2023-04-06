#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `cellmaps_downloader` package."""

import os
import unittest
import tempfile
import shutil
import json
from unittest.mock import MagicMock
import cellmaps_downloader
from cellmaps_downloader.exceptions import CellMapsDownloaderError
from cellmaps_downloader.gene import GeneQuery
from cellmaps_downloader.gene import ImageGeneNodeAttributeGenerator

SKIP_REASON = 'CELLMAPS_DOWNLOADER_INTEGRATION_TEST ' \
              'environment variable not set, cannot run integration ' \
              'tests'


class TestAPMSGeneNodeAttributeGenerator(unittest.TestCase):
    """Tests for `cellmaps_downloader` package."""

    def setUp(self):
        """Set up test fixtures, if any."""

    def tearDown(self):
        """Tear down test fixtures, if any."""

    def test_get_antibody_list(self):
        gen = ImageGeneNodeAttributeGenerator(antibody_list='foo')
        self.assertEqual('foo', gen.get_antibody_list())


