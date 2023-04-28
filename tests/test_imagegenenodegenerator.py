#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `cellmaps_downloader` package."""

import os
import unittest
import tempfile
import shutil
import json
from unittest.mock import patch, mock_open
from unittest.mock import MagicMock
import cellmaps_downloader
from cellmaps_downloader.exceptions import CellMapsDownloaderError
from cellmaps_downloader.gene import GeneQuery
from cellmaps_downloader.gene import ImageGeneNodeAttributeGenerator


class TestAPMSGeneNodeAttributeGenerator(unittest.TestCase):
    """Tests for `cellmaps_downloader` package."""

    def setUp(self):
        """Set up test fixtures, if any."""

    def tearDown(self):
        """Tear down test fixtures, if any."""

    def test_get_antibody_list(self):
        gen = ImageGeneNodeAttributeGenerator(antibody_list='foo')
        self.assertEqual('foo', gen.get_antibody_list())

    def test_get_image_antibodies_from_csvfile(self):
        try:
            # Test case when csvfile is None
            result = ImageGeneNodeAttributeGenerator.get_image_antibodies_from_csvfile()
            self.fail('Expected exception')
        except CellMapsDownloaderError as ce:
            self.assertEqual('csvfile is None', str(ce))

        # Test case when file is not found
        with self.assertRaises(FileNotFoundError):
            ImageGeneNodeAttributeGenerator.get_image_antibodies_from_csvfile('non_existent_file.csv')

        # Test case when csvfile is empty
        with patch('builtins.open', mock_open(read_data='')):
            result = ImageGeneNodeAttributeGenerator.get_image_antibodies_from_csvfile('test.csv')
            self.assertEqual(result, [])

        # Test case when csvfile has data
        csv_data = 'antibody,ensembl_ids,gene_names,atlas_name,' \
                   'locations,n_location\nABC,ENSG00000171921,CDK5,' \
                   'Brain,CA1,1\nDEF,ENSG00000173672,GAD2,Brain,CA2,' \
                   '2\nGHI,ENSG00000172137,DLG4,Brain,CA3,3\n'
        with patch('builtins.open', mock_open(read_data=csv_data)):
            result = ImageGeneNodeAttributeGenerator.get_image_antibodies_from_csvfile('test.csv')
            expected_result = [{'antibody': 'ABC',
                                'ensembl_ids': 'ENSG00000171921',
                                'gene_names': 'CDK5', 'atlas_name': 'Brain',
                                'locations': 'CA1', 'n_location': '1'},
                               {'antibody': 'DEF',
                                'ensembl_ids': 'ENSG00000173672',
                                'gene_names': 'GAD2', 'atlas_name': 'Brain',
                                'locations': 'CA2', 'n_location': '2'},
                               {'antibody': 'GHI',
                                'ensembl_ids': 'ENSG00000172137',
                                'gene_names': 'DLG4', 'atlas_name': 'Brain',
                                'locations': 'CA3', 'n_location': '3'}]
            self.assertEqual(result, expected_result)
            
    def test_get_samples_from_tsvfile(self):
        try:
            # Test case when tsvfile is None
            ImageGeneNodeAttributeGenerator.get_samples_from_tsvfile()
            self.fail('Expected exception')
        except CellMapsDownloaderError as ce:
            self.assertEqual('tsvfile is None', str(ce))

        # Test case when file is not found
        with self.assertRaises(FileNotFoundError):
            ImageGeneNodeAttributeGenerator.get_samples_from_tsvfile('non_existent_file.tsv')

        # Test case when tsvfile is empty
        with patch('builtins.open', mock_open(read_data='')):
            result = ImageGeneNodeAttributeGenerator.get_samples_from_tsvfile('test.tsv')
            self.assertEqual(result, [])

        # Test case when tsvfile has data
        tsv_data = 'gene_names\tfile_link\tfile_name\nCDK5\thttps://example.com/cdk5_1.txt\tcdk5_1.txt\nGAD2\thttps://example.com/gad2_1.txt\tgad2_1.txt\nDLG4\thttps://example.com/dlg4_1.txt\tdlg4_1.txt\n'
        with patch('builtins.open', mock_open(read_data=tsv_data)):
            result = ImageGeneNodeAttributeGenerator.get_samples_from_tsvfile('test.tsv')
            expected_result = [{'gene_names': 'CDK5', 'file_link': 'https://example.com/cdk5_1.txt', 'file_name': 'cdk5_1.txt'},
                               {'gene_names': 'GAD2', 'file_link': 'https://example.com/gad2_1.txt', 'file_name': 'gad2_1.txt'},
                               {'gene_names': 'DLG4', 'file_link': 'https://example.com/dlg4_1.txt', 'file_name': 'dlg4_1.txt'}]
            self.assertEqual(result, expected_result)

    def test_fun(self):
        samples_list = ImageGeneNodeAttributeGenerator.get_samples_from_tsvfile(tsvfile='')
