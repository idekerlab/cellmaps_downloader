#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `cellmaps_downloader` package."""

import os
import unittest
import tempfile
import shutil
import requests_mock
from unittest.mock import MagicMock
from unittest.mock import Mock
import json
import cellmaps_downloader
from cellmaps_downloader.exceptions import CellMapsDownloaderError
from cellmaps_downloader.runner import CellmapsdownloaderRunner
from cellmaps_downloader.runner import ImageDownloader
from cellmaps_downloader.gene import ImageGeneNodeAttributeGenerator
from cellmaps_downloader import runner


class TestCellmapsdownloaderrunner(unittest.TestCase):
    """Tests for `cellmaps_downloader` package."""

    def setUp(self):
        """Set up test fixtures, if any."""

    def tearDown(self):
        """Tear down test fixtures, if any."""

    def test_constructor(self):
        """Tests constructor"""
        myobj = CellmapsdownloaderRunner()
        self.assertIsNotNone(myobj)

    def test_run(self):
        """ Tests run()"""
        myobj = CellmapsdownloaderRunner()
        try:
            myobj.run()
            self.fail('Expected CellMapsDownloaderError')
        except CellMapsDownloaderError as c:
            self.assertTrue('Output directory is None' in str(c))

    def test_download_file(self):
        temp_dir = tempfile.mkdtemp()

        try:
            mockurl = 'http://fakey.fake.com/ha.txt'

            with requests_mock.mock() as m:
                m.get(mockurl, status_code=200,
                      text='somedata')
                a_dest_file = os.path.join(temp_dir, 'downloadedfile.txt')
                runner.download_file((mockurl, a_dest_file))
            self.assertTrue(os.path.isfile(a_dest_file))
            with open(a_dest_file, 'r') as f:
                data = f.read()
                self.assertEqual('somedata', data)
        finally:
            shutil.rmtree(temp_dir)

    def test_download_file_failure(self):
        temp_dir = tempfile.mkdtemp()

        try:
            mockurl = 'http://fakey.fake.com/ha.txt'

            with requests_mock.mock() as m:
                m.get(mockurl, status_code=500,
                      text='error')
                a_dest_file = os.path.join(temp_dir, 'downloadedfile.txt')
                rstatus, rtext, rtuple = runner.download_file((mockurl, a_dest_file))
            self.assertEqual(500, rstatus)
            self.assertEqual('error', rtext)
            self.assertEqual((mockurl, a_dest_file), rtuple)
            self.assertFalse(os.path.isfile(a_dest_file))

        finally:
            shutil.rmtree(temp_dir)

    def test_download_file_skip_existing_empty_file_exists(self):
        temp_dir = tempfile.mkdtemp()

        try:
            mockurl = 'http://fakey.fake.com/ha.txt'

            with requests_mock.mock() as m:
                m.get(mockurl, status_code=200,
                      text='somedata')
                a_dest_file = os.path.join(temp_dir, 'downloadedfile.txt')
                open(a_dest_file, 'a').close()

                runner.download_file_skip_existing((mockurl, a_dest_file))
            self.assertTrue(os.path.isfile(a_dest_file))
            with open(a_dest_file, 'r') as f:
                data = f.read()
                self.assertEqual('somedata', data)
        finally:
            shutil.rmtree(temp_dir)

    def test_download_file_skip_existing_file_exists(self):
        temp_dir = tempfile.mkdtemp()

        try:
            mockurl = 'http://fakey.fake.com/ha.txt'

            with requests_mock.mock() as m:
                m.get(mockurl, status_code=200,
                      text='somedata')
                a_dest_file = os.path.join(temp_dir, 'downloadedfile.txt')
                with open(a_dest_file, 'w') as f:
                    f.write('blah')

                self.assertIsNone(runner.download_file_skip_existing((mockurl, a_dest_file)))
            self.assertTrue(os.path.isfile(a_dest_file))
            with open(a_dest_file, 'r') as f:
                data = f.read()
                self.assertEqual('blah', data)
        finally:
            shutil.rmtree(temp_dir)

    def test_create_output_directory(self):
        temp_dir = tempfile.mkdtemp()
        try:
            crunner = CellmapsdownloaderRunner(outdir=temp_dir)
            crunner._create_output_directory()
            for c in CellmapsdownloaderRunner.COLORS:
                self.assertTrue(os.path.isdir(os.path.join(temp_dir, c)))
        finally:
            shutil.rmtree(temp_dir)

    def test_write_task_start_json(self):
        temp_dir = tempfile.mkdtemp()
        try:
            crunner = CellmapsdownloaderRunner(outdir=temp_dir)
            crunner._create_output_directory()
            crunner._write_task_start_json()
            start_file = None
            for entry in os.listdir(temp_dir):
                if not entry.endswith('_start.json'):
                    continue
                start_file = os.path.join(temp_dir, entry)
            self.assertIsNotNone(start_file)

            with open(start_file, 'r') as f:
                data = json.load(f)

            self.assertEqual(cellmaps_downloader.__version__,
                             data['version'])
            self.assertTrue(data['start_time'] > 0)
            self.assertEqual(temp_dir, data['outdir'])
        finally:
            shutil.rmtree(temp_dir)

    def test_get_download_tuples_from_csv(self):
        temp_dir = tempfile.mkdtemp()
        try:
            samples = [{'if_plate_id': '1',
                        'position': 'A1',
                        'sample': '1',
                        'antibody': 'HPA000992'},
                       {'if_plate_id': '2',
                        'position': 'A3',
                        'sample': '4',
                        'antibody': 'HPA000992'}
                       ]

            imagegen = ImageGeneNodeAttributeGenerator(samples_list=samples)

            link = 'http://foo'
            suffix = '.jpg'
            crunner = CellmapsdownloaderRunner(outdir=temp_dir,
                                               image_url=link,
                                               imgsuffix=suffix,
                                               imagegen=imagegen)
            dtuples = crunner._get_download_tuples_from_csv()

            self.assertEqual(8, len(dtuples))
            for c in CellmapsdownloaderRunner.COLORS:
                for fname in ['1_A1_1_', '2_A3_4_']:
                    self.assertTrue((link + '/992/' + fname + c + suffix,
                                     os.path.join(temp_dir, c,
                                                  fname +
                                                  c + suffix)) in dtuples)

        finally:
            shutil.rmtree(temp_dir)
