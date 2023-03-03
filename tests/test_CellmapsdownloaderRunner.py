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

    def test_create_output_directory(self):
        temp_dir = tempfile.mkdtemp()
        try:
            runner = CellmapsdownloaderRunner(outdir=temp_dir)
            runner._create_output_directory()
            for c in CellmapsdownloaderRunner.COLORS:
                self.assertTrue(os.path.isdir(os.path.join(temp_dir, c)))
        finally:
            shutil.rmtree(temp_dir)

    def test_write_task_start_json_tsv_not_set(self):
        temp_dir = tempfile.mkdtemp()
        try:
            runner = CellmapsdownloaderRunner(outdir=temp_dir)
            runner._create_output_directory()
            runner._write_task_start_json()
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
            self.assertEqual('Not set', data['tsvfile'])
            self.assertTrue(data['start_time'] > 0)
            self.assertEqual(temp_dir, data['outdir'])

        finally:
            shutil.rmtree(temp_dir)

    def test_write_task_start_json(self):
        temp_dir = tempfile.mkdtemp()
        try:
            runner = CellmapsdownloaderRunner(outdir=temp_dir, tsvfile='/fake/my.tsv')
            runner._create_output_directory()
            runner._write_task_start_json()
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
            self.assertEqual(os.path.abspath('/fake/my.tsv'),
                             data['tsvfile'])
            self.assertTrue(data['start_time'] > 0)
            self.assertEqual(temp_dir, data['outdir'])
        finally:
            shutil.rmtree(temp_dir)

    def test_get_download_tuples_from_tsv(self):
        temp_dir = tempfile.mkdtemp()
        try:
            link = 'https://x.y.z/359/'
            f_name_one = '1_A1_1_'
            f_name_two = '1_A1_2_'
            tsvfile = os.path.join(temp_dir, 'foo.tsv')
            with open(tsvfile, 'w') as f:
                f.write('gene_names\tfile_link\tfile_name\n')
                f.write('FOO1\t' + link + f_name_one + '\t' +
                        f_name_one + '\n')
                f.write('FOO2\t' + link + f_name_two + '\t' +
                        f_name_two + '\n')

            runner = CellmapsdownloaderRunner(outdir=temp_dir, tsvfile=tsvfile)
            runner._copy_over_tsvfile()
            dtuples = runner._get_download_tuples_from_tsv()
            self.assertTrue(8, len(dtuples))
            for c in CellmapsdownloaderRunner.COLORS:
                for fname in [f_name_one, f_name_two]:
                    self.assertTrue((link + fname + c + '.jpg',
                                     os.path.join(temp_dir, c,
                                                  fname +
                                                  c + '.jpg')) in dtuples)

        finally:
            shutil.rmtree(temp_dir)
