#! /usr/bin/env python

import os
from multiprocessing import Pool
import csv
import shutil
import logging
import logging.config
import requests
import time
from tqdm import tqdm
from cellmaps_utils import cellmaps_io
import cellmaps_downloader
from cellmaps_downloader.exceptions import CellMapsDownloaderError

logger = logging.getLogger(__name__)


def download_file_skip_existing(downloadtuple):
    """
    Downloads file in **downloadtuple** unless the file already exists
    with a size greater then 0 bytes, in which case function
    just returns

    :param downloadtuple: (download link, dest file path)
    :type downloadtuple: tuple
    :return: None upon success otherwise:
             (requests status code, text from request, downloadtuple)
    :rtype: tuple
    """
    if os.path.isfile(downloadtuple[1]) and os.path.getsize(downloadtuple[1]) > 0:
        return None
    return download_file(downloadtuple)


def download_file(downloadtuple):
    """
    Downloads file pointed to by 'download_url' to
    'destfile'

    :param downloadtuple: (download link, dest file path)
    :type downloadtuple: tuple
    :raises Exception: from requests library if there is an error or non 200 status
    :return: None upon success otherwise:
             (requests status code, text from request, downloadtuple)
    :rtype: tuple
    """
    logger.debug('Downloading ' + downloadtuple[0] + ' to ' + downloadtuple[1])
    with requests.get(downloadtuple[0], stream=True) as r:
        if r.status_code != 200:
            return r.status_code, r.text, downloadtuple
        with open(downloadtuple[1], 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)
    return None


class ImageDownloader(object):
    """

    """
    def __init__(self):
        """

        """
        pass

    def download_images(self, download_list=None):
        """
        Subclasses should implement
        :param download_list: list of tuples where first element is
                              full URL of image to download and 2nd
                              element is destination path
        :type download_list: list
        :return: 
        """
        raise CellMapsDownloaderError('Subclasses should implement this')


class MultiProcessImageDownloader(ImageDownloader):
    """
    Uses multiprocess package to download images in parallel
    """

    def __init__(self, poolsize=1, skip_existing=False,
                 override_dfunc=None):
        """
        Constructor
        """
        super().__init__()
        self._poolsize = poolsize
        if override_dfunc is not None:
            self._dfunc = override_dfunc
        else:
            self._dfunc = download_file
            if skip_existing is True:
                self._dfunc = download_file_skip_existing

    def download_images(self, download_list=None):
        """
        Downloads images returning a list of failed downloads

        :param download_list:
        :return: of tuples (`http status code`, `text of error`, (`link`, `destfile`))
        :rtype: list
        """
        failed_downloads = []
        logger.debug('Poolsize for image downloader set to: ' +
                     str(self._poolsize))
        with Pool(processes=self._poolsize) as pool:
            num_to_download = len(download_list)
            logger.info(str(num_to_download) + ' images to download')
            t = tqdm(total=num_to_download, desc='Download',
                     unit='images')
            for i in pool.imap_unordered(self._dfunc,
                                         download_list):
                t.update()
                if i is not None:
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug('Failed download: ' + str(i))
                    failed_downloads.append(i)
        return failed_downloads


class CellmapsdownloaderRunner(object):
    """
    Class to run algorithm
    """

    RED = 'red'
    BLUE = 'blue'
    GREEN = 'green'
    YELLOW = 'yellow'

    COLORS = [RED, BLUE, GREEN, YELLOW]

    TSVFILE = 'immunofluorescent.tsv'
    APMS_EDGELIST_FILE = 'apms_edgelist.tsv'
    APMS_GENE_NODE_ATTR_FILE = 'apms_gene_node_attributes.tsv'
    APMS_GENE_NODE_ERRORS_FILE = 'apms_gene_node_attributes.errors'

    def __init__(self, outdir=None, tsvfile=None,
                 imgsuffix='.jpg',
                 imagedownloader=MultiProcessImageDownloader(),
                 apmsgen=None,
                 imagegen=None):
        """
        Constructor

        :param outdir: directory where images will be downloaded to
        :type outdir: str
        :param tsvfile: image TSV file
        :type tsvfile: str
        :param imagedownloader: object that will perform image downloads
        :type imagedownloader: :py:class:`~cellmaps_downloader.runner.ImageDownloader`
        """
        self._outdir = outdir
        self._tsvfile = tsvfile
        self._imagedownloader = imagedownloader
        self._imgsuffix = imgsuffix
        self._start_time = int(time.time())
        self._end_time = -1
        self._apmsgen = apmsgen
        self._imagegen = imagegen

    def _create_output_directory(self):
        """

        :raises CellmapsDownloaderError: If output directory is None
        :return: None
        """
        if self._outdir is None:
            raise CellMapsDownloaderError('Output directory is None')

        for cur_color in CellmapsdownloaderRunner.COLORS:
            cdir = os.path.join(self._outdir, cur_color)
            if not os.path.isdir(cdir):
                logger.debug('Creating directory: ' + cdir)
                os.makedirs(cdir,
                            mode=0o755)
            else:
                logger.debug(cdir + ' already exists')

    def _get_input_tsvfile(self):
        """

        :return:
        """
        return os.path.join(self._outdir,
                            CellmapsdownloaderRunner.TSVFILE)

    def _copy_over_tsvfile(self):
        """
        Copies tsv file into output directory for record keeping purposes

        :return:
        """
        logger.debug('Copying ' + self._tsvfile + ' to ' +
                     self._get_input_tsvfile())
        shutil.copy(self._tsvfile, self._get_input_tsvfile())

    def _get_color_download_map(self):
        """

        :return:
        """
        color_d_map = {}
        for c in CellmapsdownloaderRunner.COLORS:
            color_d_map[c] = os.path.join(self._outdir, c)
        return color_d_map

    def _get_download_tuples_from_tsv(self):
        """
        Gets download list from TSV file for the 4 colors
        :return:
        """
        dtuples = []

        color_d_map = self._get_color_download_map()

        with open(self._get_input_tsvfile(), 'r') as f:
            reader = csv.reader(f, delimiter='\t')
            is_first_row = True
            for row in reader:
                if is_first_row:
                    is_first_row = False
                    continue
                for c in CellmapsdownloaderRunner.COLORS:
                    dtuples.append((row[1] + c + self._imgsuffix,
                                   os.path.join(color_d_map[c], row[2] +
                                                c + self._imgsuffix)))
        return dtuples

    def _write_task_start_json(self):
        """
        Writes task_start.json file with information about
        what is to be run

        :return:
        """
        if self._tsvfile is None:
            tsvfile = 'Not set'
        else:
            tsvfile = os.path.abspath(self._tsvfile)

        data = {'tsvfile': tsvfile,
                'image_downloader': str(self._imagedownloader),
                'image_suffix': self._imgsuffix}

        cellmaps_io.write_task_start_json(outdir=self._outdir,
                                          start_time=self._start_time,
                                          version=cellmaps_downloader.__version__,
                                          data=data)

    def _download_images(self):
        """

        :return:
        """
        if self._imagedownloader is None:
            raise CellMapsDownloaderError('Image downloader is None')

        downloadtuples = self._get_download_tuples_from_tsv()

        failed_downloads = self._imagedownloader.download_images(downloadtuples)

        if len(failed_downloads) > 0:
            logger.error(str(len(failed_downloads)) +
                         ' images failed to download. Retrying')
            # try one more time with files that failed
            # todo need to implement a retry
            raise CellMapsDownloaderError('Not implemented yet!!!')
        return 0

    def get_apms_gene_node_attributes_file(self):
        """

        :return:
        """
        return os.path.join(self._outdir,
                            CellmapsdownloaderRunner.APMS_GENE_NODE_ATTR_FILE)

    def get_apms_gene_node_errors_file(self):
        """

        :return:
        """
        return os.path.join(self._outdir,
                            CellmapsdownloaderRunner.APMS_GENE_NODE_ERRORS_FILE)

    def _write_gene_node_attrs(self, gene_node_attrs=None,
                               errors=None):
        """

        :param gene_node_attrs:
        :param errors:
        :return:
        """
        with open(self.get_apms_gene_node_attributes_file(), 'w') as f:
            f.write('\t'.join(['name', 'represents', 'ambiguous', 'bait']) +
                    '\n')
            for key in gene_node_attrs:
                f.write('\t'.join([gene_node_attrs[key]['name'],
                                   gene_node_attrs[key]['represents'],
                                   gene_node_attrs[key]['ambiguous'],
                                   str(gene_node_attrs[key]['bait'])]))
                f.write('\n')
        if errors is not None:
            with open(self.get_apms_gene_node_errors_file(), 'w') as f:
                for e in errors:
                    f.write(str(e) + '\n')

    def get_apms_edgelist_file(self):
        """

        :return:
        """
        return os.path.join(self._outdir,
                            CellmapsdownloaderRunner.APMS_EDGELIST_FILE)

    def _write_apms_network(self, edgelist=None,
                            gene_node_attrs=None):
        """

        :param edgelist:
        :param gene_node_attrs:
        :return:
        """
        with open(self.get_apms_edgelist_file(), 'w') as f:
            f.write('geneA\tgeneB\n')
            for edge in edgelist:
                if edge['GeneID1'] not in gene_node_attrs:
                    logger.error('Skipping ' + str(edge['GeneID1'] + ' cause it lacks a symbol'))
                    continue
                if edge['GeneID2'] not in gene_node_attrs:
                    logger.error('Skipping ' + str(edge['GeneID2'] + ' cause it lacks a symbol'))
                    continue

                genea = gene_node_attrs[edge['GeneID1']]['name']
                geneb = gene_node_attrs[edge['GeneID2']]['name']
                if genea is None or geneb is None:
                    logger.error('Skipping edge cause no symbol is found: ' + str(edge))
                    continue
                if len(genea) == 0 or len(geneb) == 0:
                    logger.error('Skipping edge cause no symbol is found: ' + str(edge))
                    continue
                f.write(genea + '\t' + geneb + '\n')

    def run(self):
        """
        Downloads images to output directory specified in constructor
        using tsvfile for list of images to download

        :raises CellMapsDownloaderError: If there is an error
        :return: 0 upon success, otherwise failure
        """
        try:
            exitcode = 99

            self._create_output_directory()
            cellmaps_io.setup_filelogger(outdir=self._outdir,
                                         handlerprefix='cellmaps_downloader')
            self._write_task_start_json()
            self._copy_over_tsvfile()

            # obtain apms data
            if self._apmsgen is not None:
                gene_node_attrs, errors = self._apmsgen.get_gene_node_attributes()

                # write apms attribute data
                self._write_gene_node_attrs(gene_node_attrs, errors)

                # write apms network
                self._write_apms_network(edgelist=self._apmsgen.get_apms_edgelist(),
                                         gene_node_attrs=gene_node_attrs)


            # write image attribute data

            exitcode = self._download_images()

            # todo need to validate downloaded image data
            return exitcode
        finally:
            self._end_time = int(time.time())
            # write a task finish file no matter what
            cellmaps_io.write_task_finish_json(outdir=self._outdir,
                                               start_time=self._start_time,
                                               end_time=self._end_time,
                                               status=exitcode)
