#! /usr/bin/env python

import os
from multiprocessing import Pool
import re
import shutil
import logging
import logging.config
import requests
import time
from tqdm import tqdm
from cellmaps_utils import logutils
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
    """
    Red color directory name and color name
    in red color files
    """

    BLUE = 'blue'
    """
    Blue color directory name and color name
    in blue color files
    """
    GREEN = 'green'
    """
    Green color directory name and color name
    in green color files
    """

    YELLOW = 'yellow'
    """
    Yellow color directory name and color name
    in yellow color files
    """

    COLORS = [RED, BLUE, GREEN, YELLOW]
    """
    List of colors
    """

    SAMPLES_CSVFILE = 'samples.csv'
    """
    Copy of input csv file that is stored in output
    directory by the :py:meth:`~cellmaps_downloader.runner.CellmapsdownloaderRunner.run`
    """

    UNIQUE_CSVFILE = 'unique.csv'
    """
    Copy of input csv file that is stored in output
    directory by the :py:meth:`~cellmaps_downloader.runner.CellmapsdownloaderRunner.run`
    """

    APMS_EDGELIST_FILE = 'apms_edgelist.tsv'
    APMS_GENE_NODE_ATTR_FILE = 'apms_gene_node_attributes.tsv'
    APMS_GENE_NODE_ERRORS_FILE = 'apms_gene_node_attributes.errors'

    IMAGE_GENE_NODE_ATTR_FILE = 'image_gene_node_attributes.tsv'
    IMAGE_GENE_NODE_ERRORS_FILE = 'image_gene_node_attributes.errors'

    def __init__(self, outdir=None,
                 imgsuffix='.jpg',
                 imagedownloader=MultiProcessImageDownloader(),
                 apmsgen=None,
                 imagegen=None,
                 image_url=None,
                 skip_logging=False,
                 misc_info_dict=None):
        """
        Constructor

        :param outdir: directory where images will be downloaded to
        :type outdir: str
        :param imgsuffix: suffix to append to image file names
        :type imgsuffix: str
        :param imagedownloader: object that will perform image downloads
        :type imagedownloader: :py:class:`~cellmaps_downloader.runner.ImageDownloader`
        :param apmsgen: gene node attribute generator for APMS data
        :type apmsgen: :py:class:`~cellmaps_downloader.gene.APMSGeneNodeAttributeGenerator`
        :param imagegen: gene node attribute generator for IF image data
        :type imagegen: :py:class:`~cellmaps_downloader.gene.ImageGeneNodeAttributeGenerator`
        :param image_url: Base URL for image download
        :type image_url: str
        """
        self._misc_info_dict = misc_info_dict
        self._outdir = outdir
        self._imagedownloader = imagedownloader
        self._imgsuffix = imgsuffix
        self._start_time = int(time.time())
        self._end_time = -1
        self._apmsgen = apmsgen
        self._imagegen = imagegen
        self._image_url = image_url
        if skip_logging is None:
            self._skip_logging = False
        else:
            self._skip_logging = skip_logging

    def _create_output_directory(self):
        """
        Creates output directory if it does not already exist

        :raises CellmapsDownloaderError: If output directory is None
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

    def _get_input_samplesfile(self):
        """
        Gets path to samples file that is copied into output directory specified via
        constructor

        :return: Path to file
        :rtype: str
        """
        return os.path.join(self._outdir,
                            CellmapsdownloaderRunner.SAMPLES_CSVFILE)

    def _get_input_uniquefile(self):
        """

        :return:
        """
        return os.path.join(self._outdir,
                            CellmapsdownloaderRunner.UNIQUE_CSVFILE)

    def _get_color_download_map(self):
        """
        Creates a dict where key is color name and value is directory
        path for files for that color

        ``{'red': '/tmp/foo/red'}``

        :return: map of colors to directory paths
        :rtype: dict
        """
        color_d_map = {}
        for c in CellmapsdownloaderRunner.COLORS:
            color_d_map[c] = os.path.join(self._outdir, c)
        return color_d_map

    def _get_sample_url_and_filename(self, sample=None, color=None):
        """

        :param sample:
        :return:
        """
        file_name = sample['if_plate_id'] + '_' + sample['position'] + '_' + sample['sample'] + '_' + color + self._imgsuffix
        return self._image_url + '/' + re.sub('^HPA0*|^CAB0*', '', sample['antibody']) + '/' + file_name, file_name

    def _get_download_tuples_from_csv(self):
        """
        Gets download list from CSV file for the 4 colors

        :return: list of (image download URL prefix,
                          file path where image should be written)
        :rtype: list
        """
        dtuples = []

        color_d_map = self._get_color_download_map()
        for row in self._imagegen.get_samples_list():
            for c in CellmapsdownloaderRunner.COLORS:
                image_url, file_name = self._get_sample_url_and_filename(sample=row, color=c)
                dtuples.append((image_url,
                                os.path.join(color_d_map[c], file_name)))
        return dtuples

    def _write_task_start_json(self):
        """
        Writes task_start.json file with information about
        what is to be run

        """
        data = {'image_downloader': str(self._imagedownloader),
                'image_suffix': self._imgsuffix}

        if self._misc_info_dict is not None:
            data.update(self._misc_info_dict)

        logutils.write_task_start_json(outdir=self._outdir,
                                       start_time=self._start_time,
                                       version=cellmaps_downloader.__version__,
                                       data=data)

    def _retry_failed_images(self, failed_downloads=None):
        """

        :param failed_downloads:
        :return:
        """
        downloads_to_retry = []
        error_code_map = {}
        for entry in failed_downloads:
            if entry[0] not in error_code_map:
                error_code_map[entry[0]] = 0
            error_code_map[entry[0]] += 1
            downloads_to_retry.append(entry[2])
        logger.debug('Failed download counts by http error code: ' + str(error_code_map))
        return self._imagedownloader.download_images(downloads_to_retry)

    def _download_images(self, max_retry=5):
        """
        Uses downloader specified in constructor to download images noted in
        tsvfile file also specified in constructor
        :raises CellMapsDownloaderError: if image downloader is ``None`` or
                                         if there are failed downloads
        :return: 0 upon success otherwise, failure
        :rtype: int
        """
        if self._imagedownloader is None:
            raise CellMapsDownloaderError('Image downloader is None')

        downloadtuples = self._get_download_tuples_from_csv()

        failed_downloads = self._imagedownloader.download_images(downloadtuples)
        retry_count = 0
        while len(failed_downloads) > 0 and retry_count < max_retry:
            retry_count += 1
            logger.error(str(len(failed_downloads)) +
                         ' images failed to download. Retrying #' + str(retry_count))

            # try one more time with files that failed
            failed_downloads = self._retry_failed_images(failed_downloads=failed_downloads)

        if len(failed_downloads) > 0:
            raise CellMapsDownloaderError('Failed to download: ' +
                                          str(len(failed_downloads)) + ' images')
        return 0

    def get_apms_gene_node_attributes_file(self):
        """
        Gets full path to apms gene node attribute file under output directory
        created when invoking :py:meth:`~cellmaps_downloader.runner.CellmapsdownloaderRunner.run`

        :return: Path to file
        :rtype: str
        """
        return os.path.join(self._outdir,
                            CellmapsdownloaderRunner.APMS_GENE_NODE_ATTR_FILE)

    def get_apms_gene_node_errors_file(self):
        """
        Gets full path to apms gene node attribute errors file under output directory
        created when invoking :py:meth:`~cellmaps_downloader.runner.CellmapsdownloaderRunner.run`

        :return: Path to file
        :rtype: str
        """
        return os.path.join(self._outdir,
                            CellmapsdownloaderRunner.APMS_GENE_NODE_ERRORS_FILE)

    def _write_apms_gene_node_attrs(self, gene_node_attrs=None,
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

    def get_image_gene_node_attributes_file(self):
        """
        Gets full path to image gene node attribute file under output directory
        created when invoking :py:meth:`~cellmaps_downloader.runner.CellmapsdownloaderRunner.run`

        :return: Path to file
        :rtype: str
        """
        return os.path.join(self._outdir,
                            CellmapsdownloaderRunner.IMAGE_GENE_NODE_ATTR_FILE)

    def get_image_gene_node_errors_file(self):
        """
        Gets full path to image gene node attribute errors file under output directory
        created when invoking :py:meth:`~cellmaps_downloader.runner.CellmapsdownloaderRunner.run`

        :return: Path to file
        :rtype: str
        """
        return os.path.join(self._outdir,
                            CellmapsdownloaderRunner.IMAGE_GENE_NODE_ERRORS_FILE)

    def _write_image_gene_node_attrs(self, gene_node_attrs=None,
                                     errors=None):
        """

        :param gene_node_attrs:
        :param errors:
        :return:
        """
        with open(self.get_image_gene_node_attributes_file(), 'w') as f:
            f.write('\t'.join(['name', 'represents', 'ambiguous', 'antibody', 'filename']) +
                    '\n')
            for key in gene_node_attrs:
                f.write('\t'.join([gene_node_attrs[key]['name'],
                                   gene_node_attrs[key]['represents'],
                                   gene_node_attrs[key]['ambiguous'],
                                   str(gene_node_attrs[key]['antibody']),
                                   str(gene_node_attrs[key]['filename'])]))
                f.write('\n')
        if errors is not None:
            with open(self.get_image_gene_node_errors_file(), 'w') as f:
                for e in errors:
                    f.write(str(e) + '\n')

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
            if self._skip_logging is False:
                logutils.setup_filelogger(outdir=self._outdir,
                                          handlerprefix='cellmaps_downloader')
                self._write_task_start_json()

            # obtain apms data
            if self._apmsgen is not None:
                gene_node_attrs, errors = self._apmsgen.get_gene_node_attributes()

                # write apms attribute data
                self._write_apms_gene_node_attrs(gene_node_attrs, errors)

                # write apms network
                self._write_apms_network(edgelist=self._apmsgen.get_apms_edgelist(),
                                         gene_node_attrs=gene_node_attrs)

            # write image attribute data
            if self._imagegen is not None:
                self._imagegen.write_samples_as_csvfile(outfile=self._get_input_samplesfile())
                self._imagegen.write_unique_list_as_csvfile(outfile=self._get_input_uniquefile())
                image_gene_node_attrs, errors = self._imagegen.get_gene_node_attributes()

                # write image attribute data
                self._write_image_gene_node_attrs(image_gene_node_attrs, errors)

            exitcode = self._download_images()
            # todo need to validate downloaded image data

            return exitcode
        finally:
            self._end_time = int(time.time())
            if self._skip_logging is False:
                # write a task finish file
                logutils.write_task_finish_json(outdir=self._outdir,
                                                start_time=self._start_time,
                                                end_time=self._end_time,
                                                status=exitcode)
