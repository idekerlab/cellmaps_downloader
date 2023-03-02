#! /usr/bin/env python

import os
from multiprocessing import Pool
import csv
import logging
import logging.config
import requests
from tqdm import tqdm
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

    def __init__(self, poolsize=1, skip_existing=False):
        """
        Constructor
        """
        super().__init__()
        self._poolsize = poolsize
        self._skip_existing = skip_existing

    def download_images(self, download_list=None):
        """
        Downloads images returning a list of failed downloads

        :param download_list:
        :return: of tuples (`http status code`, `text of error`, (`link`, `destfile`))
        :rtype: list
        """
        dfunc = download_file
        if self._skip_existing is True:
            dfunc = download_file_skip_existing

        failed_downloads = []
        with Pool(processes=self._poolsize) as pool:
            t = tqdm(total=len(download_list), desc='Download',
                     unit='images')
            for i in pool.imap_unordered(dfunc,
                                         download_list):
                t.update()
                if i is not None:
                    if logger.isEnabledFor(logging.INFO):
                        logger.info('Failed download: ' + str(i))
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

    def __init__(self, outdir=None, tsvfile=None,
                 imgsuffix='.jpg',
                 imagedownloader=MultiProcessImageDownloader()):
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
        logger.debug('In constructor')

    def _setup_filelogger(self):
        """
        Sets up a logger to write all debug logs
        to output directory/output.log
        and all error level log messages and higher
        to output directory/error.log

        :return: None
        """
        logging.config.dictConfig({'version': 1,
                                   'disable_existing_loggers': False,
                                   'loggers': {
                                     '': {
                                         'level': 'NOTSET',
                                         'handlers': ['cellmapsdownloader_file_handler',
                                                      'cellmapsdownloader_error_file_handler']
                                     }
                                   },
                                   'handlers': {
                                       'cellmapsdownloader_file_handler': {
                                           'level': 'DEBUG',
                                           'class': 'logging.FileHandler',
                                           'formatter': 'cellmaps_formatter',
                                           'filename': os.path.join(self._outdir, 'output.log'),
                                           'mode': 'a'
                                       },
                                       'cellmapsdownloader_error_file_handler': {
                                           'level': 'ERROR',
                                           'class': 'logging.FileHandler',
                                           'formatter': 'cellmaps_formatter',
                                           'filename': os.path.join(self._outdir, 'error.log'),
                                           'mode': 'a'
                                       }
                                   },
                                   'formatters': {
                                       'cellmaps_formatter': {
                                           'format': '%(asctime)-15s %(levelname)s %(relativeCreated)dms %(filename)s::%(funcName)s():%(lineno)d %(message)s'
                                       }
                                   }
                                   })

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

        with open(self._tsvfile, 'r') as f:
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

    def run(self):
        """
        Runs cellmaps_downloader


        :return:
        """
        self._create_output_directory()
        self._setup_filelogger()

        # todo, copy over tsv file

        # todo need a JSON file for start
        # what should we put in this file?
        # start time,
        # version of this software
        # all input flags
        # full path to output directory?
        # host running command?
        # user running command?

        downloadtuples = self._get_download_tuples_from_tsv()

        if self._imagedownloader is None:
            raise CellMapsDownloaderError('Image downloader is None')

        failed_downloads = self._imagedownloader.download_images(downloadtuples)
        for x in failed_downloads:
            logger.error('Download failed: ' + str(x))
        if len(failed_downloads) > 0:
            # try one more time with files that failed

            return 1
        return 0
        # todo need a JSON file for completion
        # what should we put in this file?
        # end time
        # duration
        # success status
        # summary of failures?
