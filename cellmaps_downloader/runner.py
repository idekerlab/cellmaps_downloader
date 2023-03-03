#! /usr/bin/env python

import os
from multiprocessing import Pool
import csv
import shutil
import logging
import logging.config
import requests
import json
import time
import platform
from tqdm import tqdm
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
        logger.debug('Poolsize for image downloader set to: ' +
                     str(self._poolsize))
        with Pool(processes=self._poolsize) as pool:
            num_to_download = len(download_list)
            logger.info(str(num_to_download) + ' images to download')
            t = tqdm(total=num_to_download, desc='Download',
                     unit='images')
            for i in pool.imap_unordered(dfunc,
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
        self._start_time = int(time.time())
        self._end_time = -1

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

        task = {'start_time': self._start_time,
                'version': str(cellmaps_downloader.__version__),
                'pid': str(os.getpid()),
                'tsvfile': tsvfile,
                'outdir': self._outdir,
                'image_downloader': str(self._imagedownloader),
                'image_suffix': self._imgsuffix,
                'login': str(os.getlogin()),
                'cwd': str(os.getcwd()),
                'platform': str(platform.platform()),
                'python': str(platform.python_version()),
                'system': str(platform.system()),
                'uname': str(platform.uname())
                }
        with open(os.path.join(self._outdir,
                               'task_' + str(self._start_time) +
                               '_start.json'), 'w') as f:
            json.dump(task, f, indent=2)

    def _write_task_finish_json(self, status=None):
        """
        Writes task_finish.json file with information about
        what is to be run

        :return:
        """
        if self._outdir is None or not os.path.isdir(self._outdir):
            logger.error('Output directory is not set or not a '
                         'directory, cannot write'
                         'task finish json file')
            return
        if self._end_time == -1:
            self._end_time = int(time.time())
        task = {'end_time': self._end_time,
                'elapsed_time': int(self._end_time - self._start_time),
                'status': str(status)
                }
        with open(os.path.join(self._outdir,
                               'task_' + str(self._start_time) +
                               '_finish.json'), 'w') as f:
            json.dump(task, f, indent=2)

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
            raise CellMapsDownloaderError('Not implemented yet!!!')
        return 0

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
            self._setup_filelogger()
            self._write_task_start_json()
            self._copy_over_tsvfile()
            return self._download_images()
        finally:
            # write a task finish file no matter what
            self._write_task_finish_json(status=exitcode)
