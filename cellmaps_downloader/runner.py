#! /usr/bin/env python

import os
import logging
import requests


logger = logging.getLogger(__name__)


class CellmapsdownloaderRunner(object):
    """
    Class to run algorithm
    """
    def __init__(self, exitcode):
        """
        Constructor

        :param exitcode: value to return via :py:meth:`.CellmapsdownloaderRunner.run` method
        :type int:
        """
        self._exitcode = exitcode
        logger.debug('In constructor')

    def _download_file(self, download_url, destfile):
        """
        Downloads file pointed to by 'download_url' to
        'destfile'

        :param theurl: link to download
        :type theurl: str
        :param destfile: path to file to write contents of 'theurl' to
        :type destfile: str
        :raises Exception: from requests library if there is an error or non 200 status
        :return: None
        """
        logger.info('Downloading ' + download_url + ' to ' + destfile)
        with requests.get(download_url, stream=True) as r:
            r.raise_for_status()
            with open(destfile, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:  # filter out keep-alive new chunks
                        f.write(chunk)

    def run(self):
        """
        Runs cellmaps_downloader


        :return:
        """
        logger.debug('In run method')
        return self._exitcode
