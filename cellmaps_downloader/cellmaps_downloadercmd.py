#! /usr/bin/env python

import argparse
import sys
import logging
import logging.config

import cellmaps_downloader
from cellmaps_downloader.runner import MultiProcessImageDownloader
from cellmaps_downloader.runner import CellmapsdownloaderRunner

logger = logging.getLogger(__name__)


LOG_FORMAT = "%(asctime)-15s %(levelname)s %(relativeCreated)dms " \
             "%(filename)s::%(funcName)s():%(lineno)d %(message)s"


class Formatter(argparse.ArgumentDefaultsHelpFormatter,
                argparse.RawDescriptionHelpFormatter):
    """
    Combine two Formatters to get help and default values
    displayed when showing help

    """
    pass


def _parse_arguments(desc, args):
    """
    Parses command line arguments

    :param desc: description to display on command line
    :type desc: str
    :param args: command line arguments usually :py:func:`sys.argv[1:]`
    :type args: list
    :return: arguments parsed by :py:mod:`argparse`
    :rtype: :py:class:`argparse.Namespace`
    """
    parser = argparse.ArgumentParser(description=desc,
                                     formatter_class=Formatter)
    parser.add_argument('outdir',
                        help='Directory to write results to')
    parser.add_argument('--tsv',
                        help='TSV file with list of IF images to download '
                             'in format of gene_names\tfile_link\tfile_name\n'
                             'GOLGA5\thttps://images.proteinatlas.org/992/1_A1_1_\t1_A1_1_')
    parser.add_argument('--poolsize', type=int,
                        default=4,
                        help='If using multiprocessing image downloader, '
                             'this sets number of current downloads to run. '
                             'Note: Going above the default overloads the server')
    parser.add_argument('--imgsuffix', default='.jpg',
                        help='Suffix for images to download')
    parser.add_argument('--skip_existing', action='store_true',
                        help='If set, skips download if image already exists and '
                             'has size greater then 0 bytes')
    parser.add_argument('--logconf', default=None,
                        help='Path to python logging configuration file in '
                             'this format: https://docs.python.org/3/library/'
                             'logging.config.html#logging-config-fileformat '
                             'Setting this overrides -v parameter which uses '
                             ' default logger. (default None)')
    parser.add_argument('--exitcode', help='Exit code this command will return',
                        default=0, type=int)
    parser.add_argument('--verbose', '-v', action='count', default=0,
                        help='Increases verbosity of logger to standard '
                             'error for log messages in this module. Messages are '
                             'output at these python logging levels '
                             '-v = ERROR, -vv = WARNING, -vvv = INFO, '
                             '-vvvv = DEBUG, -vvvvv = NOTSET (default no '
                             'logging)')
    parser.add_argument('--version', action='version',
                        version=('%(prog)s ' +
                                 cellmaps_downloader.__version__))

    return parser.parse_args(args)


def _setup_logging(args):
    """
    Sets up logging based on parsed command line arguments.
    If args.logconf is set use that configuration otherwise look
    at args.verbose and set logging for this module

    :param args: parsed command line arguments from argparse
    :raises AttributeError: If args is None or args.logconf is None
    :return: None
    """

    if args.logconf is None:
        level = (50 - (10 * args.verbose))
        logging.basicConfig(format=LOG_FORMAT,
                            level=level)
        logger.setLevel(level)
        logger.propagate = True
        return

    # logconf was set use that file
    logging.config.fileConfig(args.logconf,
                              disable_existing_loggers=False)


def main(args):
    """
    Main entry point for program

    :param args: arguments passed to command line usually :py:func:`sys.argv[1:]`
    :type args: list

    :return: return value of :py:meth:`cellmaps_downloader.runner.CellmapsdownloaderRunner.run`
             or ``2`` if an exception is raised
    :rtype: int
    """
    desc = """
    Version {version}

    Downloads immunofluorescent labeled images from the Human Protein Atlas
    (https://www.proteinatlas.org/)
    
    To use pass in a TSV file containing links to the images to download
    from HPA
    
    Format of TSV file:
    
    gene_names  file_link   file_name
    XXXX    https://images.proteinatlas.org/###/1_A1_1_ 1_A1_1_
    
    Where XXXX is gene name and ### is a id number and 1_A1_1_ is an
    example file name
    
    The downloaded images are stored under the output directory
    specified on the command line in color specific directories
    (red, blue, green, yellow) with name format of: 
    <NAME>_color.jpg 
    
    Example: 1_A1_1_blue.jpg

    """.format(version=cellmaps_downloader.__version__)
    theargs = _parse_arguments(desc, args[1:])
    theargs.program = args[0]
    theargs.version = cellmaps_downloader.__version__

    try:
        _setup_logging(theargs)
        dloader = MultiProcessImageDownloader(poolsize=theargs.poolsize,
                                              skip_existing=theargs.skip_existing)
        return CellmapsdownloaderRunner(outdir=theargs.outdir,
                                        tsvfile=theargs.tsv,
                                        imagedownloader=dloader,
                                        imgsuffix=theargs.imgsuffix).run()
    except Exception as e:
        logger.exception('Caught exception: ' + str(e))
        return 2
    finally:
        logging.shutdown()


if __name__ == '__main__':  # pragma: no cover
    sys.exit(main(sys.argv))
