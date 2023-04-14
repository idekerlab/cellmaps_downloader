#! /usr/bin/env python

import argparse
import sys
import logging
import logging.config

from cellmaps_utils import logutils
from cellmaps_utils import constants
import cellmaps_downloader
from cellmaps_downloader.runner import MultiProcessImageDownloader
from cellmaps_downloader.runner import CellmapsdownloaderRunner
from cellmaps_downloader.gene import APMSGeneNodeAttributeGenerator
from cellmaps_downloader.gene import ImageGeneNodeAttributeGenerator

logger = logging.getLogger(__name__)


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
                                     formatter_class=constants.ArgParseFormatter)
    parser.add_argument('outdir',
                        help='Directory to write results to')
    parser.add_argument('--tsv',
                        help='TSV file with list of IF images to download '
                             'in format of gene_names\tfile_link\tfile_name\n'
                             'GOLGA5\thttps://images.proteinatlas.org/992/1_A1_1_\t1_A1_1_')
    parser.add_argument('--samples',
                        help='CSV file with information about samples in '
                             'format of:\n'
                             'filename,if_plate_id,position,sample,status,'
                             'locations,'
                             'antibody,ensembl_ids,gene_names\n'
                             '/archive/1/1_A1_1_,1,A1,1,35,Golgi apparatus,'
                             'HPA000992,ENSG00000066455,GOLGA5')
    parser.add_argument('--antibodies',
                        help='CSV file with information about antibodies '
                             'in format of:\n'
                             'antibody,ensembl_ids,gene_names,atlas_name,'
                             'locations,n_location\n'
                             'HPA040086,ENSG00000094914,AAAS,U-2 OS,'
                             'Nuclear membrane,1')
    parser.add_argument('--apms_edgelist',
                        help='APMS edgelist TSV file in format of:\n'
                             'GeneID1\tSymbol1\tGeneID2\tSymbol2\n'
                             '10159\tATP6AP2\t2\tA2M')
    parser.add_argument('--apms_baitlist',
                        help='APMS baitlist TSV file in format of:\n'
                             'GeneSymbol\tGeneID\t# Interactors\n'
                             '"ADA"\t"100"\t1.')
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
    parser.add_argument('--skip_logging', action='store_true',
                        help='If set, output.log, error.log and '
                             'task_#_start/finish.json '
                             'files will not be created')
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
        logutils.setup_cmd_logging(theargs)
        apmsgen = APMSGeneNodeAttributeGenerator(apms_edgelist=APMSGeneNodeAttributeGenerator.get_apms_edgelist_from_tsvfile(theargs.apms_edgelist),
                                                 apms_baitlist=APMSGeneNodeAttributeGenerator.get_apms_baitlist_from_tsvfile(theargs.apms_baitlist))
        imagegen = ImageGeneNodeAttributeGenerator(antibody_list=ImageGeneNodeAttributeGenerator.get_image_antibodies_from_csvfile(theargs.antibodies))
        dloader = MultiProcessImageDownloader(poolsize=theargs.poolsize,
                                              skip_existing=theargs.skip_existing)
        return CellmapsdownloaderRunner(outdir=theargs.outdir,
                                        tsvfile=theargs.tsv,
                                        imagedownloader=dloader,
                                        imgsuffix=theargs.imgsuffix,
                                        apmsgen=apmsgen,
                                        imagegen=imagegen,
                                        skip_logging=theargs.skip_logging).run()
    except Exception as e:
        logger.exception('Caught exception: ' + str(e))
        return 2
    finally:
        logging.shutdown()


if __name__ == '__main__':  # pragma: no cover
    sys.exit(main(sys.argv))
