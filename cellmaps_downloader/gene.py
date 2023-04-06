
import re
import csv
import mygene
import logging

logger = logging.getLogger(__name__)


class GeneQuery(object):
    """
    Gets information about genes from mygene
    """
    def __init__(self, mygeneinfo=mygene.MyGeneInfo()):
        """
        Constructor
        """
        self._mg = mygeneinfo

    def querymany(self, queries, species=None,
                  scopes=None,
                  fields=None):

        """
        Simple wrapper that calls MyGene querymany
        returning the results

        :param queries: list of gene ids/symbols to query
        :type queries: list
        :param species:
        :type species: str
        :param scopes:
        :type scopes: str
        :param fields:
        :type fields: list
        :return: dict from MyGene usually in format of
        :rtype: list
        """
        mygene_out = self._mg.querymany(queries,
                                        scopes=scopes,
                                        fields=fields,
                                        species=species)
        return mygene_out

    def get_symbols_for_genes(self, genelist=None,
                              scopes='_id'):
        """
        Queries for genes via GeneQuery() object passed in via
        constructor

        :param genelist: genes to query for valid symbols and ensembl ids
        :type genelist: list
        :param scopes: field to query on _id for gene id, ensemble.gene
                       for ENSEMBLE IDs
        :type scopes: str
        :return: result from mygene which is a list of dict objects where
                 each dict is of format:

                 .. code-block::

                     { 'query': 'ID',
                       '_id': 'ID', '_score': #.##,
                       'ensembl': { 'gene': 'ENSEMBLEID' },
                       'symbol': 'GENESYMBOL' }
        :rtype: list
        """
        res = self.querymany(genelist,
                             species='human',
                             scopes=scopes,
                             fields=['ensembl.gene', 'symbol'])
        return res


class GeneNodeAttributeGenerator(object):
    """
    Base class for GeneNodeAttribute Generator
    """
    def __init__(self):
        """
        Constructor
        """
        pass

    @staticmethod
    def add_geneids_to_set(gene_set=None,
                           ambiguous_gene_dict=None,
                           geneid=None):
        """
        Examines **geneid** passed in and if a comma exists
        in value split by comma and assume multiple genes.
        Adds those genes into **gene_set** and add entry
        to **ambiguous_gene_dict** with key set to each gene
        name and value set to original **geneid** value

        :param gene_set: unique set of genes
        :type gene_set: set
        :param geneid: name of gene or comma delimited string of genes
        :type geneid: str
        :return: genes found in **geneid** or None if **gene_set**
                 or **geneid** is ``None``
        :rtype: list
        """
        if gene_set is None:
            return None
        if geneid is None:
            return None

        split_str = re.split('\W*,\W*', geneid)
        gene_set.update(split_str)
        if ambiguous_gene_dict is not None:
            if len(split_str) > 1:
                for entry in split_str:
                    ambiguous_gene_dict[entry] = geneid
        return split_str

    def get_gene_node_attributes(self):
        """
        Should be implemented by subclasses

        :raises NotImplementedError: Always
        """
        raise NotImplementedError('Subclasses should implement')


class ImageGeneNodeAttributeGenerator(GeneNodeAttributeGenerator):
    """
    Creates Image Gene Node Attributes table
    """

    def __init__(self, antibody_list=None,
                 genequery=GeneQuery()):
        """
        Constructor
        """
        super().__init__()
        self._antibody_list = antibody_list
        self._genequery = genequery

    def get_antibody_list(self):
        """
        Gets antibody_list passed in via the constructor

        :return:
        """
        return self._antibody_list

    @staticmethod
    def get_image_antibodies_from_csvfile(csvfile=None):
        """

        :param csvfile:
        :return:
        """
        antibodies = []
        with open(csvfile, 'r') as f:
            reader = csv.DictReader(f, delimiter=',')
            for row in reader:
                antibodies.append({'antibody': row['antibody'],
                                   'ensembl_ids': row['ensembl_ids'],
                                   'gene_names': row['gene_names'],
                                   'atlas_name': row['atlas_name'],
                                   'locations': row['locations'],
                                   'n_location': row['n_location']})
        return antibodies

    def _get_unique_genelist_from_antibodylist(self):
        """
        Gets unique list of ensemble IDS from antibody list along with a
        dict for ambiguous genes which have multiple names.
        For the ambiguous genes the dict is of format:

        ``{'ENSEMBLEID': 'AMBIGUOUS ID aka x,y,z'}``

        :return: (list of genes, dict of ambiguous genes)
        :rtype: list
        """
        gene_set = set()
        ambiguous_gene_dict = {}

        for row in self._antibody_list:
            GeneNodeAttributeGenerator.add_geneids_to_set(gene_set=gene_set,
                                                          ambiguous_gene_dict=ambiguous_gene_dict,
                                                          geneid=row['ensembl_ids'])
        return list(gene_set), ambiguous_gene_dict

    def get_gene_node_attributes(self):
        """
        TODO: need to implement this

        :return:
        """
        genelist, ambiguous_gene_dict = self._get_unique_genelist_from_antibodylist()
        query_res = self._genequery.get_symbols_for_genes(genelist=genelist,
                                                          scopes='ensembl.gene')
        errors = []
        gene_node_attrs = {}
        for x in query_res:
            if 'symbol' not in x:
                errors.append('Skipping ' + str(x) +
                              ' no symbol in query result: ' + str(x))
                logger.error(errors[-1])
                continue
            ensemblstr = 'ensembl:'
            if 'ensembl' not in x:
                errors.append('Skipping ' + str(x) +
                              ' no ensembl in query result: ' + str(x))
                logger.error(errors[-1])
                continue
            if len(x['ensembl']) > 1:
                ensemblstr += ';'.join([g['gene'] for g in x['ensembl']])
            else:
                ensemblstr += x['ensembl']['gene']

            ambiguous_str = ''
            if x['symbol'] in ambiguous_gene_dict:
                ambiguous_str = ambiguous_gene_dict[x['symbol']]

            gene_node_attrs[x['query']] = {'name': x['symbol'],
                                           'represents': ensemblstr,
                                           'ambiguous': ambiguous_str,
                                           'antibody': 'TODO',
                                           'filename': 'TODO'}

        return gene_node_attrs, errors


class APMSGeneNodeAttributeGenerator(GeneNodeAttributeGenerator):
    """
    Creates APMS Gene Node Attributes table
    """

    def __init__(self, apms_edgelist=None, apms_baitlist=None,
                 genequery=GeneQuery()):
        """
        Constructor

        :param apms_edgelist: list of dict elements where each
                              dict is of format:

                              .. code-block::

                                  {'GeneID1': VAL,
                                   'Symbol1': VAL,
                                   'GeneID2': VAL,
                                   'Symbol2': VAL}
        :type apms_edgelist: list
        :param apms_baitlist: list of dict elements where each dict is of
                              format:

                              .. code-block::

                                  { 'GeneSymbol': VAL,
                                    'GeneID': VAL,
                                    'NumIteractors': VAL }
        :type apms_baitlist: list
        :param genequery:
        """
        super().__init__()
        self._apms_edgelist = apms_edgelist
        self._apms_baitlist = apms_baitlist
        self._genequery = genequery

    @staticmethod
    def get_apms_edgelist_from_tsvfile(tsvfile=None):
        """
        Generates list of dicts by parsing TSV file specified
        by **tsvfile** with the
        format header column and corresponding values:

        .. code-block::

            GeneID1\tSymbol1\tGeneID2\tSymbol2

        :param tsvfile: Path to TSV file with above format
        :type tsvfile: str
        :return: list of dicts, with each dict of format:

                 .. code-block::

                      {'GeneID1': VAL,
                       'Symbol1': VAL,
                       'GeneID2': VAL,
                       'Symbol2': VAL}
        :rtype: list
        """
        edgelist = []
        with open(tsvfile, 'r') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                edgelist.append({'GeneID1': row['GeneID1'],
                                 'Symbol1': row['Symbol1'],
                                 'GeneID2': row['GeneID2'],
                                 'Symbol2': row['Symbol2']})
        return edgelist

    @staticmethod
    def get_apms_baitlist_from_tsvfile(tsvfile=None):
        """
        Generates list of dicts by parsing TSV file specified
        by **tsvfile** with the
        format header column and corresponding values:

        .. code-block::

            GeneSymbol\tGeneID\t# Interactors

        :param tsvfile: Path to TSV file with above format
        :type tsvfile: str
        :return: list of dicts, with each dict of format:

                 .. code-block::

                      { 'GeneSymbol': VAL,
                        'GeneID': VAL,
                        'NumIteractors': VAL }
        :rtype: list
        """
        edgelist = []
        with open(tsvfile, 'r') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                edgelist.append({'GeneSymbol': row['GeneSymbol'],
                                 'GeneID': row['GeneID'],
                                 'NumInteractors': row['# Interactors']})
        return edgelist

    def get_apms_edgelist(self):
        """
        Gets apms edgelist passed in via constructor

        :return:
        :rtype: list
        """
        return self._apms_edgelist

    def _get_unique_genelist_from_edgelist(self):
        """
        Gets unique list of genes from edge list along with a
        dict for ambiguous genes which have multiple names.
        For the ambiguous genes the dict is of format:

        ``{'GENEID': 'AMBIGUOUS ID aka x,y,z'}``

        :return: (list of genes, dict of ambiguous genes)
        :rtype: list
        """
        gene_set = set()
        ambiguous_gene_dict = {}

        for row in self._apms_edgelist:
            GeneNodeAttributeGenerator.add_geneids_to_set(gene_set=gene_set,
                                                          ambiguous_gene_dict=ambiguous_gene_dict,
                                                          geneid=row['GeneID1'])
            GeneNodeAttributeGenerator.add_geneids_to_set(gene_set=gene_set,
                                                          ambiguous_gene_dict=ambiguous_gene_dict,
                                                          geneid=row['GeneID2'])
        return list(gene_set), ambiguous_gene_dict

    def _get_apms_bait_set(self):
        """
        Gets unique set of baits

        :return:
        :rtype: set
        """
        bait_set = set()
        for entry in self._apms_baitlist:
            bait_set.add(entry['GeneID'])
        return bait_set

    def get_gene_node_attributes(self):
        """
        Gene gene node attributes which is output as a list of
        dicts in this format:

        .. code-block::

            { 'GENEID': { 'name': 'GENESYMBOL',
                          'represents': 'ensemble:ENSEMBLID1;ENSEMBLID2..',
                          'ambiguous': 'ALTERNATE GENEs' }
            }



        :return: (list of dicts containing gene node attributes,
                  list of str describing any errors encountered)
        :rtype: tuple
        """
        genelist, ambiguous_gene_dict = self._get_unique_genelist_from_edgelist()
        query_res = self._genequery.get_symbols_for_genes(genelist=genelist)
        bait_set = self._get_apms_bait_set()
        errors = []
        gene_node_attrs = {}
        for x in query_res:
            if 'symbol' not in x:
                errors.append('Skipping ' + str(x) +
                              ' no symbol in query result: ' + str(x))
                logger.error(errors[-1])
                continue

            ensemblstr = 'ensembl:'
            if 'ensembl' not in x:
                errors.append('Skipping ' + str(x) +
                              ' no ensembl in query result: ' + str(x))
                logger.error(errors[-1])
                continue
            if len(x['ensembl']) > 1:
                ensemblstr += ';'.join([g['gene'] for g in x['ensembl']])
            else:
                ensemblstr += x['ensembl']['gene']

            ambiguous_str = ''
            if x['symbol'] in ambiguous_gene_dict:
                ambiguous_str = ambiguous_gene_dict[x['symbol']]

            gene_node_attrs[x['query']] = {'name': x['symbol'],
                                           'represents': ensemblstr,
                                           'ambiguous': ambiguous_str,
                                           'bait': x['query'] in bait_set}

        return gene_node_attrs, errors
