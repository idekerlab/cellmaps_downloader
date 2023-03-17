
import re
import csv
import mygene


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

        :param gene_set:
        :param geneid:
        :return:
        """
        if gene_set is None:
            return
        if geneid is None:
            return

        split_str = re.split('\W*,\W*', geneid)
        gene_set.update(split_str)
        if ambiguous_gene_dict is not None:
            if len(split_str) > 1:
                for entry in split_str:
                    ambiguous_gene_dict[entry] = geneid
        return split_str


class ImageGeneNodeAttributeGenerator(GeneNodeAttributeGenerator):
    """
    Creates Image Gene Node Attributes table
    """
    def __init__(self):
        """
        Constructor
        """
        super().__init__()


class APMSGeneNodeAttributeGenerator(GeneNodeAttributeGenerator):
    """
    Creates APMS Gene Node Attributes table
    """
    def __init__(self, apms_edgelist=None, apms_baitlist=None,
                 genequery=GeneQuery()):
        """
        Constructor
        """
        super().__init__()
        self._apms_edgelist = apms_edgelist
        self._apms_baitlist = apms_baitlist
        self._genequery = genequery

    def _get_edgelist(self):
        """

        :return:
        """
        edgelist = []
        with open(self._apms_edgelist, 'r') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                edgelist.append({'GeneID1': row['GeneID1'],
                                 'Symbol1': row['Symbol1'],
                                 'GeneID2': row['GeneID2'],
                                 'Symbol2': row['Symbol2']})
        return edgelist

    def _get_unique_genelist_from_edgelist(self, edgelist=None):
        """

        :return: unique list of genes from edge list
        :rtype: list
        """
        gene_set = set()
        ambiguous_gene_dict = {}

        for row in edgelist:
            GeneNodeAttributeGenerator.add_geneids_to_set(gene_set=gene_set,
                                                          ambiguous_gene_dict=ambiguous_gene_dict,
                                                          geneid=row['GeneID1'])
            GeneNodeAttributeGenerator.add_geneids_to_set(gene_set=gene_set,
                                                          ambiguous_gene_dict=ambiguous_gene_dict,
                                                          geneid=row['GeneID2'])
        return list(gene_set), ambiguous_gene_dict

    def _querygenes(self, genelist=None):
        """
        Queries for genes via GeneQuery() object passed in via
        constructor
        :param genelist:
        :type genelist: list
        :return:
        :rtype: dict
        """
        res = self._querygenes.querymany(genelist,
                                         species='human',
                                         scopes='_id',
                                         fields=['ensembl.gene','symbol'])
        return res

    def _get_query_symbol_dicts(self, query_res=None):
        """

        :param genelist:
        :return:
        """
        symbol_ensembl_dict = dict()
        query_symbol_dict = dict()
        symbol_query_dict = dict()
        for x in query_res:
            query_symbol_dict[x['query']] = x['symbol']
            symbol_query_dict[x['symbol']] = x['query']

            if x['symbol'] not in symbol_ensembl_dict:
                symbol_ensembl_dict[x['symbol']] = 'ensembl:'
            if 'ensembl' not in x:
                continue
            if len(x['ensembl']) > 1:
                for g in x['ensembl']:
                    symbol_ensembl_dict[x['symbol']] += g['gene'] + ';'
            else:
                symbol_ensembl_dict[x['symbol']] += x['ensembl']['gene']
        return symbol_ensembl_dict, query_symbol_dict, symbol_query_dict

    def get_gene_node_attributes(self):
        """

        :return: list of dicts containing gene node attributes
        :rtype: dict
        """
        edgelist = self._get_edgelist()
        genelist, ambiguous_gene_dict = self._get_unique_genelist_from_edgelist(edgelist=edgelist)
        query_res = self._querygenes(genelist=genelist)
        symbol_ensembl_dict,\
        query_symbol_dict,\
        symbol_query_dict = self._get_query_symbol_dicts(query_res=query_res)

        for entry in edgelist:
            pass


