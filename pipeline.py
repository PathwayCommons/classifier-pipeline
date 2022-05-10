import csv
import itertools
import sys
from ncbiutils.ncbiutils import PubMedFetch
# import getopt
# from memory_profiler import profile
# import argparse

# parser = argparse.ArgumentParser()
# parser.add_argument('echo')

# @profile
def as_pipeline(steps):
    generator = steps.pop(0)
    for step in steps:
        generator = step(generator)
    return generator


####################################################
#                  Extract
####################################################

def csv2dict_reader(stream):
    """Return a reader that streams csv to dicts"""
    def _csv2dict_reader():
        reader = csv.DictReader(stream)
        yield from reader
    return _csv2dict_reader()

####################################################
#                  Filter
####################################################

def filter(predicate):
    def _filter(items):
        for item in items:
            if predicate(item):
                yield item
    return _filter

def limit_filter(number):
    def _limit(items):
        yield from itertools.islice(items, number)
    return _limit

####################################################
#                  Transform
####################################################

def list_transformer(field):
    "Create a list from a field"
    def _list_transformer(items):
        for item in items:
            yield item[field]
    return _list_transformer

def pubmed_citation_transformer(items):
    "Retrieve the PubMed record"
    pmf = PubMedFetch()
    uids = list(items)
    return pmf.get_citations(uids)

####################################################
#                  Load
####################################################

def print_loader(items):
    for item in items:
        print(item)

def count_loader(items):
    print(f'Number of items: {len(list(items))}')


if __name__ == '__main__':
    # args = parser.parse_args()
    # print(args.echo)

    by_evidences = lambda x: int(x['indra_evidences_entity_constraint']) > 100

    pipeline = as_pipeline([
        csv2dict_reader(sys.stdin),
        filter(by_evidences),
        list_transformer('pmid'),
        pubmed_citation_transformer,
        print_loader
    ])