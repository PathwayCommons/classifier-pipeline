import csv
import itertools
import sys
from ncbiutils.ncbiutils import PubMedFetch
from pathway_abstract_classifier.pathway_abstract_classifier import Classifier
from loguru import logger
import time
import argparse


####################################################
#            Command line args
####################################################
retmax_limit = 10000
default_threshold = 0.5
parser = argparse.ArgumentParser()
parser.add_argument('--retmax', nargs='?', type=int, default=str(retmax_limit))
parser.add_argument('--threshold', nargs='?', type=float, default=str(default_threshold))

def get_opts():
    args = parser.parse_args()
    opts = {
        'retmax': args.retmax,
        'threshold': args.threshold
    }

    if opts['retmax'] < 0:
        raise ValueError('retmax must be non-negative')
    elif opts['retmax'] > retmax_limit:
        opts.update({'retmax', retmax_limit})

    if opts['threshold'] < 0 or opts['threshold'] > 1:
        raise ValueError('threshold must be on [0, 1]')

    return opts




####################################################
#                 Pipeline
####################################################

def as_pipeline(steps):
    generator = steps.pop(0)
    for step in steps:
        generator = step(generator)
    return generator


####################################################
#                  Utils
####################################################

list2generator = lambda x: (n for n in x)


def counter(items):
    item_list = list(items)
    print(f'Number of items: {len(item_list)}')
    return list2generator(item_list)


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

def pubtype_filter(chunks):
    """filter citations by publication types"""
    pubtypes_to_exclude = [
        'D016420',  # Comment
        'D016454',  # Review
        'D016440',  # Retraction of Publication
        'D016441',  # Retracted Publication
        'D016425',  # Published Erratum
    ]

    for chunk in chunks:
        error, citations, ids = chunk
        if error is not None:
            print(f'Error retrieving ids: {ids}')
            continue
        else:
            articles = []
            logger.info('Filter IN: {n}', n=len(ids))
            documents = [c.dict() for c in citations]
            for document in documents:
                excluded = False
                for pubtype in document['publication_type_list']:
                    if pubtype in pubtypes_to_exclude:
                        excluded = True
                if not excluded:
                    articles.append(document)
            logger.info('Filter OUT: {n}', n=len(articles))
            yield articles

# @profile
def classification_transformer(**opts):
    """Filter the chunks of articles based on text content"""
    classifier = Classifier(**opts)

    def _classification_transformer(chunks):
        for articles in chunks:
            logger.info('Classifying: {n}', n=len(articles))
            start = time.time()
            predictions = classifier.predict(articles)
            end = time.time()
            logger.info(
                'Finished classification in {elapsed} seconds',
                elapsed=(end - start),
            )
            yield from predictions

    return _classification_transformer


####################################################
#                  Transform
####################################################


def list_transformer(field):
    """Create a list from a field"""

    def _list_transformer(items):
        for item in items:
            yield item[field]

    return _list_transformer


def pubmed_citation_transformer(**opts):
    """Retrieve the PubMed record"""
    pmf = PubMedFetch(**opts)

    def _pubmed_citation_transformer(items):
        uids = list(items)
        return pmf.get_citations(uids)

    return _pubmed_citation_transformer


####################################################
#                  Load
####################################################


def print_loader(items):
    for item in items:
        print(item)

def prediction_print_loader(predictions):
    positives = [p for p in predictions if p.classification == 1]
    logger.info('Identified {n} positives', n=positives)

def prediction_print_loader(predictions):
    count = 0
    for prediction in predictions:
        if prediction.classification == 1:
            count += 1
            logger.info('Identified article with prob: {prob}', prob=prediction.probability)
            logger.info('Identified {count} positives', count=count)


####################################################
#                 __main__
####################################################

if __name__ == '__main__':
    opts = get_opts()
    print(f'retmax: {opts["retmax"]}')
    print(f'threshold: {opts["threshold"]}')

    # 106 620 articles w/ 'indra_statements_entity_constraint' > 1
    by_evidences = lambda x: int(x['indra_statements_entity_constraint']) > 1



    pipeline = as_pipeline(
        [
            csv2dict_reader(sys.stdin),
            filter(by_evidences),
            list_transformer('pmid'),
            pubmed_citation_transformer(retmax=opts['retmax']),
            pubtype_filter,
            classification_transformer(threshold=opts['threshold']),
            prediction_print_loader,
        ]
    )
