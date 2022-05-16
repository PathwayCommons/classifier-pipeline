import csv
from typing import Dict, List, Callable, Any, Generator, IO
import sys
from ncbiutils.ncbiutils import PubMedFetch, PubMedDownload
from ncbiutils.pubmedxmlparser import Citation
from pathway_abstract_classifier.pathway_abstract_classifier import Classifier, Prediction
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
parser.add_argument('--type', nargs='?', type=str, default='fetch')
parser.add_argument('--idcolumn', nargs='?', type=str, default='pmid')


def get_opts() -> Dict[str, Any]:
    args = parser.parse_args()
    opts = {'retmax': args.retmax, 'threshold': args.threshold, 'type': args.type, 'idcolumn': args.idcolumn}

    if opts['retmax'] < 0:
        raise ValueError('retmax must be non-negative')
    elif opts['retmax'] > retmax_limit:
        opts['retmax'] = retmax_limit

    if opts['threshold'] < 0 or opts['threshold'] > 1:
        raise ValueError('threshold must be on [0, 1]')

    return opts


####################################################
#                 Pipeline
####################################################


def as_pipeline(steps: List[Callable[[Any], Any]]) -> Callable[[Any], Any]:
    generator = steps.pop(0)
    for step in steps:
        generator = step(generator)
    return generator


####################################################
#                  Extract
####################################################


def csv2dict_reader(stream: IO) -> Callable[[None], Generator[Dict[str, Any], None, None]]:
    """Return a reader that streams csv to dicts"""

    def _csv2dict_reader():
        reader = csv.DictReader(stream)
        yield from reader

    return _csv2dict_reader()


####################################################
#                  Filter
####################################################


def filter(predicate: Callable[[Any], Any]) -> Callable[[Generator[Any, None, None]], Generator[Any, None, None]]:
    def _filter(items):
        for item in items:
            if predicate(item):
                yield item

    return _filter


def limit_filter(limit: int) -> Callable[[Generator[Any, None, None]], Generator[Any, None, None]]:
    counter = 0

    def _limit(items):
        nonlocal counter
        for item in items:
            if counter == limit:
                break
            else:
                counter += 1
                yield item

    return _limit


def citation_pubtype_filter(citations: Generator[Citation, None, None]) -> Generator[Citation, None, None]:
    """filter citations by publication types"""
    pubtypes_to_exclude = set(
        [
            'D016420',  # Comment
            'D016454',  # Review
            'D016440',  # Retraction of Publication
            'D016441',  # Retracted Publication
            'D016425',  # Published Erratum
        ]
    )
    pubtypes_to_include = set(
        [
            'D016428',  # Journal Article
        ]
    )
    for citation in citations:
        excluded = False
        pubtypes = set(citation.publication_type_list)
        if len(pubtypes_to_exclude.intersection(pubtypes)) > 0:
            excluded = True
        if len(pubtypes_to_include.intersection(pubtypes)) == 0:
            excluded = True
        if not excluded:
            yield citation


####################################################
#                  Transform
####################################################


def chunker(size: int) -> Callable[[Generator[Any, None, None]], Generator[Any, None, None]]:
    """Aggregate items into chunks of a given size"""

    def _chunker(items):
        chunk = []
        while True:
            try:
                if len(chunk) == size:
                    yield chunk
                    chunk = []
                else:
                    item = next(items)
                    chunk.append(item)
            except StopIteration:
                if chunk:
                    yield chunk
                break

    return _chunker


def list_transformer(field: str) -> Callable[[Generator[Any, None, None]], Generator[Any, None, None]]:
    """Create a list from a field"""

    def _list_transformer(items):
        for item in items:
            yield item[field]

    return _list_transformer


def classification_transformer(
    **opts,
) -> Callable[[Generator[Citation, None, None]], Generator[Prediction, None, None]]:
    """Filter the chunks of articles based on text content"""
    classifier = Classifier(**opts)

    def _classification_transformer(chunks):
        for chunk in chunks:
            logger.info('Classifying: {n}', n=len(chunk))
            start = time.time()
            prediction = classifier.predict([c.dict() for c in chunk])
            end = time.time()
            logger.info(
                'Finished classification in {elapsed} seconds',
                elapsed=(end - start),
            )
            yield from prediction

    return _classification_transformer


def pubmed_transformer(
    type: str = 'fetch', **opts
) -> Callable[[Generator[Any, None, None]], Generator[Citation, None, None]]:
    """Retrieve the PubMed records"""
    pmt = PubMedDownload() if type == 'download' else PubMedFetch(**opts)

    def _pubmed_fetch_transformer(items):
        chunks = pmt.get_citations(list(items))
        for chunk in chunks:
            error, citations, ids = chunk
            if error is not None:
                logger.error(f'Error retrieving ids: {ids}')
                continue
            else:
                logger.info('Downloaded: {n}', n=len(citations))
                yield from citations

    return _pubmed_fetch_transformer


####################################################
#                  Load
####################################################


def print_loader(items):
    for item in items:
        logger.info(len(item))


def prediction_print_loader(predictions):
    tcount = 0
    pcount = 0
    for p in predictions:
        tcount += 1
        if p.classification == 1:
            pcount += 1
            logger.info(
                'Positive {n} out of {t} analyzed ({rate}%) -- pmid: {pmid}; prob: {prob}',
                n=pcount,
                t=tcount,
                rate=pcount / tcount,
                pmid=p.document['pmid'],
                prob=p.probability,
            )
    logger.info('Analyzed {tcount} articles', tcount=tcount)
    logger.info('Identified {pcount} positives', pcount=pcount)


####################################################
#                 __main__
####################################################

if __name__ == '__main__':
    opts = get_opts()
    print(opts)

    pipeline = as_pipeline(
        [
            csv2dict_reader(sys.stdin),
            list_transformer(opts['idcolumn']),
            pubmed_transformer(type=opts['type']),
            citation_pubtype_filter,
            limit_filter(100000),
            chunker(1000),
            classification_transformer(threshold=opts['threshold']),
            prediction_print_loader,
        ]
    )
