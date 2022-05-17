import sys
from loguru import logger
import argparse
from classifier_pipeline.utils import as_pipeline, csv2dict_reader, list_transformer, limit_filter, chunker
from classifier_pipeline.classifier_pipeline import (
    pubmed_transformer,
    citation_pubtype_filter,
    classification_transformer,
)

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


def get_opts():
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
            list_transformer(field=opts['idcolumn']),
            pubmed_transformer(type=opts['type']),
            citation_pubtype_filter,
            limit_filter(10000),
            chunker(1000),
            classification_transformer(threshold=opts['threshold']),
            prediction_print_loader,
        ]
    )
