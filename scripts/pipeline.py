import sys
from loguru import logger
import argparse
from collections import deque
from classifier_pipeline.utils import (
    as_pipeline,
    csv2dict_reader,
    list_transformer,
    limit_filter,
    chunker,
    db_loader,
    filter,
)
from classifier_pipeline.classifier_pipeline import (
    pubmed_transformer,
    citation_pubtype_filter,
    classification_transformer,
    prediction_db_transformer,
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
        logger.info(item)


def exhaust(generator):
    deque(generator, maxlen=0)


def prediction_print_loader(predictions):
    tcount = 0
    pcount = 0
    tprobability = 0
    for p in predictions:
        tcount += 1
        if p.classification == 1:
            pcount += 1
            tprobability += p.probability
            logger.info(
                'Counted {n} hits out of {t} ({rate:.3g}%) -- pmid: {pmid} -- prob={prob:.3g}, mean={mu:.3g}',
                n=pcount,
                t=tcount,
                rate=pcount / tcount,
                pmid=p.document['pmid'],
                prob=p.probability,
                mu=tprobability / pcount,
            )
        yield p


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
            limit_filter(100),
            chunker(25),
            classification_transformer(threshold=opts['threshold']),
            prediction_print_loader,
            filter(lambda x: x.classification == 1),
            prediction_db_transformer(),
            db_loader(table_name='articles'),
            exhaust,
        ]
    )
