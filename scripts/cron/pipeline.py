import sys
from loguru import logger
import argparse
from collections import deque
from classifier_pipeline.utils import (
    as_pipeline,
    csv2dict_reader,
    list_transformer,
    chunker,
    db_loader,
    filter,
)
from classifier_pipeline.pubmed import (
    updatefiles_extractor,
    updatefiles_data_filter,
    updatefiles_content2facts_transformer,
    updatefiles_facts_db_filter,
    pubmed_transformer,
    citation_pubtype_filter,
    classification_transformer,
    prediction_db_transformer,
    citation_date_filter,
)


####################################################
#            Command line args
####################################################
default_threshold = 0.5
parser = argparse.ArgumentParser()
parser.add_argument('--threshold', nargs='?', type=float, default=str(default_threshold))
parser.add_argument('--table', nargs='?', type=str, default='articles')
parser.add_argument('--minyear', nargs='?', type=int)


def get_opts():
    args = parser.parse_args()
    opts = {
        'threshold': args.threshold,
        'minyear': args.minyear,
    }

    if opts['threshold'] < 0 or opts['threshold'] > 1:
        raise ValueError('threshold must be on [0, 1]')

    if opts['minyear'] == 'None':
        raise ValueError('threshold must be on [0, 1]')

    return opts


####################################################
#                  Helpers
####################################################


def printer(items):
    for item in items:
        print(item)
        yield item


def exhaust(generator):
    deque(generator, maxlen=0)


def prediction_print_spy(predictions):
    tcount = 0
    pcount = 0
    tprobability = 0
    for p in predictions:
        tcount += 1
        if p.classification == 1:
            pcount += 1
            tprobability += p.probability
            logger.info(
                'Identified {n} hits from {t} tested ({rate:.3g}%);'
                'mean probability: {mu:.3g} --- pmid: {pmid}; prob={prob:.3g}',
                n=pcount,
                t=tcount,
                rate=100 * (pcount / tcount),
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
            updatefiles_extractor(),
            updatefiles_data_filter(),
            updatefiles_content2facts_transformer,
            updatefiles_facts_db_filter(table_name = 'contents'),
            printer,
            # pubmed_transformer(type=opts['type']),
            # citation_pubtype_filter,
            # citation_date_filter(opts['minyear']),
            # chunker(1000),
            # classification_transformer(threshold=opts['threshold']),
            # prediction_print_spy,
            # filter(lambda x: x.classification == 1),
            # prediction_db_transformer(),
            # db_loader(table_name=opts['table']),
            exhaust,
        ]
    )
