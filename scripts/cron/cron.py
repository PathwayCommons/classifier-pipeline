from loguru import logger
import argparse
from classifier_pipeline.utils import as_pipeline, chunker, db_loader, filter, exhaust
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
    prediction_print_spy,
)


####################################################
#            Logging
####################################################


logger.add("cron_{time}.log")


####################################################
#            Command line args
####################################################

parser = argparse.ArgumentParser()
parser.add_argument('--threshold', nargs='?', type=float, default=str(0.990))
parser.add_argument('--table', nargs='?', type=str, default='documents')
parser.add_argument('--minyear', nargs='?', type=int, default=str(2021))


def get_opts():
    args = parser.parse_args()
    opts = {
        'threshold': args.threshold,
        'table': args.table,
        'minyear': args.minyear,
    }
    if opts['threshold'] < 0 or opts['threshold'] > 1:
        raise ValueError('threshold must be on [0, 1]')
    return opts


####################################################
#                 __main__
####################################################

if __name__ == '__main__':
    opts = get_opts()
    logger.info('Run config: {opts}', opts=opts)
    pipeline = as_pipeline(
        [
            updatefiles_extractor(),
            updatefiles_data_filter(),
            updatefiles_content2facts_transformer,
            updatefiles_facts_db_filter(),
            pubmed_transformer(type='download'),
            citation_pubtype_filter,
            citation_date_filter(opts['minyear']),
            chunker(1000),
            classification_transformer(threshold=opts['threshold']),
            prediction_print_spy,
            filter(lambda x: x.classification == 1),
            prediction_db_transformer(),
            db_loader(table_name=opts['table']),
            exhaust,
        ]
    )
