from typing import Callable, Generator, List, Dict, Any, Tuple
from ncbiutils.ncbiutils import PubMedFetch, PubMedDownload
from ncbiutils.pubmedxmlparser import Citation
from pathway_abstract_classifier.pathway_abstract_classifier import Classifier, Prediction
from . import ftp
from loguru import logger
import time
import re
from . import db


####################################################
#                  Extractor
####################################################


def updatefiles_extractor(
    host: str = 'ftp.ncbi.nlm.nih.gov', passwd: str = 'info@biofactoid.org', path: str = 'pubmed/updatefiles'
) -> Generator[Tuple[str, Dict[str, str]], None, None]:
    """Extract file and associated facts from the remote server"""
    ftp_client = ftp.Ftp(host=host, passwd=passwd)
    contents = ftp_client.list(path)
    yield from contents


####################################################
#                  Filter
####################################################


def updatefiles_data_filter() -> Callable[
    [Generator[Tuple[str, Dict[str, str]], None, None]], Generator[Tuple[str, Dict[str, str]], None, None]
]:
    """Select contents associated with PubMed article data"""
    file_re = re.compile(r'^pubmed.+\.xml\.gz$')

    def _updatefiles_filter(contents):
        for content in contents:
            name, _ = content
            match = file_re.match(name)
            if match is not None:
                yield content

    return _updatefiles_filter


def updatefiles_facts_db_filter(
    table_name: str = 'contents',
    host: str = 'localhost',
    port: int = 28015,
    username: str = 'admin',
    password: str = '',
):
    """Select filenames that are new (not present in database) and persist"""
    database = db.Db(host=host, port=port, username=username, password=password)
    _, conn, _, table = database.access_table(table_name=table_name)

    def _updatefiles_facts_db_filter(facts):
        for fact in facts:
            db_fact = table.get(fact['id']).run(conn)
            if db_fact is None:
                # save to db
                database.set(table_name=table_name, data=fact)
                # yield the filename
                yield fact['id']

    return _updatefiles_facts_db_filter


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


def citation_date_filter(
    min_year: int = None,
) -> Callable[[Generator[Citation, None, None]], Generator[Citation, None, None]]:
    """filter citations by publication date"""

    def _citation_date_filter(citations):
        for citation in citations:
            try:
                if min_year is None:
                    yield citation
                else:
                    year = citation.journal.pub_year
                    if year is not None and int(year) >= min_year:
                        yield citation
            except ValueError:
                logger.info('Could not identify publication year')
                continue

    return _citation_date_filter


####################################################
#                  Transform
####################################################


def updatefiles_content2facts_transformer(
    contents: Generator[Tuple[str, Dict[str, str]], None, None]
) -> Generator[Dict[str, str], None, None]:
    """Map the file contents to a db-friendly format"""
    for content in contents:
        name, facts = content
        facts['id'] = name
        facts['filename'] = name
        yield facts


def classification_transformer(
    **opts,
) -> Callable[[Generator[List[Citation], None, None]], Generator[Prediction, None, None]]:
    """Filter the chunks of articles based on text content"""
    classifier = Classifier(**opts)

    def _classification_transformer(chunks):
        for chunk in chunks:
            logger.info('Classifying: {n}', n=len(chunk))
            start = time.time()
            prediction = classifier.predict([c.dict() for c in chunk])
            end = time.time()
            logger.info(
                'Finished classification in {elapsed:.3g} seconds',
                elapsed=(end - start),
            )
            yield from prediction

    return _classification_transformer


def pubmed_transformer(
    type: str = 'fetch', **opts
) -> Callable[[Generator[str, None, None]], Generator[Citation, None, None]]:
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
                if type == 'download':
                    logger.info('Processed: {name}', name=chunk)
                logger.info('Downloaded: {n}', n=len(citations))
                yield from citations

    return _pubmed_fetch_transformer


def prediction_db_transformer() -> Callable[
    [Generator[Prediction, None, None]], Generator[Dict[str, Any], None, None]
]:
    """Format prediction so it can be inserted into database"""

    def _prediction_db_transformer(
        predictions: Generator[Prediction, None, None]
    ) -> Generator[Dict[str, Any], None, None]:
        for prediction in predictions:
            document, classification, probability = prediction
            document.update({'id': document['pmid'], 'classification': classification, 'probability': probability})
            yield document

    return _prediction_db_transformer


####################################################
#                  Helpers
####################################################


def prediction_print_spy(predictions):
    """Print out stats related to the classifier output"""
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
