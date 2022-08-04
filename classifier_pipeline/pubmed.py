from typing import Callable, Generator, List, Dict, Any, Tuple
from ncbiutils.ncbiutils import PubMedFetch, PubMedDownload
from ncbiutils.pubmedxmlparser import Citation
from ncbiutils.types import DbEnum
from pathway_abstract_classifier.pathway_abstract_classifier import Classifier, Prediction
from . import ftp
from loguru import logger
import time
import re
from . import db
import calendar
import datetime
import pytz


def unique_list(alist):
    return list(dict.fromkeys(alist))


####################################################
#                  Extractor
####################################################


def updatefiles_extractor(
    host: str = 'ftp.ncbi.nlm.nih.gov', passwd: str = 'info@biofactoid.org', path: str = 'pubmed/updatefiles'
):
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

    def _updatefiles_facts_db_filter(facts):
        for fact in facts:
            db_fact = database.get(table_name=table_name, id=fact['id'])
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
            'D016428',
        ]
    )  # Journal Article
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
        item_list = items if type == 'download' else [item for item in items]
        chunks = pmt.get_citations(item_list)
        for chunk in chunks:
            error, citations, ids = chunk
            if error is not None:
                logger.error(f'Error retrieving ids: {ids}')
                continue
            else:
                logger.info('Downloaded {n} citations', n=len(citations))
                yield from citations

    return _pubmed_fetch_transformer


def prediction_db_transformer() -> Callable[
    [Generator[Prediction, None, None]], Generator[Dict[str, Any], None, None]
]:
    """Format prediction so it can be inserted into database"""
    month_names = {month: index for index, month in enumerate(calendar.month_abbr) if month}

    def _get_pub_month(journal):
        result = None
        pub_month = journal['pub_month']
        if pub_month is not None:
            try:
                result = int(pub_month)
            except ValueError:
                result = month_names[pub_month]
        return result

    def _get_pub_date(document):
        MIN_RETHINKDB_MONTH = 1
        MIN_RETHINKDB_DAY = 1
        journal = document['journal']
        pub_year = journal['pub_year']
        if pub_year is None:
            return None
        else:
            pub_year = int(pub_year)
            pub_month = _get_pub_month(journal) if journal['pub_month'] is not None else MIN_RETHINKDB_MONTH
            pub_day = int(journal['pub_day']) if journal['pub_day'] is not None else MIN_RETHINKDB_DAY
            return datetime.datetime(pub_year, pub_month, pub_day, tzinfo=pytz.UTC)

    def _prediction_db_transformer(
        predictions: Generator[Prediction, None, None]
    ) -> Generator[Dict[str, Any], None, None]:
        for prediction in predictions:
            document, classification, probability = prediction
            pub_date = _get_pub_date(document)
            document.update(
                {
                    'id': document['pmid'],
                    'classification': classification,
                    'probability': probability,
                    'pub_date': pub_date,
                    'last_updated': datetime.datetime.now(pytz.UTC)
                }
            )
            yield document

    return _prediction_db_transformer


def _supplement_author(doc: Dict[str, Any], citation: Citation):
    """Merge the citation email information per author, and correspondence"""
    citation_authors = citation.author_list
    doc_authors = doc['author_list']
    doc['correspondence'] = doc['correspondence'] + citation.correspondence
    authors_with_emails = [a for a in citation_authors if a.emails is not None]
    for author_with_emails in authors_with_emails:
        matching_doc_author = next(
            (
                doc_author
                for doc_author in doc_authors
                if doc_author['fore_name'] == author_with_emails.fore_name
                and doc_author['last_name'] == author_with_emails.last_name
            ),
            None,
        )
        if matching_doc_author is not None:
            emails = author_with_emails.emails
            if matching_doc_author['emails'] is not None:
                emails = emails + matching_doc_author['emails']
            matching_doc_author['emails'] = unique_list(emails)


def _supplement_docs(docs: List[Dict[str, Any]], citations: List[Citation]):
    """Match citations to a doc, then delegate tasks"""
    for citation in citations:
        pmid = citation.pmid
        doc = next((item for item in docs if item['pmid'] == pmid), None)
        if doc is None:
            continue
        else:
            _supplement_author(doc, citation)


def pmc_supplement_transfomer() -> Callable[
    [Generator[List[Dict[str, Any]], None, None]], Generator[Dict[str, Any], None, None]
]:
    """Add PMC information to documents (when available)"""
    pmt = PubMedFetch(db=DbEnum.pmc)

    def _pmc_supplement_transfomer(
        chunks: Generator[List[Dict[str, Any]], None, None]
    ) -> Generator[Dict[str, Any], None, None]:
        for docs in chunks:
            uids = [doc['pmc'] for doc in docs if doc['pmc'] is not None]
            logger.info('Retrieving {n} PMC IDs', n=len(uids))
            pubmed_chunks = pmt.get_citations(uids=uids)
            for pubmed_chunk in pubmed_chunks:
                error, citations, ids = pubmed_chunk
                if error is not None:
                    logger.error(f'Error retrieving ids: {ids}')
                    continue
                else:
                    logger.info('Downloaded {n} pmc citations', n=len(citations))
                    _supplement_docs(docs, citations)
            yield from docs

    return _pmc_supplement_transfomer


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
