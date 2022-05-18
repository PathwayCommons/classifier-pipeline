from typing import Callable, Generator, List, Dict, Any
from ncbiutils.ncbiutils import PubMedFetch, PubMedDownload
from ncbiutils.pubmedxmlparser import Citation
from pathway_abstract_classifier.pathway_abstract_classifier import Classifier, Prediction
from loguru import logger
import time


####################################################
#                  Filter
####################################################


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
    min_year: int = None
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
