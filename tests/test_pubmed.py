import pytest
from classifier_pipeline.pubmed import (
    citation_pubtype_filter,
    classification_transformer,
    pubmed_transformer,
    prediction_db_transformer,
    citation_date_filter,
)
from ncbiutils.pubmedxmlparser import Citation
from pathway_abstract_classifier.pathway_abstract_classifier import Prediction

uids = ('1', '2', '3')
citations = (
    Citation(pmid='1', title='title1', abstract='', journal={'pub_year': None}, publication_type_list=['D016428']),
    Citation(
        pmid='2',
        title='title2',
        abstract='',
        journal={'pub_year': '1997'},
        publication_type_list=['D016454', 'D016428'],
    ),
    Citation(pmid='3', title='title3', abstract='', journal={'pub_year': '2022'}, publication_type_list=['D055823']),
)


@pytest.fixture
def citation_items():
    return (c for c in citations)


@pytest.fixture
def citation_chunks():
    yield [citations]


@pytest.fixture
def prediction_items():
    c_as_dicts = [c.dict() for c in citations]
    p0 = Prediction(document=c_as_dicts[0], classification=1, probability=1)
    p1 = Prediction(document=c_as_dicts[1], classification=1, probability=1)
    return (p for p in [p0, p1])


@pytest.fixture
def uid_items():
    return (c for c in uids)


@pytest.fixture
def citations_chunks():
    error = None
    yield [(error, citations, uids)]


####################################################
#                  Filter
####################################################


def test_citation_pubtype_filter(citation_items):
    results = list(citation_pubtype_filter(citation_items))
    assert len(results) == 1


def test_citation_date_filter(citation_items):
    citations = list(citation_date_filter(2021)(citation_items))
    assert len(citations) == 1

def test_citation_date_filter_disabled(citation_items):
    citations = list(citation_date_filter()(citation_items))
    assert len(citations) == 3


####################################################
#                  Transform
####################################################


def test_classification_transformer(mocker, citation_chunks, prediction_items):
    mocker.patch('classifier_pipeline.pubmed.Classifier.predict', return_value=prediction_items)
    predictions = classification_transformer()(citation_chunks)
    p_list = list(predictions)
    assert p_list is not None
    assert len(p_list) == 2


def test_pubmed_transformer(mocker, uid_items, citations_chunks):
    mocker.patch('classifier_pipeline.pubmed.PubMedFetch.get_citations', return_value=citations_chunks)
    citations = list(pubmed_transformer()(uid_items))
    assert len(citations) == 3


def test_prediction_db_transformer(prediction_items):
    formatted = list(prediction_db_transformer()(prediction_items))
    for item in formatted:
        assert 'id' in item