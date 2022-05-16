import pytest
from classifier_pipeline.classifier_pipeline import (
    csv2dict_reader,
    filter,
    limit_filter,
    list_transformer,
    chunker,
    citation_pubtype_filter
    )
from ncbiutils.pubmedxmlparser import Citation

@pytest.fixture
def numeric_items():
    items = range(10)
    return (item for item in items)


@pytest.fixture
def dict_items():
    items = [{'field1': 1, 'field2': 2}, {'field1': 3, 'field2': 4}]
    return (item for item in items)


@pytest.fixture
def updatefiles_stream(shared_datadir):
    return (shared_datadir / 'updatefiles.csv').open()


@pytest.fixture
def citation_items():
    c_article = Citation(pmid='pmid1', title='title1', journal={}, publication_type_list=['D016428'])
    c_review = Citation(pmid='pmid2', title='title2', journal={}, publication_type_list=['D016454', 'D016428'])
    c_cookbook = Citation(pmid='pmid3', title='title3', journal={}, publication_type_list=['D055823'])
    return (c for c in [c_article, c_review, c_cookbook])

####################################################
#                  Extract
####################################################


def test_csv2dict_reader_generates_correct_keys(updatefiles_stream):
    items = csv2dict_reader(updatefiles_stream)
    item = next(items)
    assert item is not None
    assert isinstance(item, dict)
    assert 'filename' in item
    assert 'type' in item


####################################################
#                  Filter
####################################################


def test_filter(numeric_items):
    predicate = lambda x: x < 5
    items = filter(predicate)(numeric_items)
    results = list(items)
    assert len(results) == 5


def test_limit_filter(numeric_items):
    limit = 3
    items = limit_filter(limit)(numeric_items)
    results = list(items)
    assert len(results) == limit


def test_pubtype_filter(citation_items):
    citations = list(citation_pubtype_filter(citation_items))
    assert len(citations) == 1
    assert citations[0].pmid == 'pmid1'


####################################################
#                  Transform
####################################################


def test_list_transformer(dict_items):
    field = 'field1'
    items = list_transformer(field)(dict_items)
    results = list(items)
    assert isinstance(results, list)
    assert 1 in results
    assert 3 in results


def test_chunker(numeric_items):
    size = 3
    items = chunker(size)(numeric_items)
    results = list(items)
    assert len(results[0]) == 3
    assert len(results[1]) == 3
    assert len(results[2]) == 3
    assert len(results[3]) == 1
