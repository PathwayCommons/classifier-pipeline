import pytest
from classifier_pipeline.classifier_pipeline import csv2dict_reader, filter, limit_filter, list_transformer, chunker


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
