import pytest
from classifier_pipeline.classifier_pipeline import csv2dict_reader


@pytest.fixture
def items_numeric():
    items = range(10)
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
