import pytest
from classifier_pipeline.db import Db
from rethinkdb import RethinkDB
from classifier_pipeline.utils import db_loader

# from collections import deque

r = RethinkDB()


@pytest.fixture
def dict_items():
    items = [{'id': '1', 'field1': 1, 'field2': 2}, {'id': '2', 'field1': 3, 'field2': 4}]
    return (item for item in items)


class TestDbInstance:
    db = Db(db_name='test')

    def test_db_default_attr(self):
        assert self.db.host == 'localhost'
        assert self.db.port == 28015
        assert self.db.user == 'admin'
        assert self.db.password == ''

    def test_set_db_attr(self):
        db_custom = Db(host='ahost', port=28016, user='u', password='p')
        assert db_custom.host == 'ahost'
        assert db_custom.port == 28016
        assert db_custom.user == 'u'
        assert db_custom.password == 'p'

    def test_table_access(self):
        table_name = 'sometable'
        _, conn, db, _ = self.db.access_table(table_name)
        assert table_name in db.table_list().run(conn)

    def test_table_set(self):
        table_name = 'sometable'
        initial = {'id': '1', 'field1': 1}
        update = {'id': '1', 'field1': 2}
        _, conn, _, table = self.db.access_table(table_name)
        self.db.set(table_name, initial)
        found_initial = table.get(initial['id']).run(conn)
        assert found_initial['field1'] == initial['field1']

        self.db.set(table_name, update)
        found_update = table.get(update['id']).run(conn)
        assert found_update['field1'] == update['field1']


####################################################
#                  Load
####################################################


def test_db_loader(dict_items):
    results = db_loader(table_name='test')(dict_items)
    for result in results:
        assert result is not None
