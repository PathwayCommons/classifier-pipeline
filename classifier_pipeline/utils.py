import csv
from typing import Dict, List, Callable, Any, Generator, IO
from . import db

####################################################
#                 Pipeline
####################################################


def as_pipeline(steps: List[Callable[[Any], Any]]) -> Callable[[Any], Any]:
    generator = steps.pop(0)
    for step in steps:
        generator = step(generator)
    return generator


####################################################
#                  Extract
####################################################


def csv2dict_reader(stream: IO) -> Callable[[None], Generator[Dict[str, Any], None, None]]:
    """Return a reader that streams csv to dicts"""

    def _csv2dict_reader():
        reader = csv.DictReader(stream)
        yield from reader

    return _csv2dict_reader()


####################################################
#                  Filter
####################################################


def filter(predicate: Callable[[Any], Any]) -> Callable[[Generator[Any, None, None]], Generator[Any, None, None]]:
    def _filter(items):
        for item in items:
            if predicate(item):
                yield item

    return _filter


def limit_filter(limit: int) -> Callable[[Generator[Any, None, None]], Generator[Any, None, None]]:
    counter = 0

    def _limit(items):
        nonlocal counter
        for item in items:
            if counter == limit:
                break
            else:
                counter += 1
                yield item

    return _limit


####################################################
#                  Transform
####################################################


def chunker(size: int) -> Callable[[Generator[Any, None, None]], Generator[Any, None, None]]:
    """Aggregate items into chunks of a given size"""

    def _chunker(items):
        chunk = []
        while True:
            try:
                if len(chunk) == size:
                    yield chunk
                    chunk = []
                else:
                    item = next(items)
                    chunk.append(item)
            except StopIteration:
                if chunk:
                    yield chunk
                break

    return _chunker


def list_transformer(field: str) -> Callable[[Generator[Any, None, None]], Generator[Any, None, None]]:
    """Create a list from a field"""

    def _list_transformer(items):
        for item in items:
            yield item[field]

    return _list_transformer


####################################################
#                  Load
####################################################


def db_loader(table_name: str, host: str = 'localhost', port:int = 28015, username:str = 'admin', password: str = ''):
    """Insert (or replace) items in the named database table"""
    database = db.Db(host=host, port=port, username=username, password=password)

    def _db_loader(items):
        for item in items:
            yield database.set(table_name, item)

    return _db_loader