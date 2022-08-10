from typing import Dict, Any, Generator
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse
from datetime import datetime
import pytz
from classifier_pipeline.db import Db, MAX_DATE, MIN_DATE, MAX_NUM_ITEMS
from loguru import logger
from enum import Enum
import csv
from io import StringIO


####################################################
#                  Variables & Types
####################################################

app = FastAPI()
date_regex = r'^\d{4}-\d{2}-\d{2}$'


class RetTypeEnum(str, Enum):
    default = 'default'
    merge = 'merge'


class RetModeEnum(str, Enum):
    json = 'json'
    csv = 'csv'


####################################################
#                  Helpers
####################################################


def to_date(input: str):
    output = None
    try:
        output = datetime.strptime(input, '%Y-%m-%d').replace(tzinfo=pytz.UTC)
    except ValueError:
        logger.error('Invalid date: {input}', input=input)
        raise HTTPException(status_code=500, detail=f'Invalid date: {input}')
    else:
        return output


####################################################
#                  Database
####################################################

MAX_DATE_TIME = to_date(MAX_DATE)
DEFAULT_LIMIT = MAX_NUM_ITEMS
DB_HOST = 'localhost'
DB_PORT = '28015'
DB_USERNAME = 'admin'
DB_PASSWORD = ''
DB_TABLE = 'documents'
database = Db(host=DB_HOST, port=DB_PORT, username=DB_USERNAME, password=DB_PASSWORD)
r, conn, db, table = database.access_table(table_name=DB_TABLE)


####################################################
#                  Endpoints
####################################################


def load(updated: str, start: str, end: str, limit: int, skip: int):
    """Access the database as specified"""
    last_updated = to_date(updated)
    start_date = to_date(start)
    end_date = to_date(end)
    q = table
    # ---- Select ----
    # Minimum last updated date
    q = q.between(last_updated, MAX_DATE_TIME, index='last_updated', right_bound='closed')

    # ---- Filter ----
    # Publication date range
    pubdate_filter = r.row['pub_date'].ge(start_date) & r.row['pub_date'].le(end_date)
    docFilters = pubdate_filter
    q = q.filter(docFilters)

    # ---- Order ----
    q = q.order_by(r.desc('pub_date'))

    # ---- Limit ----
    q = q.limit(limit)
    q = q.skip(skip)

    cursor = q.run(conn)
    yield from cursor


def _as_merge(items: Generator[Dict[str, Any], None, None]) -> Generator[Dict[str, Any], None, None]:
    """Format output fields useful in mail-merge"""

    def _get_citation(item: Dict[str, Any]):
        journal = item['journal']
        full_title = journal['title'] if 'title' in journal else ''
        abbrev_title = journal['iso_abbreviation'] if 'iso_abbreviation' in journal else None
        title = abbrev_title if abbrev_title is not None else full_title
        sep = ', ' if title else ''
        year = item['pub_date'].year
        citation = f'{title}{sep}{year}'
        return citation

    def _get_author(item: Dict[str, Any]):
        """Get a useful author

        Retrieve the last listed author with an associated email.
        Otherwise retrieve the last author, possibly with a correponding email.
        """
        author = None
        emailRecipientAddress = None
        author_list = item['author_list']  # always exists
        with_emails = [a for a in author_list if a['emails'] is not None]
        has_author_email = len(with_emails)

        if has_author_email:
            author = with_emails[-1]
            emailRecipientAddress = author['emails'][-1]
        else:
            author = author_list[-1]

        if not emailRecipientAddress:
            correspondence = item['correspondence']
            has_correspondence = len(correspondence) > 0
            if has_correspondence:
                correspondence_item = correspondence[-1]
                if 'emails' in correspondence_item and len(correspondence_item['emails']):
                    emailRecipientAddress = correspondence_item['emails'][-1]
        return {'emailRecipientAddress': emailRecipientAddress, 'authorName': author['fore_name']}

    for item in items:
        author = _get_author(item)
        yield {
            'pmid': item['pmid'],
            'doi': item['doi'],
            'articleCitation': _get_citation(item),
            'pubDate': item['pub_date'],
            'lastUpdated': item['last_updated'],
            'authorName': author['authorName'],
            'emailRecipientAddress': author['emailRecipientAddress'],
        }


def to_ret_type(
    items: Generator[Dict[str, Any], None, None], rettype: RetTypeEnum
) -> Generator[Dict[str, Any], None, None]:
    """Format by delegating to a specific formatter"""
    typed = items
    if rettype == RetTypeEnum.default:
        pass
    elif rettype == RetTypeEnum.merge:
        typed = _as_merge(items)
    else:
        raise ValueError(f'Unsupported RetType: {rettype}')
    yield from typed


def _as_csv(items: Generator[Dict[str, Any], None, None]) -> Generator[Any, None, None]:
    """
    Create a new csv file that represents generated data.
    """
    csvfile = StringIO()
    first = next(items, None)
    if first is not None:
        fieldnames = list(first.keys())
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(first)
        writer.writerows(items)
        csvfile.seek(0)  # need to seek first posittion for readline() to return something
        line = csvfile.readline()
        while len(line) > 0:
            yield line
            line = csvfile.readline()
    csvfile.close()


def to_ret_mode(items: Generator[Dict[str, Any], None, None], retmode: RetModeEnum) -> Generator[Any, None, None]:
    """Map to particular MIME type"""
    result = items
    if retmode == RetModeEnum.json:
        return result
    elif retmode == RetModeEnum.csv:
        result = _as_csv(items)
        return StreamingResponse(result, media_type="text/csv")
    else:
        raise ValueError(f'Unsupported RetMode: {retmode}')


@app.get('/')
def feed(
    updated: str = Query(default=MIN_DATE, regex=date_regex),
    start: str = Query(default=MIN_DATE, regex=date_regex),
    end: str = Query(default=MAX_DATE, regex=date_regex),
    limit: int = Query(default=DEFAULT_LIMIT, ge=0, le=MAX_NUM_ITEMS),
    skip: int = Query(default=0, ge=0, le=MAX_NUM_ITEMS),
    rettype: RetTypeEnum = RetTypeEnum.default,
    retmode: RetModeEnum = RetModeEnum.json,
):
    items = load(updated, start, end, limit, skip)
    items = to_ret_type(items, rettype)
    items = to_ret_mode(items, retmode)
    return items
