from fastapi import FastAPI, HTTPException, Query
from datetime import datetime
import pytz
from classifier_pipeline.db import Db, MAX_DATE, MIN_DATE, MAX_NUM_ITEMS
from loguru import logger


####################################################
#                  Variables
####################################################

app = FastAPI()
date_regex = r'^\d{4}-\d{2}-\d{2}$'


####################################################
#                  Helpers
####################################################

def to_date (input: str):
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
DEFAULT_LIMIT = 1000
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

def load(last_updated: datetime, start_date: datetime, end_date: datetime, limit: int, skip: int):
    q = table
    ### Select
    # Minimum last updated date
    q = q.between(
        last_updated, MAX_DATE_TIME, index='last_updated', right_bound='closed'
    )

    ### Filter
    # Publication date range
    pubdate_filter = r.row['pub_date'].ge(start_date) & r.row['pub_date'].le(end_date)
    docFilters = pubdate_filter
    q = q.filter(docFilters)

    ### Order
    q = q.order_by(r.desc('pub_date'))

    ### Limit
    q = q.limit(limit)
    q = q.skip(skip)

    cursor = q.run(conn)
    results = list(cursor)
    return results


@app.get('/')
def feed(
    updated: str = Query(
        default=MIN_DATE, regex=date_regex
    ),
    start: str = Query(
        default=MIN_DATE, regex=date_regex
    ),
    end: str = Query(
        default=MAX_DATE, regex=date_regex
    ),
    limit: int = Query(
        default=DEFAULT_LIMIT, ge=0, le=MAX_NUM_ITEMS
    ),
    skip: int = Query(
        default=0, ge=0, le=MAX_NUM_ITEMS
    )
):
    last_updated = to_date(updated)
    start_date = to_date(start)
    end_date = to_date(end)
    data = load(last_updated, start_date, end_date, limit, skip)
    return data
