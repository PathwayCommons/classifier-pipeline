from classifier_pipeline.db import Db
import pandas as pd

db = Db()
rethink, conn, db, table = db.access_table('documents')


pmcid_map = pd.read_csv('../data/PMC-ids.csv', index_col='PMID', dtype={'PMID': str})

# documents = table.filter(rethink.row['author_list'].filter(lambda author: author['emails'].ne(None) ).count().eq(0)).run(conn)
documents = table.run(conn)

total = 0
count = 0

for doc in documents:
    total += 1
    if doc['pmid'] in pmcid_map.index:
        count += 1

print(f'Total: {total} -- Count: {count}')

