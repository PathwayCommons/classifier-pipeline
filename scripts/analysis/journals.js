// Group by journal and count
r.db('classifier')
  .table('documents')
  .group( r.row('journal')('title') )
  .count()
