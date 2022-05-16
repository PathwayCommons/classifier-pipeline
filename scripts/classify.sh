#!/bin/bash

## Ben's INDRA output
nohup python pipeline.py --type fetch --idcolumn pmid < ./data/pmid_indra_counts_hasStatement.csv > pmid_indra_counts_hasStatement.out 2>&1 &

### Update files
# nohup python pipeline.py --type download --idcolumn filename < ./data/ftp_updatefiles.csv > ftp_updatefiles.out 2>&1 &