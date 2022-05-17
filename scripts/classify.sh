#!/bin/bash

# This is a helper script to run a python script

echo "--"
echo "Starting $JOB_NAME"
date

WORKING_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# stop the old screen session
echo "Quitting old screen session..."
# screen -S $JOB_NAME -X -p 0 stuff ^C && echo "Sent ^C" || echo "No screen session to ^C"
# screen -S $JOB_NAME -X quit && echo "Quit old screen session" || echo "No screen session to stop"

# swap out old workspace with new one
# start the server in a screen session
echo "Starting new screen session..."
# screen -d -m -S $JOB_NAME bash -c "npm run ${START_SCRIPT:-start} 2>&1 | tee ~/$JOB_NAME.screen.log"
echo "New screen session started"

echo "CI script complete"


# python pipeline.py --type fetch --idcolumn pmid --table indra --threshold 0.990 < ./data/pmid_indra_counts_hasStatement.csv > pmid_indra_counts_hasStatement.out 2>&1

## Ben's INDRA output
# nohup python pipeline.py --type fetch --idcolumn pmid --table indra --threshold 0.990 < ./data/pmid_indra_counts_hasStatement.csv > pmid_indra_counts_hasStatement.out 2>&1 &

### Update files
# nohup python pipeline.py --type download --idcolumn filename --table updatefiles --threshold 0.990 < ./data/ftp_updatefiles.csv > ftp_updatefiles.out 2>&1 &