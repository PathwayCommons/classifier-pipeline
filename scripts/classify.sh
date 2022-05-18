#!/bin/bash

set -e

### This is a helper script to run a python script
echo "--"
echo "Starting ${JOB_NAME} within environment ${CONDA_ENV}"

WORKING_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
CONDA_BASE=$(conda info --base)

# stop the old screen session
echo "Quitting old screen session..."
screen -S $JOB_NAME -X -p 0 stuff ^C && echo "Sent ^C" || echo "No screen session to ^C"
screen -S $JOB_NAME -X quit && echo "Quit old screen session" || echo "No screen session to stop"

### start a database if not already
if [[ -z $(docker-compose ps -aq) ]]; then
  echo "Start the database instance"
  docker-compose up -d db
fi

###initialize conda
source ${CONDA_BASE}/etc/profile.d/conda.sh
conda activate ${CONDA_ENV}

### run the script in a new screen session
echo "Starting new screen session..."
screen -d -m -S $JOB_NAME bash -c "python pipeline.py \
                                       --type ${ARG_TYPE} \
                                       --idcolumn ${ARG_IDCOLUMN} \
                                       --table ${ARG_TABLE} \
                                       --threshold ${ARG_THRESHOLD} \
                                   < ${DATA_PATH} 2>&1 | tee ${LOG_PATH}"

