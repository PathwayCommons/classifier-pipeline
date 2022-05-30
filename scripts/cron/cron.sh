#!/bin/bash
# Run this script in a cron job to analyze PubMed daily updates

set -e

# Local variables
JOB_NAME="cron"
CONDA_ENV="pipeline"

# Environment variables
set -a
    LOGURU_LEVEL="INFO"
    WORKING_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
    CONDA_BASE="/home/baderlab/miniconda3"
    COMPOSE="/usr/local/bin/docker-compose --file ${WORKING_DIR}/../../docker-compose.yml"
set +a

echo "--"
echo "Starting ${JOB_NAME} within environment ${CONDA_ENV}"

# stop the old screen session
echo "Quitting old screen session..."
screen -S $JOB_NAME -X -p 0 stuff ^C && echo "Sent ^C" || echo "No screen session to ^C"
screen -S $JOB_NAME -X quit && echo "Quit old screen session" || echo "No screen session to stop"

### start a database if not already
if [[ -z $(${COMPOSE} ps -aq) ]]; then
  echo "Start the database instance"
  ${COMPOSE} up -d db
fi

###initialize conda
source ${CONDA_BASE}/etc/profile.d/conda.sh
conda activate ${CONDA_ENV}

### run the script in a new screen session
cd $WORKING_DIR
echo "Starting new screen session..."
### stderr passes through, stdout thrown away (python logs to file)
screen -d -m -S $JOB_NAME bash -c "python cron.py 2>&1 | tee > /dev/null"
echo "New screen session created successfully"

