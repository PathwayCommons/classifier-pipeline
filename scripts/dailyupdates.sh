#!/bin/bash

# Use this script to run the classifier pipeline

# Local variables
WORKING_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
DATA_DIR="data"
DATA_FILE="ftp_updatefiles.csv"
LOG_FILE="ftp_updatefiles.log"

# Environment variables
set -a
    JOB_NAME="pubmed_dailyupdates_classification"
    CONDA_ENV="pipeline"

    # pipeline.py script arguments
    ARG_TYPE="download"
    ARG_IDCOLUMN="filename"
    ARG_TABLE="pubmed"
    ARG_THRESHOLD="0.990"

    # source data and logging output
    DATA_PATH="${WORKING_DIR}/${DATA_DIR}/${DATA_FILE}"
    LOG_PATH="${WORKING_DIR}/${DATA_DIR}/${LOG_FILE}"
set +a

./classify.sh