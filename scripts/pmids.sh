#!/bin/bash

# Use this script to run the classifier pipeline

# Local variables
WORKING_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
DATA_DIR="data"
DATA_FILE="pmid_indra_counts_hasStatement_lite.csv"
LOG_FILE="pmid_indra_counts_hasStatement.log"

# Environment variables
set -a
    JOB_NAME="indra_classification"
    CONDA_ENV="pipeline"

    # pipeline.py script arguments
    ARG_TYPE="fetch"
    ARG_IDCOLUMN="pmid"
    ARG_TABLE="documents"
    ARG_THRESHOLD="0.990"

    # source data and logging output
    DATA_PATH="${WORKING_DIR}/${DATA_DIR}/${DATA_FILE}"
    LOG_PATH="${WORKING_DIR}/${DATA_DIR}/${LOG_FILE}"
set +a

./classify.sh