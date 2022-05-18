#!/bin/bash

# Download from PubMed and classify

WORKING_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

export CONDA_ENV="flow"
export JOB_NAME="indra-classification"

# pipeline.py arguments
export ARG_TYPE="fetch"
export ARG_IDCOLUMN="pmid"
export ARG_TABLE="indra"
export ARG_THRESHOLD="0.990"

# data file
DATA_DIR="data"
DATA_FILE="pmid_indra_counts_hasStatement_lite.csv"
LOG_FILE="pmid_indra_counts_hasStatement.log"
export DATA_PATH="${WORKING_DIR}/${DATA_DIR}/${DATA_FILE}"
export LOG_PATH="${WORKING_DIR}/${DATA_DIR}/${LOG_FILE}"

./classify.sh