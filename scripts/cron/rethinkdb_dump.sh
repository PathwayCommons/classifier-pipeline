#!/bin/bash

### Dump the rethinkdb database, then copy to host directory

set -e

# Environment variables
WORKING_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
DOCKER="/usr/bin/docker"

DATE=$(date +"%Y_%m_%d_%I_%M_%p")
DB_DUMP_FILE="rethinkdb_dump_${DATE}.tar.gz"
DB_NAME=classifier
DB_WORKDIR=/data

echo "Dump the database ${DB_NAME} to ${WORKING_DIR}"
${DOCKER} exec -t db /bin/bash -c "rethinkdb dump --connect localhost:28015 --export ${DB_NAME} --file ${DB_DUMP_FILE}"
${DOCKER} cp db:${DB_WORKDIR}/${DB_DUMP_FILE} ${WORKING_DIR}

