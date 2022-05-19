## dump the database
#!/bin/bash

### Dump the rethinkdb database, then copy to host directory

set -e

echo "Dump the database"

WORKING_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
DATE=$(date +"%Y_%m_%d_%I_%M_%p")
DB_DUMP_FILE="rethinkdb_dump__${DATE}.tar.gz"
DB_NAME=classifier
DB_WORKDIR=/data
docker exec -it db /bin/bash -c "rethinkdb dump --connect localhost:28015 \
                                             --export ${DB_NAME} \
                                             --file ${DB_DUMP_FILE}"
docker cp db:${DB_WORKDIR}/${DB_DUMP_FILE} ${WORKING_DIR}

