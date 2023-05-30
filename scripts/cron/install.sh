#!/bin/bash

# NAME
#   install
#
# SYNOPSIS:
#   install.sh
#
# DESCRIPTION:
#   Installs the cron job

# Example of job definition:
# .---------------- minute (0 - 59)
# |  .------------- hour (0 - 23)
# |  |  .---------- day of month (1 - 31)
# |  |  |  .------- month (1 - 12) OR jan,feb,mar,apr ...
# |  |  |  |  .---- day of week (0 - 6) (Sunday=0 or 7) OR sun,mon,tue,wed,thu,fri,sat
# |  |  |  |  |
# *  *  *  *  * user-name  command to be executed

WORKDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# Append to cron jobs: Check PubMed FTP dailyupdates
(crontab -l 2>/dev/null; echo "0 0 * * 0 ${WORKDIR}/cron.sh > ${WORKDIR}/cron.log 2> ${WORKDIR}/cron.err") | uniq - | crontab -
(crontab -l 2>/dev/null; echo "0 1 * * 0 ${WORKDIR}/rethinkdb_dump.sh > ${WORKDIR}/rethinkdb_dump.log 2> ${WORKDIR}/rethinkdb_dump.err") | uniq - | crontab -
