#!/bin/sh
# This is a script to migrate the SQL database with up-to date data or force the database back to certain version
#
# Usage ./update-db.sh

mkdir -p sql_data

aws s3 cp s3://docker-container-data/migrations/ ./sql_data/ --recursive

type=$1
if [ -z "$type" ];
then
    ./migrate -path /sql_data -database "mysql://${MYSQL_USERNAME}:${MYSQL_PASSWORD}@tcp(${MYSQL_HOSTNAME})/${MYSQL_DB_NAME}" -verbose up
else
    ./migrate -path /sql_data -database "mysql://${MYSQL_USERNAME}:${MYSQL_PASSWORD}@tcp(${MYSQL_HOSTNAME})/${MYSQL_DB_NAME}" force ${type}
fi