#!/bin/bash

set -e

OPHDB_LOGIN=root
OPHDB_PWD=$1
OPHDB_HOST=$2
OPHDB_PORT=$3

USER=root
PASSWORD=abcd
HOST=$4
PORT=65000
TYPE=ophidiaio_memory

cleanup() {
	echo "Cleanup"
	mysql -u ${OPHDB_LOGIN} -p${OPHDB_PWD} -h ${OPHDB_HOST} -P ${OPHDB_PORT} ophidiadb -e "UPDATE host SET status='down' WHERE hostname='$HOST';"
}

# Trap the main signals
trap 'cleanup' SIGQUIT
trap 'cleanup' SIGINT
trap 'cleanup' SIGTERM

# Add entries to Ophidia DB
mysql -u ${OPHDB_LOGIN} -p${OPHDB_PWD} -h ${OPHDB_HOST} -P ${OPHDB_PORT} ophidiadb -e "INSERT INTO host (hostname, cores, memory) SELECT '$HOST', 4, 1 FROM DUAL WHERE NOT EXISTS (SELECT hostname FROM host WHERE hostname='$HOST') LIMIT 1;"
mysql -u ${OPHDB_LOGIN} -p${OPHDB_PWD} -h ${OPHDB_HOST} -P ${OPHDB_PORT} ophidiadb -e "INSERT IGNORE INTO hashost (idhostpartition, idhost) SELECT 1, host.idhost FROM host WHERE hostname='$HOST';"
mysql -u ${OPHDB_LOGIN} -p${OPHDB_PWD} -h ${OPHDB_HOST} -P ${OPHDB_PORT} ophidiadb -e "INSERT INTO dbmsinstance (idhost, login, password, port, ioservertype) SELECT host.idhost, '$USER', '$PASSWORD', $PORT, '$TYPE' FROM host WHERE hostname='$HOST' AND NOT EXISTS (SELECT * FROM dbmsinstance INNER JOIN host ON host.idhost = dbmsinstance.idhost WHERE hostname='$HOST' AND port=$PORT) LIMIT 1;"
mysql -u ${OPHDB_LOGIN} -p${OPHDB_PWD} -h ${OPHDB_HOST} -P ${OPHDB_PORT} ophidiadb -e "UPDATE host SET status='up' WHERE hostname='$HOST';"

# Start the server
/usr/local/ophidia/oph-cluster/oph-io-server/bin/oph_io_server >>/var/log/oph_io_server.log 2>>/var/log/oph_io_server.log </dev/null &

# Wait for a final signal
wait $!

# Cleanup
cleanup

