#!/bin/bash

OPT=${1:-cycle}

GFXD=/usr/bin/gfxd
export BIND_ADDRESS=HOSTNAME

GFXD_HOME=/usr/lib/gphd/gfxd
export CLASSPATH=$GFXD_HOME/lib/gemfirexd.jar:$GFXD_HOME/lib/gemfirexd-client.jar

if [ "${OPT}" = "stop" ]; then
  $GFXD server stop -dir=server1
  $GFXD locator stop -dir=locator1
fi


if [ "${OPT}" = "clear" ]; then
  $GFXD server stop -dir=server1
  $GFXD locator stop -dir=locator1
  rm -rf server1; mkdir server1
  rm -rf locator1; mkdir locator1
  hdfs dfs -rm -r /output
  hdfs dfs -rm -r /output_int
fi

if [ "${OPT}" = "start" ]; then
  $GFXD locator start -dir=locator1 -peer-discovery-port=19992 -client-bind-address=$BIND_ADDRESS
  $GFXD server start -dir=server1 -locators=localhost[19992] -client-port=1528 -client-bind-address=$BIND_ADDRESS
fi

if [ "${OPT}" = "load" ]; then
$GFXD <<EOF
connect client '$BIND_ADDRESS:1527';
DROP TABLE IF EXISTS BUSY_AIRPORT;
DROP TABLE IF EXISTS FLIGHTS_HISTORY;
DROP TABLE IF EXISTS MAPS;
DROP TABLE IF EXISTS FLIGHTAVAILABILITY;
DROP TABLE IF EXISTS FLIGHTS;
DROP TABLE IF EXISTS CITIES;
DROP TABLE IF EXISTS COUNTRIES;
DROP TABLE IF EXISTS AIRLINES;
DROP HDFSSTORE IF EXISTS airlines;
EOF

  hadoop fs -rm -r /tmp/gfxd

  $GFXD run -file=create_hdfs_schema.sql -client-bind-address=$BIND_ADDRESS
  $GFXD run -file=fh-10000.sql -client-bind-address=$BIND_ADDRESS
fi

if [ "${OPT}" = "runMR" ]; then
  ./mr.sh ../target/mapreduce-1.0-RELEASE.jar demo.gfxd.mr2.TopBusyAirportGemfirexd /output /tmp/gfxd FLIGHTS_HISTORY
  hdfs dfs -ls /output_int
  $GFXD run -file=mrquery.sql -client-bind-address=$BIND_ADDRESS
fi
