#!/bin/sh

startup() {

  echo "Starting MySQL Cluster from $1"

  sudo /usr/sbin/ndb_mgmd -f $1/config.ini --initial

  ssh hs21b sudo /usr/sbin/ndbmtd -c 'host=hs21a.dc.gemstone.com' --initial

  /bin/cp $1/my.cnf /home/lises/.my.cnf
  ssh hs21b sudo /etc/init.d/mysql start --new
}

startup $PWD
