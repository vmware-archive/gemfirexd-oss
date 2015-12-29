#!/bin/sh

startup() {

  echo "Copying $PWD/my.cnf to w1-gst-dev29"
  scp my.cnf lises@w1-gst-dev29:/etc/my.cnf
  echo "Starting MySQL on w1-gst-dev29 from $1"

  ssh w1-gst-dev29 sudo /etc/init.d/mysqld start
}

startup $PWD
