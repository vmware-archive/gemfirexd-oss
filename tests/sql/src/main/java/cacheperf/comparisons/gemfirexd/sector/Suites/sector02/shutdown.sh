#!/bin/sh

ssh hs21b sudo /etc/init.d/mysql stop
/usr/bin/ndb_mgm -e shutdown
