#!/bin/sh

ssh hs21b sudo /etc/init.d/mysql stop
ssh hs21c sudo /etc/init.d/mysql stop
ssh hs21d sudo /etc/init.d/mysql stop
ssh hs21e sudo /etc/init.d/mysql stop
/usr/bin/ndb_mgm -e shutdown
