#!/bin/sh
echo "show variables;" > tmpvars
/usr/bin/mysql < tmpvars > mysql.config
/bin/rm tmpvars
