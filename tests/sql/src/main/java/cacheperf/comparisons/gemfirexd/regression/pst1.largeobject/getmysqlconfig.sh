#!/bin/sh
echo "show variables;" > tmpvars
ssh w1-gst-dev29 /usr/bin/mysql < tmpvars > mysql.config
/bin/rm tmpvars
