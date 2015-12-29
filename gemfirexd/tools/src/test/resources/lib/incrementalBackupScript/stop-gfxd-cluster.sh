#!/bin/bash

. ./setenv

D_ADDRESS=`uname -n`
D_PORT=10101
MCAST_PORT=10334
LOCATOR="$D_ADDRESS[$D_PORT]"

S_PATH=$S_HOME/product-gfxd/bin
DISK_DIR=$S_HOME/../../../diskStores

echo "shut-down-all servers..."
$S_PATH/gfxd shut-down-all -locators=$LOCATOR
#$S_PATH/gfxd shut-down-all -mcast-port=$MCAST_PORT

echo "stop locator..."
$S_PATH/gfxd locator stop -dir=$DISK_DIR/locator1

