#!/bin/bash

. ./setenv

S_PATH=$S_HOME/product-gfxd/bin
DISK_DIR=$S_HOME/../../../diskStores
echo $S_PATH

echo "upgrading diskstore locator"
$S_PATH/gfxd upgrade-disk-store SQLF-DEFAULT-DISKSTORE $DISK_DIR/locator1

echo "upgrading diskstore locator datadictionary"
$S_PATH/gfxd upgrade-disk-store SQLF-DD-DISKSTORE $DISK_DIR/locator1/datadictionary

echo "upgrading diskstore server1"
$S_PATH/gfxd upgrade-disk-store SQLF-DEFAULT-DISKSTORE $DISK_DIR/server1

echo "upgrading diskstore server1 datadictionary"
$S_PATH/gfxd upgrade-disk-store SQLF-DD-DISKSTORE $DISK_DIR/server1/datadictionary

echo "upgrading diskstore CONTEXTDISKSTORE in server1"
$S_PATH/gfxd upgrade-disk-store CONTEXTDISKSTORE $DISK_DIR/server1

echo "upgrading diskstore server2"
$S_PATH/gfxd upgrade-disk-store SQLF-DEFAULT-DISKSTORE $DISK_DIR/server2

echo "upgrading diskstore server2 datadictionary"
$S_PATH/gfxd upgrade-disk-store SQLF-DD-DISKSTORE $DISK_DIR/server2/datadictionary

echo "upgrading diskstore CONTEXTDISKSTORE in server2"
$S_PATH/gfxd upgrade-disk-store CONTEXTDISKSTORE $DISK_DIR/server2

echo "upgrading diskstore server3"
$S_PATH/gfxd upgrade-disk-store SQLF-DEFAULT-DISKSTORE $DISK_DIR/server3

echo "upgrading diskstore server3 datadictionary"
$S_PATH/gfxd upgrade-disk-store SQLF-DD-DISKSTORE $DISK_DIR/server3/datadictionary

echo "upgrading diskstore CONTEXTDISKSTORE in server3"
$S_PATH/gfxd upgrade-disk-store CONTEXTDISKSTORE $DISK_DIR/server3

echo "upgrading diskstore server4"
$S_PATH/gfxd upgrade-disk-store SQLF-DEFAULT-DISKSTORE $DISK_DIR/server4

echo "upgrading diskstore server4 datadictionary"
$S_PATH/gfxd upgrade-disk-store SQLF-DD-DISKSTORE $DISK_DIR/server4/datadictionary

echo "upgrading diskstore CONTEXTDISKSTORE in server4"
$S_PATH/gfxd upgrade-disk-store CONTEXTDISKSTORE $DISK_DIR/server4