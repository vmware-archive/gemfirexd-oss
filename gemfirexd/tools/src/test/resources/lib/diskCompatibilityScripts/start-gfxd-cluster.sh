#!/bin/bash

. ./setenv

# This creates 1 locator and 4 data nodes (each with 4GB memory) 

D_ADDRESS=`uname -n`
D_PORT=10101

C_BIND=`uname -n`
C_PORT0="1527"
C_PORT1="1528"
C_PORT2="1529"
C_PORT3="1530"
C_PORT4="1531"
LOCATOR="$D_ADDRESS[$D_PORT]"
DIR=`pwd`
GROUP="SPC_REP,SPC_PAR,SPC_WAN,FDC_REP,FDC_PAR,TEST"

S_PATH=$S_HOME/product-gfxd/bin
DISK_DIR=$S_HOME/../../../diskStores

XMN="1G"
XSS="256K"
MAXHEAP="4G"
JVM_OPTS="-J-Xmn$XMN -J-Xss$XSS -J-XX:+UseConcMarkSweepGC -J-XX:PermSize=192M -J-XX:MaxPermSize=192M -J-XX:+OptimizeStringConcat -J-XX:+UseCMSInitiatingOccupancyOnly -J-XX:CMSInitiatingOccupancyFraction=60"

MEM_OPTS="-J-javaagent:"$S_PATH/../lib/gemfirexd.jar
JVM_OPTS=$JVM_OPTS" "$MEM_OPTS

# Locator
echo "Starting locator " 
#mkdir -p locator1
$S_PATH/sqlf locator start $JVM_OPTS -peer-discovery-address=$D_ADDRESS -peer-discovery-port=$D_PORT -dir=$DISK_DIR/locator1 -log-file=$DISK_DIR/locator1/locator.log -J-Dgemfire.statistic-archive-file=$DISK_DIR/locator1/locator.gfs -client-bind-address=$C_BIND -client-port=$C_PORT0 -jmx-manager=true -jmx-manager-start=true -sqlfire.properties=$DIR/sqlfire.properties
pid1=$!
wait $pid1

# servers
#server=`ps -ef |grep java | grep "server1.log" | awk '{print $2}'`

echo "Starting server1"
#mkdir -p server1 
$S_PATH/sqlf server start $JVM_OPTS -heap-size=$MAXHEAP -critical-heap-percentage=90 -eviction-heap-percentage=80 -server-groups=$GROUP -locators=$LOCATOR -client-bind-address=$C_BIND -client-port=$C_PORT1 -dir=$DISK_DIR/server1 -log-file=$DISK_DIR/server1/server.log -J-Dgemfire.statistic-archive-file=$DISK_DIR/server1/stat.gfs -sync=false -sqlfire.properties=$DIR/sqlfire.properties
pid2=$!
wait $pid2


echo "Starting server2"
#mkdir -p server2
$S_PATH/sqlf server start $JVM_OPTS -heap-size=$MAXHEAP -critical-heap-percentage=90 -eviction-heap-percentage=80 -server-groups=$GROUP -locators=$LOCATOR -client-bind-address=$C_BIND -client-port=$C_PORT2 -dir=$DISK_DIR/server2 -log-file=$DISK_DIR/server2/server.log -J-Dgemfire.statistic-archive-file=$DISK_DIR/server2/stat.gfs -sync=false -sqlfire.properties=$DIR/sqlfire.properties
pid3=$!
wait $pid3


echo "Starting server3"
#mkdir -p server3
$S_PATH/sqlf server start $JVM_OPTS -heap-size=$MAXHEAP -critical-heap-percentage=90 -eviction-heap-percentage=80 -server-groups=$GROUP -locators=$LOCATOR -client-bind-address=$C_BIND -client-port=$C_PORT3 -dir=$DISK_DIR/server3 -log-file=$DISK_DIR/server3/server.log -J-Dgemfire.statistic-archive-file=$DISK_DIR/server3/stat.gfs -sync=false -sqlfire.properties=$DIR/sqlfire.properties
pid4=$!
wait $pid4


echo "Starting server4"
#mkdir -p server4
$S_PATH/sqlf server start $JVM_OPTS -heap-size=$MAXHEAP -critical-heap-percentage=90 -eviction-heap-percentage=80 -server-groups=$GROUP -locators=$LOCATOR -client-bind-address=$C_BIND -client-port=$C_PORT4 -dir=$DISK_DIR/server4 -log-file=$DISK_DIR/server4/server.log -J-Dgemfire.statistic-archive-file=$DISK_DIR/server4/stat.gfs -sync=false -sqlfire.properties=$DIR/sqlfire.properties
pid5=$!
wait $pid5



