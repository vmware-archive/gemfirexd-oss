#!/bin/sh
. ./gfxdenv.sh
GFXD_SITE=${GFXD_SITE_ctl:-"/s2qa/tangc/workspace/sqlBuild/product-gfxd"}
BUILD_DATE=$1
BUILD=$GFXD_SITE$1
TEST_DIR=${TEST_DIR_ctl:-"/s2qa/tangc/samples/tpce/implementation/0.SetupEnv/logs"}

LOCATOR_ADDR=${LOCATOR_ADDR_ctl:-"10.150.30.37"}
LOCATOR_PORT=${LOCATOR_PORT_ctl:-"7710"}
DISCOVERY_PORT=${DISCOVERY_PORT_ctl:-"12345"}
SRVI_ADDR=${SRVI_ADDR_ctl:-"10.150.30.37"}
SRVI_PORT=${SRVI_PORT_ctl:-"7720"}
INIT_HEAP=${INIT_HEAP_ctl:-"512m"}
MAX_HEAP=${MAX_HEAP_ctl:-"16800m"}

MYSQL_DRIVER=/s2qa/tangc/samples/petclinic/trunk/src/main/resources/db/mysql/mysql-connector-java-5.1.12-bin.jar
ORCL_DRIVER=/s2qa/tangc/ecotest/ojdbc6.jar
#DEBUG_OPTION=-J-Xrunjdwp:transport=dt_socket,server=y,address=8901,suspend=y

BAK_STAMP=`date '+%m-%d-%y-%H_%M_%S'`
echo "Join Time: $BAK_STAMP"

mkdir -p ${TEST_DIR}/gfxdserverI

$BUILD/bin/gfxd server start -initial-heap=${INIT_HEAP} -max-heap=${MAX_HEAP} -dir=${TEST_DIR}/gfxdserverI -classpath=${CLASSPATH} -server-groups=lsg1 -client-bind-address=${SRVI_ADDR}  -client-port=${SRVI_PORT} -mcast-port=0 -host-data=true -locators=${LOCATOR_ADDR}[${DISCOVERY_PORT}] $DEBUG_OPTION
echo "Start ServerI at ${SRVI_ADDR}:${SRVI_PORT}/${LOCATOR_ADDR}[${DISCOVERY_PORT}] -- $?"

exit


