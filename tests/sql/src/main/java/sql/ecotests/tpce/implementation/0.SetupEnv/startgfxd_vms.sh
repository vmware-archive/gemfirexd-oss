#!/bin/sh
. ./gfxdenv.sh
GFXD_SITE=${GFXD_SITE_ctl:-"/s2qa/tangc/workspace/sqlBuild/product-gfxd"}
BUILD_DATE=$1
BUILD=$GFXD_SITE$1
TEST_DIR=${TEST_DIR_ctl:-"/s2qa/tangc/samples/tpce/implementation/0.SetupEnv/logs"}

LOCATOR_ADDR=${LOCATOR_ADDR_ctl:-"10.150.30.36"}
LOCATOR_PORT=${LOCATOR_PORT_ctl:-"7710"}
DISCOVERY_PORT=${DISCOVERY_PORT_ctl:-"12345"}
SRV1_ADDR=${SRV1_ADDR_ctl:-"10.150.30.36"}
SRV1_PORT=${SRV1_PORT_ctl:-"7711"}
SRV2_ADDR=${SRV2_ADDR_ctl:-"10.150.30.37"}
SRV2_PORT=${SRV2_PORT_ctl:-"7712"}
SRV3_ADDR=${SRV3_ADDR_ctl:-"10.150.30.38"}
SRV3_PORT=${SRV3_PORT_ctl:-"7713"}
SRV4_ADDR=${SRV4_ADDR_ctl:-"10.150.30.39"}
SRV4_PORT=${SRV4_PORT_ctl:-"7714"}
INIT_HEAP=${INIT_HEAP_ctl:-"512m"}
MAX_HEAP=${MAX_HEAP_ctl:-"16800m"}

MYSQL_DRIVER=/s2qa/tangc/samples/petclinic/trunk/src/main/resources/db/mysql/mysql-connector-java-5.1.12-bin.jar
ORCL_DRIVER=/s2qa/tangc/ecotest/ojdbc6.jar
#-J-Xrunjdwp:transport=dt_socket,server=y,address=8901,suspend=y
JAVA_AGENT_OPT=-J-javaagent:$BUILD/lib/gemfirexd.jar
MARTET_SRVGROUP=server-groups=MarketGroup
CUSTOMER_SRVGROUP=server-groups=CustomerGroup

BAK_STAMP=`date '+%m-%d-%y-%H_%M_%S'`
echo "Start Time: $BAK_STAMP"
for (( i = 0 ; i <= 4; i++ ))
do
  mkdir -p ${TEST_DIR}/gfxdserver$i
done

START_LOCATOR="$BUILD/bin/gfxd locator start -initial-heap=256m -max-heap=1024m -dir=${TEST_DIR}/gfxdserver0  -classpath=${CLASSPATH} -client-port=${LOCATOR_PORT} -client-bind-address=${LOCATOR_ADDR} -mcast-port=0 -peer-discovery-address=${LOCATOR_ADDR} -peer-discovery-port=${DISCOVERY_PORT} ${JAVA_AGENT_OPT} &"
eval $START_LOCATOR
#ssh -l tangc -n ${LOCATOR_ADDR} ${START_LOCATOR}
echo "Start Locator at ${LOCATOR_ADDR}: $?"

sleep 20


START_SRV1="$BUILD/bin/gfxd server start -initial-heap=${INIT_HEAP} -max-heap=${MAX_HEAP} -dir=${TEST_DIR}/gfxdserver1 -classpath=${CLASSPATH} -server-groups=CustomerGroup -client-bind-address=${SRV1_ADDR} -client-port=${SRV1_PORT} -mcast-port=0 -host-data=true -locators=${LOCATOR_ADDR}[${DISCOVERY_PORT}] ${JAVA_AGENT_OPT} &"
ssh -l tangc -n ${SRV1_ADDR} ${START_SRV1}
#eval $START_SRV1
echo
echo "Start Server1 at ${SRV1_ADDR}:${SRV1_PORT}/${LOCATOR_ADDR}[${DISCOVERY_PORT}]-- $?"


START_SRV2="$BUILD/bin/gfxd server start -initial-heap=${INIT_HEAP} -max-heap=${MAX_HEAP} -dir=${TEST_DIR}/gfxdserver2 -classpath=${CLASSPATH} -server-groups=CustomerGroup -client-bind-address=${SRV2_ADDR}  -client-port=${SRV2_PORT} -mcast-port=0 -host-data=true -locators=${LOCATOR_ADDR}[${DISCOVERY_PORT}] ${JAVA_AGENT_OPT} &"
#eval $START_SRV2
ssh -l tangc -n ${SRV2_ADDR} ${START_SRV2}
echo "Start Server2 at ${SRV2_ADDR}:${SRV2_PORT}/${LOCATOR_ADDR}[${DISCOVERY_PORT}] -- $?"

START_SRV3="$BUILD/bin/gfxd server start -initial-heap=${INIT_HEAP} -max-heap=${MAX_HEAP} -dir=${TEST_DIR}/gfxdserver3 -classpath=${CLASSPATH} -server-groups=MarketGroup -client-bind-address=${SRV3_ADDR} -client-port=${SRV3_PORT} -mcast-port=0 -host-data=true -locators=${LOCATOR_ADDR}[${DISCOVERY_PORT}] ${JAVA_AGENT_OPT} &"
#eval $START_SRV3
ssh -l tangc -n ${SRV3_ADDR} ${START_SRV3}
echo "Start Server3 at ${SRV3_ADDR}:${SRV3_PORT}/${LOCATOR_ADDR}[${DISCOVERY_PORT}]-- $?"

START_SRV4="$BUILD/bin/gfxd server start -initial-heap=${INIT_HEAP} -max-heap=${MAX_HEAP} -dir=${TEST_DIR}/gfxdserver4 -classpath=${CLASSPATH} -server-groups=MarketGroup -client-bind-address=${SRV4_ADDR}  -client-port=${SRV4_PORT} -mcast-port=0 -host-data=true -locators=${LOCATOR_ADDR}[${DISCOVERY_PORT}] ${JAVA_AGENT_OPT} &"
#eval $START_SRV4
ssh -l tangc -n ${SRV4_ADDR} ${START_SRV4}
echo "Start Server4 at ${SRV4_ADDR}:${SRV4_PORT}/${LOCATOR_ADDR}[${DISCOVERY_PORT}] -- $?"

exit


