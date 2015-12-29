#!/bin/sh
#
#-------------------------------------------------------------------------------
# SUPPLY YOUR BUILD HERE
#-------------------------------------------------------------------------------

export GEMFIRE_BUILD=<path_to_build>

#-------------------------------------------------------------------------------
# SUPPLY EDITED VERSIONS OF LOCAL.CONF FILES HERE
#-------------------------------------------------------------------------------

export LOCAL_CONF_DIR=<path_to_edited_local_conf_files>

#-------------------------------------------------------------------------------
# SET ENVIRONMENT
#-------------------------------------------------------------------------------

export GEMFIRE=$GEMFIRE_BUILD/product
export LD_LIBRARY_PATH=$GEMFIRE/../hidden/lib:$GEMFIRE/lib
export CLASSPATH=$GEMFIRE/lib/gemfire.jar:$GEMFIRE/../tests/classes
export JAVA_HOME=/export/gcm/where/jdk/1.6.0_17/x86_64.linux
export JTESTS=$GEMFIRE/../tests/classes

#-------------------------------------------------------------------------------
# scale64.bt
#-------------------------------------------------------------------------------

echo ""
echo "Running scale64.bt using local.6hosts64bit.conf"

$JAVA_HOME/bin/java -server \
  -classpath $CLASSPATH -DGEMFIRE=$GEMFIRE -DJTESTS=$JTESTS \
  -DmasterHeapMB=3000 -DnukeHungTest=true -DmoveRemoteDirs=true \
  -DlocalConf=$LOCAL_CONF_DIR/local.6hosts64bit.conf \
  -DtestFileName=$JTESTS/scale64/scale64.bt \
  batterytest.BatteryTest

#-------------------------------------------------------------------------------
# scalewan64.bt
#-------------------------------------------------------------------------------

echo "Running scalewan64.bt using local.6hosts64bitwan.conf"

$JAVA_HOME/bin/java -server \
  -classpath $CLASSPATH -DGEMFIRE=$GEMFIRE -DJTESTS=$GEMFIRE/../tests/classes \
  -DmasterHeapMB=3000 -DnukeHungTest=true -DmoveRemoteDirs=true \
  -DlocalConf=$LOCAL_CONF_DIR/local.6hosts64bitwan.conf \
  -DtestFileName=$JTESTS/scale64/scalewan64.bt \
  batterytest.BatteryTest
