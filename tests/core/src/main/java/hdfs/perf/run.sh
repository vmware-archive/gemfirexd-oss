#!/bin/sh

#-------------------------------------------------------------------------------
# SUPPLY YOUR BUILD HERE
#-------------------------------------------------------------------------------

export GEMFIRE_BUILD=/export/hs21a1/users/lhughes/cedarSanc

#-------------------------------------------------------------------------------
# SUPPLY EDITED VERSIONS OF LOCAL.CONF FILES HERE
#-------------------------------------------------------------------------------

export LOCAL_CONF_DIR=/export/hs21b1/users/lhughes/hdfsperf

#-------------------------------------------------------------------------------
# SET ENVIRONMENT
#-------------------------------------------------------------------------------

export GEMFIRE=$GEMFIRE_BUILD/product
export JTESTS=$GEMFIRE/../tests/classes
export LD_LIBRARY_PATH=$GEMFIRE/../hidden/lib:$GEMFIRE/lib
export CLASSPATH=$GEMFIRE/lib/gemfire.jar:$JTESTS
export JAVA_HOME=/export/gcm/where/jdk/1.6.0_26/x86_64.linux

#-------------------------------------------------------------------------------
# partitionedPeer.bt with local.hdfsperf.conf
#-------------------------------------------------------------------------------

echo ""
echo "Running $LOCAL_CONF_DIR/partitionedPeer.bt with $LOCAL_CONF_DIR/local.hdfsperf.conf..."
$JAVA_HOME/bin/java -server \
  -classpath $CLASSPATH -DGEMFIRE=$GEMFIRE -DJTESTS=$JTESTS \
  -DprovideRegressionSummary=false -DnukeHungTest=true -DmoveRemoteDirs=true \
  -DtestFileName=$LOCAL_CONF_DIR/partitionedPeer.bt \
  -DlocalConf=$LOCAL_CONF_DIR/local.hdfsperf.conf \
  batterytest.BatteryTest

