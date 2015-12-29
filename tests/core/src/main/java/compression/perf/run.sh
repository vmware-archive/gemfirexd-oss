#!/bin/sh

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
export JTESTS=$GEMFIRE/../tests/classes
export LD_LIBRARY_PATH=$GEMFIRE/../hidden/lib:$GEMFIRE/lib
export CLASSPATH=$GEMFIRE/lib/gemfire.jar:$JTESTS:$GEMFIRE/lib/snappy-java-1.0.4.1.jar
export JAVA_HOME=/export/gcm/where/jdk/1.6.0_26/x86_64.linux

#-------------------------------------------------------------------------------
# compression/perf/replicated.bt (without compression)
#-------------------------------------------------------------------------------

echo ""
echo "Running $JTESTS/compression/perf/replicated.bt with $JTESTS/compression/perf/local.perf.conf..."
$JAVA_HOME/bin/java -server \
  -classpath $CLASSPATH -DGEMFIRE=$GEMFIRE -DJTESTS=$JTESTS \
  -DprovideRegressionSummary=false -DnukeHungTest=true -DmoveRemoteDirs=true \
  -DtestFileName=$JTESTS/compression/perf/replicated.bt \
  -DlocalConf=$LOCAL_CONF_DIR/local.perf.conf \
  batterytest.BatteryTest

#-------------------------------------------------------------------------------
# compression/perf/replicated.bt (with compression)
#-------------------------------------------------------------------------------

echo ""
echo "Running $JTESTS/compression/perf/replicated.bt with $JTESTS/compression/perf/local.compression.conf..."
$JAVA_HOME/bin/java -server \
  -classpath $CLASSPATH -DGEMFIRE=$GEMFIRE -DJTESTS=$JTESTS \
  -DprovideRegressionSummary=false -DnukeHungTest=true -DmoveRemoteDirs=true \
  -DtestFileName=$JTESTS/compression/perf/replicated.bt \
  -DlocalConf=$LOCAL_CONF_DIR/local.compression.conf \
  batterytest.BatteryTest

#-------------------------------------------------------------------------------
# compression/perf/partitioned.bt (without compression)
#-------------------------------------------------------------------------------

echo ""
echo "Running $JTESTS/compression/perf/partitioned.bt with $JTESTS/compression/perf/local.perf.conf..."
$JAVA_HOME/bin/java -server \
  -classpath $CLASSPATH -DGEMFIRE=$GEMFIRE -DJTESTS=$JTESTS \
  -DprovideRegressionSummary=false -DnukeHungTest=true -DmoveRemoteDirs=true \
  -DtestFileName=$JTESTS/compression/perf/partitioned.bt \
  -DlocalConf=$LOCAL_CONF_DIR/local.perf.conf \
  batterytest.BatteryTest

#-------------------------------------------------------------------------------
# compression/perf/partitioned.bt (with compression)
#-------------------------------------------------------------------------------

echo ""
echo "Running $JTESTS/compression/perf/partitioned.bt with $JTESTS/compression/perf/local.compression.conf..."
$JAVA_HOME/bin/java -server \
  -classpath $CLASSPATH -DGEMFIRE=$GEMFIRE -DJTESTS=$JTESTS \
  -DprovideRegressionSummary=false -DnukeHungTest=true -DmoveRemoteDirs=true \
  -DtestFileName=$JTESTS/compression/perf/partitioned.bt \
  -DlocalConf=$LOCAL_CONF_DIR/local.compression.conf \
  batterytest.BatteryTest
