#!/bin/sh

runtest() {
  export GEMFIRE_BUILD=$1
  export GEMFIRE=$GEMFIRE_BUILD/product
  export LD_LIBRARY_PATH=$GEMFIRE/../hidden/lib:$GEMFIRE/lib
  export JTESTS=$GEMFIRE/../tests/classes
  export CLASSPATH=$GEMFIRE/lib/gemfire.jar:$JTESTS
  #export JAVA_HOME=/export/gcm/where/java/jrockit/1.6.0_17/x86.linux
  export JAVA_HOME=/export/gcm/where/jdk/1.6.0_17/x86.linux
  echo ""
  echo "Running $JTESTS/cacheperf/poc/useCase3_2/useCase3.bt with $JTESTS/cacheperf/poc/useCase3_2/local.conf..."
  echo ""
  $JAVA_HOME/bin/java -server \
    -classpath $CLASSPATH -DGEMFIRE=$GEMFIRE -DJTESTS=$JTESTS \
    -DprovideRegressionSummary=false -DnukeHungTest=true -DmoveRemoteDirs=true \
    -DtestFileName=$JTESTS/cacheperf/poc/useCase3_2/useCase3.bt \
    -DlocalConf=$JTESTS/cacheperf/poc/useCase3_2/local.conf \
    batterytest.BatteryTest
}

#-------------------------------------------------------------------------------

if [ -z "$1" ]
then
  echo "No gemfire build was specified."
  exit 0
fi

runtest $1
