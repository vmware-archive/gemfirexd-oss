#!/bin/bash

runtest() {
  export GEMFIRE_BUILD=$1
  export GEMFIRE=$GEMFIRE_BUILD/product
  export LD_LIBRARY_PATH=$GEMFIRE/../hidden/lib:$GEMFIRE/lib
  export JTESTS=$GEMFIRE/../tests/classes
  export CLASSPATH=$GEMFIRE/lib/gemfire.jar:$JTESTS:$GEMFIRE/lib/snappy-java-1.0.4.1.jar
  export JAVA_HOME=/export/gcm/where/jdk/1.7.0_72/x86_64.linux
  echo ""
  echo "Running $JTESTS/smoketest/scale/scale.bt with $JTESTS/smoketest/scale/local.conf..."
  echo ""
  $JAVA_HOME/bin/java -server \
    -classpath $CLASSPATH -DGEMFIRE=$GEMFIRE -DJTESTS=$JTESTS \
    -DprovideRegressionSummary=false -DnukeHungTest=true -DmoveRemoteDirs=true \
    -DtestFileName=$JTESTS/smoketest/scale/scale.bt \
    -DlocalConf=$JTESTS/smoketest/scale/local.conf \
    batterytest.BatteryTest
}

#-------------------------------------------------------------------------------

trap 'exit 1' 2 #traps Ctrl-C (signal 2)

if [ -z "$1" ]
then
  echo "No gemfire build was specified."
  exit 0
fi

runtest $1
