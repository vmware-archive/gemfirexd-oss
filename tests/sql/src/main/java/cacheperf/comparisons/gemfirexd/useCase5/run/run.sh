#!/bin/bash

trap 'exit 1' 2 #traps Ctrl-C (signal 2)

#-------------------------------------------------------------------------------

runtest() {
  export GEMFIRE_BUILD=$1
  export GEMFIRE=$GEMFIRE_BUILD/product
  export LD_LIBRARY_PATH=$GEMFIRE/../hidden/lib:$GEMFIRE/lib
  export JTESTS=$GEMFIRE/../tests/classes
  export CLASSPATH=$GEMFIRE/../product-gfxd/lib/gemfirexd.jar:$GEMFIRE/lib/gemfire.jar:$JTESTS
  export JAVA_HOME=/export/gcm/where/jdk/1.6.0_26/x86_64.linux
  echo "Running $2 using $3..."
  echo ""

  $JAVA_HOME/bin/java -server \
    -classpath $CLASSPATH -DGEMFIRE=$GEMFIRE -DJTESTS=$JTESTS \
    -DprovideRegressionSummary=false -DnukeHungTest=true -DmoveRemoteDirs=true \
    -DnumTimesToRun=1 -DtestFileName=$2 -DlocalConf=$3 \
    batterytest.BatteryTest
}

#-------------------------------------------------------------------------------

if [ -z "$1" ]
then
  echo "No gemfire build was specified."
  exit 0
fi

runtest $1 insertTicket.bt insertTicket.local.conf
