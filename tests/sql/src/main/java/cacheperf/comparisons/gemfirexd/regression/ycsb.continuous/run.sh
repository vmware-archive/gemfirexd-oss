#!/bin/sh

trap 'exit 1' 2 #traps Ctrl-C (signal 2)

runtest() {
  export GEMFIRE_BUILD=$1
  export GEMFIRE=$GEMFIRE_BUILD/product
  export LD_LIBRARY_PATH=$GEMFIRE/../hidden/lib:$GEMFIRE/lib
  export JTESTS=$GEMFIRE/../tests/classes
  export CLASSPATH=$GEMFIRE/lib/gemfire.jar:$JTESTS
  export JAVA_HOME=/export/gcm/where/jdk/1.7.0_72/x86_64.linux
  echo "Running ycsb continuous..."
  echo ""
  $JAVA_HOME/bin/java -server \
    -classpath $CLASSPATH:$GEMFIRE/../product-gfxd/lib/gemfirexd.jar -DGEMFIRE=$GEMFIRE -DJTESTS=$JTESTS \
    -DprovideRegressionSummary=false -DnukeHungTest=true -DmoveRemoteDirs=true \
    -DnumTimesToRun=1 -DtestFileName=$JTESTS/gfxdperf/ycsb/gfxd/continuous/continuous.bt -DlocalConf=$JTESTS/gfxdperf/ycsb/gfxd/continuous/continuous.local.conf \
    batterytest.BatteryTest
  echo "Saving test results to $2..."
  echo ""
  mkdir $2
  /bin/mv *-*-* $2
  /bin/mv batterytest.log batterytest.bt oneliner.txt $2
  /bin/cp $2
}

#-------------------------------------------------------------------------------

if [ -z "$1" ]
then
  echo "No gemfire build was specified."
  exit 0
fi

### RUN TESTS

runtest $1 ycsb.continuous.$2
