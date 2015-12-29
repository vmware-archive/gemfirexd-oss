#!/bin/sh

runtest() {
  export GEMFIRE_BUILD=$1
  export GEMFIRE=$GEMFIRE_BUILD/product
  export LD_LIBRARY_PATH=$GEMFIRE/../hidden/lib:$GEMFIRE/lib
  export JTESTS=$GEMFIRE/../tests/classes
  export CLASSPATH=$GEMFIRE/lib/gemfire.jar:$JTESTS
  export JAVA_HOME=/export/gcm/where/jdk/1.6.0_17/x86.linux
  echo "Running $2..."
  echo ""
  $JAVA_HOME/bin/java -server \
    -classpath $CLASSPATH:$GEMFIRE/../product-gfxd/lib/gemfirexd.jar -DGEMFIRE=$GEMFIRE -DJTESTS=$JTESTS \
    -DprovideRegressionSummary=false -DnukeHungTest=true -DmoveRemoteDirs=true \
    -DnumTimesToRun=1 -DtestFileName=$2\
    batterytest.BatteryTest
  echo "Saving test results to $3..."
  echo ""
  mkdir $3
  /bin/mv *-*-* $3
  /bin/mv batterytest.log batterytest.bt oneliner.txt $3
  /bin/cp my.cnf config.ini $3
  /bin/cp $2 $3
}

#-------------------------------------------------------------------------------

if [ -z "$1" ]
then
  echo "No gemfire build was specified."
  exit 0
fi

runtest $1 p2p.gfxd.bt p2p.gfxd
