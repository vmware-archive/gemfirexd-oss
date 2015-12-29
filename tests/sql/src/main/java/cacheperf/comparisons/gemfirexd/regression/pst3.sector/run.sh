#!/bin/sh

trap 'exit 1' 2 #traps Ctrl-C (signal 2)

runtest() {
  export GEMFIRE_BUILD=$1
  export GEMFIRE=$GEMFIRE_BUILD/product
  export LD_LIBRARY_PATH=$GEMFIRE/../hidden/lib:$GEMFIRE/lib
  export JTESTS=$GEMFIRE/../tests/classes
  export CLASSPATH=$GEMFIRE/lib/gemfire.jar:$JTESTS
  export JAVA_HOME=/export/gcm/where/jdk/1.7.0_72/x86_64.linux
  echo "Running $2 with $3..."
  echo ""
  $JAVA_HOME/bin/java -server \
    -classpath $CLASSPATH:$GEMFIRE/../product-gfxd/lib/gemfirexd.jar -DGEMFIRE=$GEMFIRE -DJTESTS=$JTESTS \
    -DprovideRegressionSummary=false -DnukeHungTest=true -DmoveRemoteDirs=true \
    -DnumTimesToRun=1 -DtestFileName=$2 -DlocalConf=$3 \
    batterytest.BatteryTest
  echo "Saving test results to $4..."
  echo ""
  mkdir $4
  /bin/mv *-*-* $4
  /bin/mv batterytest.log batterytest.bt oneliner.txt $4
  /bin/cp $2 $3 $4
}

#-------------------------------------------------------------------------------

if [ -z "$1" ]
then
  echo "No gemfire build was specified."
  exit 0
fi

### RUN GFXD TESTS
#runtest $1 2.bt  local.gfxd.conf  2.thin
#runtest $1 3.bt  local.gfxd.conf  3.thin
#runtest $1 4.bt  local.gfxd.conf  4.thin
runtest $1 5.bt  local.gfxd.conf  pst3.sector.thin.5.$2
