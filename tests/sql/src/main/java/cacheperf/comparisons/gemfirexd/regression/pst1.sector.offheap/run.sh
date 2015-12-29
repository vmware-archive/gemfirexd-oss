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
  /bin/cp my.cnf $4
  /bin/cp mysql.config $4
  /bin/cp $2 $3 $4
}

#-------------------------------------------------------------------------------

if [ -z "$1" ]
then
  echo "No gemfire build was specified."
  exit 0
fi

### RUN GFXD TESTS
runtest $1 pst1.gfxd.bt     local.gfxd.conf  pst1.sector.thin.9.$2.offheap
#runtest $1 pst1.gfxd.16.bt  local.gfxd.conf  pst1.sector.thin.144.$2.offheap
