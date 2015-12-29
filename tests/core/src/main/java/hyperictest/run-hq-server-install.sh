#!/bin/sh

export GEMFIRE_BUILD=/common/hydra/build-artifacts/linux
export GEMFIRE=$GEMFIRE_BUILD/product
export LD_LIBRARY_PATH=$GEMFIRE/../hidden/lib:$GEMFIRE/lib
export JTESTS=$GEMFIRE/../tests/classes
export CLASSPATH=$GEMFIRE/lib/gemfire.jar:$JTESTS
export JAVA_HOME=/usr
echo ""
echo "Running $JTESTS/hyperictest/hq-server-install.bt with $JTESTS/hyperictest/hypericServerRemoteInstaller-linux.conf..."
echo ""
$JAVA_HOME/bin/java -server \
    -classpath $CLASSPATH -DGEMFIRE=$GEMFIRE -DJTESTS=$JTESTS \
    -DmoveRemoteDirs=true \
    -DtestFileName=hq-server-install-linux.bt \
    -DresultDir=/common/hydra/testresults \
     batterytest.BatteryTest
