#!/bin/bash
# Run debug.

####################################
# setup env vars for testing gemfire

export GFXD=/export/w2-2013-lin-20b/users/royc/gemfirexd_rebrand_Dec13/product-gfxd
export GEMFIRE=/export/w2-2013-lin-20b/users/royc/gemfirexd_rebrand_Dec13/product
export TEST_BASE=/export/w2-2013-lin-20b/users/royc/gemfirexd_rebrand_Dec13

export JTESTSROOT=${TEST_BASE}/tests
export JTESTS=$JTESTSROOT/classes
echo "JTESTS     :"$JTESTS

export CLASSPATH=.:$JTESTSROOT/classes:$GEMFIRE/lib/backport-util-concurrent.jar:$GEMFIRE/lib/gemfire.jar:$GEMFIRE/lib/griddb-dependencies.jar:$GFXD/lib/gemfirexd.jar:/gcm/where/java/derby/derby-10.3.1.4/jars/insane/derby.jar:/gcm/where/java/derby/derby-10.3.1.4/jars/insane/derbynet.jar:$GFXD/lib/gemfirexd-tools.jar
echo "CLASSPATH     :" $CLASSPATH

####################################


#build.sh unit-tests
CUR_DIR=`pwd`
BT_FILE=/home/royc/gemfirexd_rebrand_Dec13/tpch.bt
RESULT_DIR=/export/w2-2013-lin-20b/users/royc/tpchrun
LOCAL_CONF=/home/royc/gemfirexd_rebrand_Dec13/local.conf
NUM_TIMES=1
REMOVE_PASSED_TEST=false
echo "RUNNING TEST WITH....\n BT_FILE : $BT_FILE \n LOCAL_CONF :
$LOCAL_CONF \n RESULT_DIR : $RESULT_DIR"
#
# RUN TEST
#
# For running dunit-tests.
#$CUR_DIR/build.sh execute-hydra-test -Dbt.file=$BT_FILE
####-Dlocal.conf=$LOCAL_CONF -Dbt.numTimesToRun=$NUM_TIMES
####-DnukeHungTest=true -Dbt.result.dir=$RESULT_DIR
#$CUR_DIR/build.sh execute-battery-nobuild
####-DtestJVM=/export/java/users/java_share/jdk/1.6.0-ibm/x86.linux/jre/bin/java
####-Dbt.file=$BT_FILE -Dlocal.conf=$LOCAL_CONF
####-Dbt.numTimesToRun=$NUM_TIMES
####-Dbt.removePassedTest=$REMOVE_PASSED_TEST -DnukeHungTest=true
####-Dbt.result.dir=$RESULT_DIR
#
# For running battery tests.
$CUR_DIR/build.sh execute-battery-nobuild -Dbt.file=$BT_FILE -Dlocal.conf=$LOCAL_CONF -Dbt.numTimesToRun=$NUM_TIMES -Dbt.removePassedTest=$REMOVE_PASSED_TEST -DtestJVM=/export/gcm/where/jdk/1.6.0_26/x86_64.linux/bin/java -DnukeHungTest=true -Dbt.result.dir=$RESULT_DIR
#/export/gcm/where/jdk/1.7.0_05/x86_64.linux/bin/java -DmoveRemoteDirs=true -DnukeHungTest=true -DJTESTS=$JTESTS -DGEMFIRE=$GEMFIRE -DtestJVM=/export/gcm/where/jdk/1.7.0_5/x86_64.linux/bin/java -DtestFileName=tpch.bt -DnumTimesToRun=1 batterytest.BatteryTest

#
# CLEANUP.
#
cd $RESULT_DIR; echo Running nukeall in `pwd`;nukeall.sh
cd $CUR_DIR
#
