#! /bin/sh
set -e
exec 1>&1
exec 2>&1
exec 0>&1
set -v
set -x
date
##
rm -f oneliner.txt
rm -f batterytest.log
java -DnukeHungTest=true -DJTESTS=$JTESTS -DGEMFIRE=$GEMFIRE -DtestFileName=simple.bt -DnumTimesToRun=1 batterytest.BatteryTest
