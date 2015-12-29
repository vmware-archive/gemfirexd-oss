#!/bin/sh

# Compare the suite of test results in <beforeDir> to those in <afterDir> using
# ratio comparison.
java -cp $GEMFIRE/lib/gemfire.jar:$JTESTS -Xmx750m -Dmode=ratio -DcompareByKey=false -DJTESTS=$JTESTS -Dgemfire.home=$GEMFIRE -DaddTestKey=true perffmwk.PerfComparer <beforeDir> <afterDir>

# Compare the suite of test results in <beforeDir> to those in <afterDir> using
# raw comparison.
#java -cp $GEMFIRE/lib/gemfire.jar:$JTESTS -Xmx750m -Dmode=ratio -DcompareByKey=false -DJTESTS=$JTESTS -Dgemfire.home=$GEMFIRE -DaddTestKey=true perffmwk.PerfComparer <beforeDir> <afterDir>
