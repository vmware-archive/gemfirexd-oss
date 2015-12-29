#!/bin/bash

compare() {
  USAGE="Usage: $0 report mode statspec dir1 dir2 dir3 ... dirN"

  if [ "$#" == "0" ]; then
    echo "$USAGE"
    exit 1
  fi

  report=$1
  shift

  mode=$1
  shift

  statspec=$1
  shift
  if [ -f $statspec ]
  then
    statspeccmd="-DstatSpecFile=${statspec}"
  else
    statspeccmd=""
  fi

  compfile=${mode}.${report}.txt
  echo "Preparing ${compfile}..."
  java -cp $GEMFIRE/lib/gemfire.jar:$JTESTS -Xms4g -Xmx4g -DomitFailedTests=true \
       -DcompReportFile=$compfile -Dmode=$mode $statspeccmd -DcompareByKey=true \
       -DJTESTS=$JTESTS -Dgemfire.home=$GEMFIRE -DaddTestKey=true \
       -DgenerateCSV=true -DcsvFile=$compfile.csv \
       perffmwk.PerfComparer $@
  echo "...see $compfile"
}

all='KEEP.10m.WITH_ETL.SAME_HOST/thin KEEP.10m.WITH_ETL/thin KEEP.10m.SAME_HOST/thin KEEP.10m/thin KEEP.10m.WITH_ETL.SAME_HOST/peer KEEP.10m.WITH_ETL/peer KEEP.10m.SAME_HOST/peer KEEP.10m/peer'

compare thin.v.peer ratio $JTESTS/cacheperf.comparisons.gemfirexd.useCase1/specs/useCase1.spec ${all}
compare thin.v.peer raw   $JTESTS/cacheperf.comparisons.gemfirexd.useCase1/specs/useCase1.spec ${all}
