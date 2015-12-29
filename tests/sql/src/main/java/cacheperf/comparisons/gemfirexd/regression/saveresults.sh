#!/bin/bash

BASEDIR=/export/perf/users/lises/sqlfire/regression.2013

if [ -z "$1" ]
then
  echo "No gemfire build revision number was specified."
  exit 0
fi

TESTS="\
       pst1.largeobject.thin.9 \
       pst1.sector.thin.9 \
       pst1.tpcc.thin.9 \
       pst1.tpcc.thin.full.9 \
       pst1.tpcc.thin.full.persist.9 \
       pst3.largeobject.peer.5 \
       pst3.largeobject.peer.5.gfe \
       pst3.largeobject.thin.5 \
       pst3.largeobject.thin.5.gfe \
       pst3.largeobject.thin.sh.5 \
       pst3.largeobject.thin.sh.5.gfe \
       pst3.sector.thin.5 \
       pst3.tpcc.peer.full.5 \
       pst3.tpcc.thin.2 \
       pst3.tpcc.thin.5 \
       pst3.tpcc.thin.full.2 \
       pst3.tpcc.thin.full.5 \
       pst3.tpcc.thin.full.dtx.5 \
       pst3.tpcc.thin.full.persist.2 \
       pst3.tpcc.thin.full.rr.5 \
       tpcc.restart \
       tpcc.restart.hdfs \
       ycsb.continuous \
     "

for t in $TESTS
do
  if [ -d */$t.$1 ];
  then
    mv */$t.$1 $BASEDIR/$t
  fi
done
for t in $TESTS
do
  if [ -d */$t.$1.offheap ];
  then
    mv */$t.$1.offheap $BASEDIR/$t.offheap
  fi
done
