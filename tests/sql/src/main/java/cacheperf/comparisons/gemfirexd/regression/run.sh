#!/bin/bash

#-------------------------------------------------------------------------------

trap 'exit 1' 2 #traps Ctrl-C (signal 2)

if [ -z "$1" ]
then
  echo "No gemfire build was specified."
  exit 0
fi

if [ -z "$2" ]
then
  echo "No gemfire build revision number was specified."
  exit 0
fi

#-------------------------------------------------------------------------------
# REGRESSION SUITE
#
# BENCHMARK: horizontal scaling with largeobject schema (at smallest and largest scale only)
# PST 1: single server with largeobject schema (at smallest client scale only)
# PST 1: single server with sector schemas (at smallest client scale only)
# PST 1: single server with tpcc schemas (at smallest client scale only); includes individual queries and full test
# PST 3: multiple servers with largeobject schema (at smallest and largest scale only)
# PST 3: multiple servers with sector schema (at smallest and largest scale only)
# PST 3: multiple servers with tpcc schema (at smallest and largest scale only);  includes individual queries and full test
# PST 3 VARIATION: distributed transactions (full tpcc test only)
# PST 3 VARIATION: persistence (full tpcc test only)
# PST 3 VARIATION: repeatable read (full tpcc test only)
# PST 4: eviction (full tpcc test only)
#-------------------------------------------------------------------------------

BASEDIR=$PWD

cd $BASEDIR/tpcc.restart/
./run.sh $1 $2
cd $BASEDIR/pst3.tpcc/
./run.sh $1 $2
cd $BASEDIR/pst3.sector/
./run.sh $1 $2
cd $BASEDIR/pst1.largeobject/
./run.sh $1 $2
cd $BASEDIR/pst3.tpcc.peer/
./run.sh $1 $2
cd $BASEDIR/pst3.tpcc.persist/
./run.sh $1 $2
cd $BASEDIR/pst1.sector/
./run.sh $1 $2
cd $BASEDIR/pst1.tpcc/
./run.sh $1 $2
cd $BASEDIR/pst3.largeobject/
./run.sh $1 $2
cd $BASEDIR/pst3.tpcc.rr/
./run.sh $1 $2
cd $BASEDIR/pst3.tpcc.dtx/
./run.sh $1 $2
cd $BASEDIR/ycsb.continuous/
./run.sh $1 $2
cd $BASEDIR/pst3.tpcc.offheap/
./run.sh $1 $2
cd $BASEDIR/pst3.sector.offheap/
./run.sh $1 $2
cd $BASEDIR/pst1.largeobject.offheap/
./run.sh $1 $2
cd $BASEDIR/pst3.tpcc.peer.offheap/
./run.sh $1 $2
cd $BASEDIR/pst3.tpcc.persist.offheap/
./run.sh $1 $2
cd $BASEDIR/pst1.sector.offheap/
./run.sh $1 $2
cd $BASEDIR/pst1.tpcc.offheap/
./run.sh $1 $2
cd $BASEDIR/pst3.largeobject.offheap/
./run.sh $1 $2
cd $BASEDIR/pst3.tpcc.rr.offheap/
./run.sh $1 $2
cd $BASEDIR/pst3.tpcc.dtx.offheap/
./run.sh $1 $2
#cd $BASEDIR/pst3.tpcc.rr.opt/
#./run.sh $1 $2
#cd $BASEDIR/pst3.tpcc.rr.opt.offheap/
#./run.sh $1 $2
#cd $BASEDIR/pst3.tpcc.dtx.persist/
#./run.sh $1 $2
#cd $BASEDIR/pst3.tpcc.dtx.persist.offheap/
#./run.sh $1 $2
#cd $BASEDIR/pst4.tpcc/
#./run.sh $1 $2
#cd $BASEDIR/pst4.tpcc.offheap/
#./run.sh $1 $2
#cd $BASEDIR/pst5.tpcc/
#./run.sh $1 $2
#cd $BASEDIR/pst5.tpcc.offheap/
#./run.sh $1 $2
