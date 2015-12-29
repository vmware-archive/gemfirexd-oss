#!/bin/sh
set -x 
set -v
export CLASSPATH=dist/msgsim.jar
#servers="-server -DnumServers=3 -Dsize=10000 -DnumOps=5000 -DdebugServer=true"
servers="-server -DnumServers=3 -Dsize=10000 -DnumOps=500000"
java $servers msgsim.sockserver server 0&
java $servers msgsim.sockserver server 1&
sleep 2

time java $servers msgsim.sockserver server 2 scenario 2 scenarioThreads 2

echo "shutting down"
java $servers msgsim.sockserver server 2 scenario 9
