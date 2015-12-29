#!/bin/sh
touch errlog
grep -i error *.log */*.log | egrep -v "hydra.Prms-stopSystemsOnError" | egrep -v "hydra.Prms-dumpStacksOnError" | egrep -v "hydra.timeserver.TimeServerPrms-errorOnExceededClockSkewThreshold" | egrep -v "\-XX\:\+HeapDumpOnOutOfMemoryError" >> errlog
grep -i exception *.log */*.log >> errlog
grep -i fail *.log */*.log >> errlog
grep -i severe *.log */*.log | egrep -v "may result in severe civil" >> errlog
