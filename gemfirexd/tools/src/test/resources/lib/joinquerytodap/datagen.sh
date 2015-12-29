#!/bin/bash

. ./setenv

C_BIND=`uname -n`
C_PORT="1528"

_DGEN=/export/rdiyewar1/mydev/poc/datagenertor
java $DBG -cp .:$_DGEN:$S_HOME/product-gfxd/lib/gemfirexdclient.jar DataGenerator_ CONTEXT_HISTORY,CHART_HISTORY,RECEIVED_HISTORY_LOG $C_BIND:$C_PORT 50,50,50 mapper
