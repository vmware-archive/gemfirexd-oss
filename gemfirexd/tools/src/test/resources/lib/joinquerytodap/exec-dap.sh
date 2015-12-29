#!/bin/bash

. ./setenv

#C_BIND=`uname -n`
#C_PORT="1528"
#S_PATH=$S_HOME/product-gfxd/bin

java -cp ../JoinQueryToDap.jar:$S_HOME/product-gfxd/lib/gemfirexd-client.jar examples.joinquerytodap.JoinQueryTuneDAPClient

