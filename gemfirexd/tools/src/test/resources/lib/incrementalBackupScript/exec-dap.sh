#!/bin/bash

. ./setenv

#C_BIND=`uname -n`
#C_PORT="1528"
#S_PATH=$S_HOME/product-gfxd/bin

java -cp dap/dap.jar:$S_HOME/product-gfxd/lib/gemfirexd-client.jar dap/SprocSelectClient
