#!/bin/bash

. ./setenv

C_BIND=`uname -n`
C_PORT="1528"
S_PATH=$S_HOME/product-gfxd/bin

$S_PATH/sqlf run -file=dap.sql -client-port=$C_PORT -client-bind-address=$C_BIND

