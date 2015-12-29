#!/bin/bash

. ./setenv

C_BIND=`uname -n`
C_PORT="1528"
S_PATH=$S_HOME/product-gfxd/bin

#echo "creating schema"
$S_PATH/gfxd run -file=schema_temp.sql -client-port=$C_PORT -client-bind-address=$C_BIND

