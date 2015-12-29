#!/bin/bash

. ./setenv

. ./removeDatFiles

C_BIND=`uname -n`
C_PORT="1528"
S_PATH=$S_HOME/product-gfxd/bin

#echo "exporting data"
$S_PATH/gfxd run -file=export.sql -client-port=$C_PORT -client-bind-address=$C_BIND  
