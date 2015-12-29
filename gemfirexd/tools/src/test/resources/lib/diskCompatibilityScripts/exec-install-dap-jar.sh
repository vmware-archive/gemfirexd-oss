#!/bin/bash

. ./setenv

C_BIND=`uname -n`
C_PORT="1528"
S_PATH=$S_HOME/product-gfxd/bin

# To install
# $S_PATH/gfxd install-jar -name=SPROC_JAR -file=../sproc.jar -client-port=$C_PORT -client-bind-address=$C_BIND

# To replace
# $S_PATH/gfxd replace-jar -name=SPROC_JAR -file=../sproc.jar -client-port=$C_PORT -client-bind-address=$C_BIND	

# To remove
 $S_PATH/gfxd remove-jar -name=SPROC_JAR -client-port=$C_PORT -client-bind-address=$C_BIND	
	
