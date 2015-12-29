#!/bin/bash

if [[ "$1" =~ cc ]]; then
 echo Compiling .. 
 rm *.class
 javac DataGenerator_.java
fi

if [[ "$1" =~ d$ ]]; then
 DBG="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=localhost:1044"
else
 DBG=
fi

_SQ=/soubhikc1/builds/cheetah_dev_May13/build-artifacts/linux/product-gfxd/lib
#_SQ=/soubhikc1/builds/gemfirexd110X_maint/build-artifacts/linux/product-gfxd/lib
java $DBG -cp .:$_SQ/gemfirexd-client.jar DataGenerator_ APP.C_BO_PRTY,APP.C_BO_POSTAL_ADDR pnq-soubhikc:5541 10000,10000 samplemapping
#java $DBG -cp .:$_SQ/gemfirexd-client.jar DataGenerator_ APP.C_BO_PRTY,APP.C_BO_POSTAL_ADDR pnq-soubhikc:1527 400000,400000 samplemapping
