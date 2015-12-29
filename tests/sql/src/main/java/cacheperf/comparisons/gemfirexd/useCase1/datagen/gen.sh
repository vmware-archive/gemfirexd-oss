#!/bin/bash

if [[ "$1" =~ cc ]]; then
 echo Compiling .. 
 javac DataGenerator.java
fi

if [[ "$1" =~ d$ ]]; then
 DBG="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=localhost:1044"
else
 DBG=
fi

rm *.dat
java $DBG -cp .:/soubhikc1/builds/gemfirexd_rebrand_Dec13/build-artifacts/linux/product-gfxd/lib/gemfirexd-client.jar DataGenerator SEC_OWNER.DATAHINTS,SEC_OWNER.SECT_CHANNEL_DATA,SEC_OWNER.SECT_CHANNEL_RAW_DATA "localhost:1123/;user=locatoradmin;password=locatorpassword" 10,90,90 samplemapping
