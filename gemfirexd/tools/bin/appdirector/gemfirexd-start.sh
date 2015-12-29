#!/bin/sh
# vFabric ApplicationDirector Sample START script for vFabric GemFire 

# This example uses the values posted below as defaults.   To change any of these
# values, add the Property Name as shown below as individual properties in your 
# service definition in the ApplicationDirector Catalog.   The value specified after
# the Property name is the Type to use for the property (i.e. String, Content, Array etc)
# There are two types of properties for this script: Required and Optional.  Both are 
# listed below.
#
# REQUIRED PROPERTIES:
# These are the properties you must add in order for this sample script to work. The property
# is added when you create your service definition in the ApplicationDirector Catalog.  
# Property Description:                                Property Value settable in blueprint [type]:
# --------------------------------------------------------------------------------------------
# Location of global configuration data                global_conf [Content]
# value: https://${darwin.server.ip}:8443/darwin/conf/darwin_global.conf   
#                                                            
# OPTIONAL PROPERTIES:
# Property Description:                                 Property Name settable in blueprint:
# --------------------------------------------------------------------------------------------
# which java to use                                     JAVA_HOME [String]
# Installed Location of GemFire                         GEMFIREXD_HOME [String]
# GemFire version installed (default is 663)            GEMFIREXD_VERSION [String]

# From ApplicationDirector - Import and source global configuration
# . $global_conf

# This sample script simply starts a CacheServer from using the cacheserver service call setup from using the RPM install. 

set -e
echo "######################################################################"
echo "#"
echo "# Starting vFabric SQLfire From Application Director"
echo "#"
echo "######################################################################"

export PATH=$PATH:/usr/sbin:/sbin:/usr/bin:/bin
export VMWARE_HOME=/opt/vmware
export GEMFIREXD_PACKAGE=vfabric-gemfirexd
export GEMFIREXD_VERSION=${GEMFIREXD_VERSION:="103"}
export GEMFIREXD_HOME=${GEMFIREXD_HOME:="$VMWARE_HOME/$GEMFIREXD_PACKAGE/vFabric_GemFireXD_$GEMFIREXD_VERSION"}

# Any of the following may be set as Properties in your service definition, and if enabled, may be overwritten 
# in your application blueprint.
export JAVA_HOME=${JAVA_HOME:="/usr"}

if [ -f ${GEMFIREXD_HOME}/bin/gfxd ]; then
    echo "Creating Directories For New GemFireXD Cluster and Locator"
    cd ${GEMFIREXD_HOME}
    su gemfirexd -c 'rm -rf appd_locator appd_server1 appd_server2'
    su gemfirexd -c 'mkdir appd_server1'
    su gemfirexd -c 'mkdir appd_server2'
    su gemfirexd -c 'mkdir appd_locator'
    echo "Starting GemFireXD Locator"
    cd ${GEMFIREXD_HOME}/bin
    su gemfirexd -c 'gfxd locator start -dir=${GEMFIREXD_HOME}/appd_locator -peer-discovery-address=127.0.0.1 -peer-discovery-port=10101 -client-bind-address=127.0.0.1 -client-port=1527'
    status=`su gemfirexd -c 'gfxd locator status -dir=${GEMFIREXD_HOME}/appd_locator | grep running'`
    if [ -n "$status" ]; then
      echo "GemFireXD Locator Started"
    else
      echo "GemFireXD Locator Failed to start"
      exit 1
    fi
    echo "Starting GemFireXD Server1"
    su gemfirexd -c 'gfxd server start -dir=${GEMFIREXD_HOME}/appd_server1 -locators=127.0.0.1[10101] -client-bind-address=127.0.0.1 -client-port=1528'
    status=`su gemfirexd -c 'gfxd server status -dir=${GEMFIREXD_HOME}/appd_server1 | grep running'`
    if [ -n "$status" ]; then
      echo "GemFireXD Server1 Started"
    else
      echo "GemFireXD Server1 Failed to start"
      exit 1
    fi
    echo "Starting GemFireXD Server2"
    su gemfirexd -c 'gfxd server start -dir=${GEMFIREXD_HOME}/appd_server2 -locators=127.0.0.1[10101] -client-bind-address=127.0.0.1 -client-port=1529'
    status=`su gemfirexd -c 'gfxd server status -dir=${GEMFIREXD_HOME}/appd_server2 | grep running'`
    if [ -n "$status" ]; then
      echo "GemFireXD Server2 Started"
    else
      echo "GemFireXD Server2 Failed to start"
      exit 1
    fi
    cd ${GEMFIREXD_HOME}/quickstart
    echo "Loading Data Into Tables"
    su gemfirexd -c 'gfxd run -file=ToursDB_schema.sql'
    su gemfirexd -c 'gfxd run -file=loadTables.sql'
else
    echo "ERROR! GemFireXD executable not found in ${GEMFIREXD_HOME}; Exiting"
    exit
fi
echo "######################################################################"
echo "#"
echo "# vFabric SQLfire Start Script Finished"
echo "#"
echo "######################################################################"
