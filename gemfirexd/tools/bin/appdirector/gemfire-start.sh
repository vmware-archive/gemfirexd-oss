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
# Installed Location of GemFire                         GEMFIRE_HOME [String]
# GemFire version installed (default is 663)            GEMFIRE_VERSION [String]

# From ApplicationDirector - Import and source global configuration
# . $global_conf

# This sample script simply starts a CacheServer from using the cacheserver service call setup from using the RPM install. 

set -e
echo "######################################################################"
echo "#"
echo "# Starting vFabric Gemfire From Application Director"
echo "#"
echo "######################################################################"

export PATH=$PATH:/usr/sbin:/sbin:/usr/bin:/bin
export VMWARE_HOME=/opt/vmware
export GEMFIRE_PACKAGE=vfabric-gemfire
export GEMFIRE_VERSION=${GEMFIRE_VERSION:="663"}
export GEMFIRE_HOME=${GEMFIRE_HOME:="$VMWARE_HOME/$GEMFIRE_PACKAGE/vFabric_GemFire_$GEMFIRE_VERSION"}

# Any of the following may be set as Properties in your service definition, and if enabled, may be overwritten 
# in your application blueprint.
export JAVA_HOME=${JAVA_HOME:="/usr"}

if [ -f ${GEMFIRE_HOME}/bin/gemfire ]; then
    service cacheserver start
else
    echo "ERROR! GemFire executable not found in ${GEMFIRE_HOME}; Exiting"
    exit
fi
echo "######################################################################"
echo "#"
echo "# vFabric Gemfire Start Script Finished"
echo "#"
echo "######################################################################"
