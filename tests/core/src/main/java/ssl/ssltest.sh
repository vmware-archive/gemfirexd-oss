#!/bin/bash

set -xv

######
# Common options
######
PROTOCOLS=SSLv3
CIPHERS=SSL_RSA_WITH_RC4_128_MD5

lport=10334

######
# Functions
######

function usage {
  echo "$0 <product-dir> <jtests-dir>"
  exit 1
}

# get a clean pwd on windows
function wpwd {
  if [ -d "$WINDIR" ]; then
    cygpath -w `pwd` | perl -p -e 's%\\%/%g'
  else
    pwd
  fi
}

# take the spaces out of paths on windows and change to unix slashes.
function fixpath {
  if [ -d "$WINDIR" ]; then
    cygpath -d "$*" | perl -p -e 's%\\%/%g'
  else
    echo $*
  fi
}

function waitfor {
  tmpi=0
  while [ $tmpi != 60 ]; do
    grep "$1" $2
    if [ $? != 0 ]; then
      tmpi=`expr $tmpi + 1`
      sleep 1
    else
      return 0
    fi
  done
  return 1
}

function createXML {

  cat >./cache.xml <<eof

<!DOCTYPE cache PUBLIC
    "-//GemStone Systems, Inc.//GemFire Declarative Caching 3.0//EN"
    "http://www.gemstone.com/dtd/cache3_0.dtd">
<cache>
  <vm-root-region name="TEST">
    <region-attributes scope="distributed-no-ack"/>
  </vm-root-region>
</cache>


eof

}


function createGemfireProperties {

  cat >./gemfire.properties <<eof

mcast-port=0
locators=localhost[$lport]
ssl-enabled=true
ssl-protocols=$PROTOCOLS
ssl-ciphers=$CIPHERS
ssl-require-authentication=true

mcast-address=239.192.81.1
archive-disk-space-limit=0
license-file=gemfireLicense.zip
bind-address=
log-file-size-limit=0
statistic-sample-rate=1000
license-type=evaluation
cache-xml-file=cache.xml
ack-wait-threshold=15
log-disk-space-limit=0
system-directory=`wpwd`
archive-file-size-limit=0
statistic-sampling-enabled=false
statistic-archive-file=statArchive.gfs
name=unitTests
log-level=fine

eof

createXML
}

function createKeystore {
  keytool -genkey \
   -keyalg rsa \
   -alias self \
   -dname "CN=trusted" \
   -validity 3650 \
   -keypass password \
   -keystore ./trusted.keystore \
   -storepass password \
   -storetype JKS
}


######
# script
######

function script {

DIR=`wpwd`

export PATH=$PRODUCT/bin:$PATH
export CLASSPATH=$JTESTS:$PRODUCT/lib/gemfire.jar
if [ -d "$WINDIR" ]; then
  export CLASSPATH=`cygpath -pw $CLASSPATH`
else
  export LD_LIBRARY_PATH=$PRODUCT/lib:$LD_LIBRARY_PATH
fi

createGemfireProperties
# createKeystore

export STORE=`fixpath $JTESTS`/ssl/trusted.keystore

KEYSTORE_ARGS="-Djavax.net.ssl.keyStorePassword=password -Djavax.net.ssl.trustStorePassword=password -Djavax.net.ssl.keyStore=$STORE -Djavax.net.ssl.trustStore=$STORE"

gemfire start-locator -dir=`wpwd` $KEYSTORE_ARGS || return $?

gemfire create mcast-port=0 ssl-enabled=true locators=localhost\[10334\] ssl-protocols=$PROTOCOLS ssl-ciphers=$CIPHERS -dir=$DIR/gfmanager1 || return $?

gemfire config -dir=$DIR/gfmanager1 manager-parameter="-Djavax.net.ssl.keyStore=$STORE" manager-parameter="-Djavax.net.ssl.keyStorePassword=password" manager-parameter="-Djavax.net.ssl.trustStore=$STORE" manager-parameter="-Djavax.net.ssl.trustStorePassword=password" || return $?

gemfire start -dir=$DIR/gfmanager1 || return $?

time java $KEYSTORE_ARGS -Dgemfire.log-file=client1_sys.log ssl.DefaultCacheParticipant >client1.log 2>&1 &
time java $KEYSTORE_ARGS -Dgemfire.log-file=client2_sys.log ssl.DefaultCacheParticipant >client2.log 2>&1 &

waitfor "client complete" client1.log || return $?
waitfor "client complete" client2.log || return $?

gemfire stop -dir=$DIR/gfmanager1 || return $?

gemfire stop-locator -dir=`wpwd` $KEYSTORE_ARGS || return $?

}


#############################################
# main
#

export PRODUCT=$1
shift
if [ ! -f $PRODUCT/lib/gemfire.jar ]; then
  set +xv
  echo "GEMFIRE = $PRODUCT"
  echo "  this is not a product directory."
  usage
fi

export JTESTS=$1
shift
if [ ! -f $JTESTS/hydra/MasterController.class ]; then
  set +xv
  echo "JTESTS = $JTESTS"
  echo "  this is not a test directory."
  usage
fi

workdir=`basename $0 .sh`_results
mkdir $workdir
cd $workdir


( script 2>&1 ; echo "$?" >.status ) | tee output.log

status=`cat .status`
rm .status
echo exiting with errorCode $status
exit $status
