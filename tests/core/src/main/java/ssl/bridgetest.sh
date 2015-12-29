#!/bin/bash

set -xv

######
# Common options
######
PROTOCOLS=SSLv3
CIPHERS=SSL_RSA_WITH_RC4_128_MD5

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


# pass a port number for the locator...
function createGemfireProperties {
  lport=$1

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
statistic-sampling-enabled=true
statistic-archive-file=statArchive.gfs
name=unitTests
log-level=fine

eof

}

function createBridgeXML {

  cat >./cache.xml <<eofa

<!DOCTYPE cache PUBLIC
    "-//GemStone Systems, Inc.//GemFire Declarative Caching 3.0//EN"
    "http://www.gemstone.com/dtd/cache3_0.dtd">
<cache>
  <shared-root-region name="TEST">
    <region-attributes scope="distributed-no-ack"/>

    <!-- Add one entry to the root region -->

eofa

  i=0
  while [ $i != 50 ]; do
    cat >>./cache.xml <<eofb
<entry><key><string>key${i}</string></key><value><string>value${i}</string></value></entry>
eofb

    i=`expr $i + 1`
  done


  cat >>./cache.xml <<eofc

  </shared-root-region>
</cache>


eofc

}

function createClientXML {

  cat >./cache.xml <<eof

<!DOCTYPE cache PUBLIC
    "-//GemStone Systems, Inc.//GemFire Declarative Caching 3.0//EN"
    "http://www.gemstone.com/dtd/cache3_0.dtd">
<cache>
  <vm-root-region name="TEST">
    <region-attributes>
      <cache-loader>
        <class-name>com.gemstone.gemfire.cache.util.BridgeLoader</class-name>
        <parameter name="endpoints">
          <string>MyHost=localhost:20222</string>
        </parameter>
      </cache-loader>
    </region-attributes>
  </vm-root-region>
</cache>


eof

}

function startBridgeSys {
  lport=10334
  bridgedir=$DIR/bridgesys

  gemfire create mcast-port=0 ssl-enabled=true locators=localhost\[$lport\] \
    ssl-protocols=$PROTOCOLS ssl-ciphers=$CIPHERS -dir=$bridgedir \
  || return $?

  gemfire config -dir=$bridgedir \
    manager-parameter="-Djavax.net.ssl.keyStore=$STORE" \
    manager-parameter="-Djavax.net.ssl.keyStorePassword=password" \
    manager-parameter="-Djavax.net.ssl.trustStore=$STORE" \
    manager-parameter="-Djavax.net.ssl.trustStorePassword=password" \
  || return $?

  cd $bridgedir
  gemfire start-locator -dir=$bridgedir -port=10334 $KEYSTORE_ARGS || return $?

  createBridgeXML

  gemfire start -dir=$bridgedir || return $?

  java $KEYSTORE_ARGS -Dgemfire.log-file=sys_bridge1.log ssl.SSLBridgeServer >bridge.log 2>&1 &

  sleep 2
  waitfor "initialized on port 20222" sys_bridge1.log || return $?


  cd $DIR
}

function stopBridgeSys {
  lport=10334
  bridgedir=$DIR/bridgesys
  
  cd $bridgedir
  touch stopBridgeServer.touch
  sleep 2;
  
  gemfire stop -dir=$bridgedir || return $?
  
  gemfire stop-locator -dir=$bridgedir -port=10334 $KEYSTORE_ARGS || return $?
  
  cd $DIR
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
  
  export STORE=`fixpath $JTESTS`/ssl/trusted.keystore
  
  KEYSTORE_ARGS="-Djavax.net.ssl.keyStorePassword=password -Djavax.net.ssl.trustStorePassword=password -Djavax.net.ssl.keyStore=$STORE -Djavax.net.ssl.trustStore=$STORE"
  
  ## start bridge server.
  startBridgeSys
  
  createGemfireProperties 10444
  createClientXML
  gemfire start-locator -dir=`wpwd` -port=10444 $KEYSTORE_ARGS || return $?
  java $KEYSTORE_ARGS -Dgemfire.log-file=sys_client1.log ssl.BridgeClient >client1.log 2>&1 || return $?
  
  gemfire stop-locator -dir=`wpwd` -port=10444 $KEYSTORE_ARGS || return $?

  stopBridgeSys
  
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

