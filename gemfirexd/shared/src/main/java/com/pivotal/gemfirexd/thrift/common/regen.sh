#!/bin/sh

PATH=$PATH:/gcm/where/cplusplus/thrift/linux/bin:/gcm/where/cplusplus/thrift/linux64/4.8.3/bin:/gcm/where/cplusplus/thrift/linux64/bin
LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/gcm/where/cplusplus/thrift/linux/lib:/gcm/where/cplusplus/thrift/linux64/4.8.3/lib:/gcm/where/cplusplus/thrift/linux64/lib

export PATH LD_LIBRARY_PATH

rm -f ../*.java
thrift --gen "java:skip_async=true" gfxd.thrift
mv gen-java/com/pivotal/gemfirexd/thrift/*.java ../.
rm -rf gen-java
cp HostAddress.java.tmpl ../HostAddress.java
cp Row.java.tmpl ../Row.java
cp ServerType.java.tmpl ../ServerType.java
