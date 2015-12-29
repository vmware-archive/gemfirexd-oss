#!/bin/sh

PATH=$PATH:/gcm/where/cplusplus/thrift/linux/bin
LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/gcm/where/cplusplus/thrift/linux/lib

export PATH LD_LIBRARY_PATH

thrift --gen csharp ../java/shared/com/pivotal/gemfirexd/thrift/common/gfxd.thrift && rm -rf thrift/ && mv gen-csharp/Pivotal/Data/GemFireXD/Thrift thrift && rm -rf gen-csharp
