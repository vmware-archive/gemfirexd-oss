#!/bin/sh
set -e

# Please note that there is new optimized code for thrift::Row and some other
# classes. As of now regen.sh cannot be done directly for that reason.
# TODO: update C++ thrift generator to enable generating separate file
# for each class to allow easily overriding/copying optimized bits.

PATH=$PATH:/gcm/where/cplusplus/thrift/linux/4.8.3/bin:/gcm/where/cplusplus/thrift/linux64/4.8.3/bin
LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/gcm/where/cplusplus/thrift/linux/4.8.3/lib:/gcm/where/cplusplus/thrift/linux64/4.8.3/lib

export PATH LD_LIBRARY_PATH

CPP_HEADERS="GFXDService.h LocatorService.h gfxd_struct_GFXDException.h"

#thrift --gen cpp ../../../shared/src/main/java/com/pivotal/gemfirexd/thrift/common/gfxd.thrift
thrift --gen "cpp:struct_separate_files=true,static_consts=true" ../../../shared/src/main/java/com/pivotal/gemfirexd/thrift/common/gfxd.thrift && rm -rf cpp/thrift/ && rm -rf headers/thrift/ && mv gen-cpp cpp/thrift && rm cpp/thrift/*skele* && mkdir -p headers/thrift && mv cpp/thrift/*.h headers/thrift/.

# copy all files from tmpl
for tname in tmpl/*cpp.tmpl; do
  if [ -f "${tname}" ]; then
    fname="`basename "${tname}" | sed 's/.tmpl$//'`"
    rm -f "cpp/thrift/${fname}"
    cp "${tname}" "cpp/thrift/${fname}"
  fi
done
for tname in tmpl/*h.tmpl; do
  if [ -f "${tname}" ]; then
    fname="`basename "${tname}" | sed 's/.tmpl$//'`"
    rm -f "headers/thrift/${fname}"
    cp "${tname}" "headers/thrift/${fname}"
  fi
done

# move back non-public headers
for f in ${CPP_HEADERS}; do
  mv "headers/thrift/${f}" cpp/thrift/.
done
