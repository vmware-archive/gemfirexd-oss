#!/bin/sh

rm -f *.so

CURR_DIR=`pwd`
cd $CURR_DIR

script_real_path=`realpath $0`
script_home=`dirname $script_real_path`

# generate and fetch ./com_gemstone_gemfire_internal_NanoTimer.h
cd $script_home/../../../../../../../../../../gemfire-core/src/main/java/
javah  com.gemstone.gemfire.internal.NanoTimer
mv ./com_gemstone_gemfire_internal_NanoTimer.h $script_home/
cd -
if [ -e $script_home/com_gemstone_gemfire_internal_NanoTimer.h ];then
  echo "SUCCESS: com_gemstone_gemfire_internal_NanoTimer.h generated "
fi

# generate and fetch com_pivotal_gemfirexd_internal_GemFireXDVersion.h
cd $script_home/../../../../../
ppp=`pwd`
echo "currently at $ppp"
javah com.pivotal.gemfirexd.internal.GemFireXDVersion
mv ./com_pivotal_gemfirexd_internal_GemFireXDVersion.h $script_home/
cd -
if [ -e $script_home/com_pivotal_gemfirexd_internal_GemFireXDVersion.h ];then
  echo "SUCCESS: com_pivotal_gemfirexd_internal_GemFireXDVersion.h generated "
fi

cd $script_home

gcc -m32 -O3 -fPIC -DPIC -D_REENTRANT -I $JAVA_HOME/include -I $JAVA_HOME/include/linux -I $script_home/ -shared utils.c jvmkill.c -o libgemfirexd.so
gcc -m64 -O3 -fPIC -DPIC -D_REENTRANT -I $JAVA_HOME/include -I $JAVA_HOME/include/linux -I $script_home/ -shared utils.c jvmkill.c -o libgemfirexd64.so

chmod -x *.so


cd $CURR_DIR
