#export ANT_HOME=w:/java/ant/jakarta-ant-1.5
#export JAVA_HOME=r:/java/jdk1.4.1.01
#export PATH=.:$JAVA_HOME/bin:$ANT_HOME/bin:$PATH
DS=';'

export NO_BUILD_LOG=true

src=c:/gemfire
obj=c:/gemfire_obj

#export JTESTSROOT=$GEMFIRE/../tests
export JTESTSROOT=$obj/tests
if [ ! -d "$JTESTSROOT" ]; then
  echo "JTESTSROOT $JTESTSROOT does not exist"
fi

export JTESTS=$JTESTSROOT/classes
if [ ! -d "$JTESTS" ]; then
  echo "JTESTS $JTESTS does not exist!"
else
  echo "JTESTS     : " $JTESTS
fi

export GEMFIRE_CHECKOUT=$obj
if [ ! -d "$GEMDIRE_CHECKOUT/product" ]; then
  echo "$GEMFIRE $GEMFIRE_CHECKOUT/product does not exist"
fi
pushd $GEMFIRE_CHECKOUT/product
. ./bin/setenv.sh
popd

export CLASSPATH=.${DS}$JTESTS${DS}$GEMFIRE/lib/gemfire.jar
echo "CLASSPATH  : $CLASSPATH"

if [ "$HOSTTYPE.$OSTYPE" = "i686.cygwin" ]; then
  export PATH=`cygpath -u $GEMFIRE_CHECKOUT/product/../hidden/lib`:`cygpath -u $GEMFIRE_CHECKOUT/product/jre/bin`:$PATH
else
  export PATH=$GEMFIRE_CHECKOUT/product/../hidden/lib:$GEMFIRE_CHECKOUT/product/jre/bin:$PATH
fi

echo "PATH       : $PATH"
