#!/usr/bin/env bash

export HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:${CLASSPATH}

GEMFIREXD_URL="jdbc:gemfirexd://$BIND_ADDRESS:1527"

RUN_JAR=$1
RUN_CLASS=$2

shift 2

# Set this for local jobs. We really want to run local jobs as they are a lot
# faster - especially on a VM.
LOCAL=1

if [ -n "$LOCAL" ]; then
    # Need to specify mapreduce.cluster.local.dir
    LIB_JARS="-Dmapreduce.framework.name=local -Dmapreduce.cluster.local.dir=/tmp/mr-local"
else
    LIB_JARS="-libjars $RUN_JAR,\
$CLASSPATH"
    LIB_JARS="$LIB_JARS -Dmapreduce.framework.name=yarn"
fi

time eval yarn jar ${RUN_JAR} ${RUN_CLASS} ${LIB_JARS} -Dgemfirexd.url=${GEMFIREXD_URL} $@
