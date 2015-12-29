# Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License. You
# may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License. See accompanying
# LICENSE file.

# Filename: create_hdfsclient_dist.sh
# Description: Script to create an HDFS-client zip file for the use
# of copying distribution to GemFire XD HDFS client installation.

#!/bin/bash
set -e

# Check to see if tmp directory exists before trying to create it
if [ -d /tmp/gfxd_hdfsclient_dist ]; then
  rm -rf /tmp/gfxd_hdfsclient_dist
  mkdir /tmp/gfxd_hdfsclient_dist
else
  mkdir /tmp/gfxd_hdfsclient_dist
fi

# Set default HADOOP_HOME
# If unable to find GPHD_HOME, then exit
if [ -d /usr/lib/gphd ]; then
  HADOOP_HOME=/usr/lib/gphd
else
  echo “Unable to find GPHD Home /usr/lib/gphd”
  exit 1
fi

# Set variables from where most of the jars are located
HADOOP_COMMON=$HADOOP_HOME/hadoop
HADOOP_NATIVE=$HADOOP_COMMON/lib/native
HADOOP_MR=$HADOOP_HOME/hadoop-mapreduce
HADOOP_HDFS=$HADOOP_HOME/hadoop-hdfs

# Gather all the Jars into a single directory
for j in $HADOOP_COMMON/hadoop-annotations.jar \
  $HADOOP_COMMON/hadoop-auth.jar \
  $HADOOP_COMMON/hadoop-common.jar \
  $HADOOP_HDFS/hadoop-hdfs.jar \
  $HADOOP_MR/hadoop-mapreduce-client-core.jar \
  $HADOOP_COMMON/lib/guava-*.jar \
  $HADOOP_COMMON/lib/commons-codec-*.jar \
  $HADOOP_COMMON/lib/commons-configuration-*.jar \
  $HADOOP_COMMON/lib/commons-io-*.jar \
  $HADOOP_COMMON/lib/commons-lang-*.jar \
  $HADOOP_COMMON/lib/jsr*.jar \
  $HADOOP_COMMON/lib/protobuf-*.jar \
  $HADOOP_COMMON/lib/slf4j-api-*.jar; do
  if [ -f "$j" ]; then
    cp $j /tmp/gfxd_hdfsclient_dist
  fi
done

# Create zip file
LOCAL_DIR=`pwd`
pushd /tmp/gfxd_hdfsclient_dist
zip gfxd_hdfsclient_dist.zip *
popd

# Copy zip file back to original directory where the script was executed
cp /tmp/gfxd_hdfsclient_dist/gfxd_hdfsclient_dist.zip $LOCAL_DIR

# Remove tmp directory
rm -rf /tmp/gfxd_hdfsclient_dist
