#!/bin/bash

# This script copies the latest sanctioned GemFire build to a given
# directory

function usage() {
  echo ""
  echo "** $* "
  echo ""
  echo "usage: fectchLatestBuild.sh gemfireVersion directory"
  echo "  Copies the latest GemFire sanctioned build to a given directory"
  echo ""

  exit 1
}

# Copies $1 to $2 if $1 is newer than $2
function copyNewer() {
  latest=$1/gf32sancout
  dir=$2

  if [ ! -d ${dir} ] || [ ${dir} -ot ${latest} ]; then
    rm -rf $2
    mkdir -p $2

    if [ `uname` = "SunOS" ]; then
       gcp -a ${latest}/product ${dir}/product
       gcp -a ${latest}/hidden ${dir}/hidden
       gcp -a ${latest}/tests ${dir}/tests

    elif [ `uname` = "Linux" ]; then
       cp -a ${latest}/product ${dir}/product
       cp -a ${latest}/hidden ${dir}/hidden
       cp -a ${latest}/tests ${dir}/tests

    else
       # Copy directories to look like gf*sancout
       cp -a ${latest}/../product ${dir}/product
       cp -a ${latest}/../hidden ${dir}/hidden

       mkdir -p ${dir}/tests
       cp -a ${latest}/../test_classes ${dir}/tests/classes
       cp -a ${latest}/tests/enhanced-classes ${dir}/tests
    fi
  fi
}

if [ $# -lt 2 ]; then
  usage "Missing arguments"
fi

here=`dirname $0`
latest=`${here}/latestSanctionedBuild.sh $1`

copyNewer ${latest} $2
