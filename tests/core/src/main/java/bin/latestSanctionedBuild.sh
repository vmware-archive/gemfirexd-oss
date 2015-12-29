#! /bin/bash
#set -xv
# Prints the name of the latest sanctioned build for GemFire

# Need to make sure we print absolute paths
set -P

function usage() {
  echo ""
  echo "** $* "
  echo ""
  echo "usage: latestSanctionedBuild.sh gemfireVersion"
  echo "  Prints the location of the latest sanctioned GemFire build"
  echo ""

  exit 1
}

if [ $# -lt 1 ]; then
  usage "Missing GemFire version"
fi

version=$1
here=`pwd`

# Unix platforms follow the snapshots link to find the latest
if [ `uname` = "SunOS" ]; then
  cd /gcm/where/gemfire/${version}/snaps.sparc.Solaris/snapshots
  latest=`pwd`

elif [ `uname` = "Linux" ]; then
  platform=i686.Linux
  cd /gcm/where/gemfire/${version}/snaps.i686.Linux/snapshots
  latest=`pwd`

else
  # Windows uses the UNC to locate the latest snapshot by date/time
  platform=Windows
  snapdir=//n080-fil01/sancsnaps/users/gemfiresnaps/${version}/$platform
  latest=`ls -1drt $snapdir/snapshots.* | tail -1`

fi

cd $here

if [ -d ${latest} ]; then
  echo ${latest}
else
  usage "Cannot find latest build: ${latest}"
fi
