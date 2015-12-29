#! /bin/sh
set -e
# ------------------------------------
# delTree -- Remove directory tree from a remote host
#
# Arguments:
# 1 - remoteHost, the remote host to which the tree is being sent
# 2 - remoteDir, the root directory above which the tree will be copied
# ------------------------------------

if [ "$1" = "" ]; then
  echo "Usage: $0 remoteHost remoteDir"
  exit 1
fi
remoteHost="$1"
remoteDir="$2"

# ------------------------------------
SSH="ssh -o BatchMode=yes"

removeRemoteDir () {
  zhost="$1";
  zname="$2";
  zdir=`dirname "$zname"`;
  zbase=`basename "$zname"`;
  cmd="      set -e";
  cmd="$cmd; cd \"$zdir\"" ;
  cmd="$cmd; if [ -d \"$zbase\" ]; then ";
  cmd="$cmd    chmod -R ugo+rwx \"$zbase\"";
  cmd="$cmd;   rm -r \"$zbase\"";
  cmd="$cmd; fi";
  cmd="$cmd; if [ -d \"$zbase\" ]; then ";
  cmd="$cmd    echo \"Directory $zdir/$zbase still exists\"";
  cmd="$cmd;   exit 1";
  cmd="$cmd; fi";
  $SSH $remoteHost "$cmd";
  }

removeRemoteDir  $remoteHost "$remoteDir/$remoteName"
if [ $? -ne 0 ]; then
  echo "Failed removing $remoteDir/$remoteName"
  exit 1
fi

echo "Successful removed $remoteHost:$remoteDir"
exit 0
