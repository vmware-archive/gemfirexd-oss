#!/bin/bash

dispError() {
	echo $1
	exit;
}

startSecureDataNode() {
  $1/sbin/hadoop-daemon.sh --config $2 --script hdfs start datanode 2> /dev/null
  exit;
}

stopSecureDataNode() {
  $1/sbin/hadoop-daemon.sh --config $2 --script hdfs stop datanode 2> /dev/null
  exit;
}

# answer 0 if the process exists, 1 otherwise
processExists() {
  /bin/kill -0 $1 2> /dev/null ; echo $?
  exit;
}

killPid() {
  /bin/kill $1 2> /dev/null
  exit;
}

setOwnership() {
  /bin/chown -R $1 $2
  exit;
}

setReadPermission() {
  /bin/chmod -R a+r $1
  exit;
}

setWritePermission() {
  /bin/chmod -R a+w $1
  exit;
}

copyFile() {
  /bin/cp $1 $2
  exit;
}

copyFileRecursive() {
  /bin/cp -R $1 $2
  exit;
}

setupContainerExecutor() {
  /bin/chgrp users $1/bin/container-executor
  /bin/chmod 6050 $1/bin/container-executor
}

if [ "$#" -lt 1 ] ; then
  dispError "ERROR-You must provide at least one parameter that tells the script what to do.";
else
  case $1 in
    startSecureDataNode)
      if [ "$#" -lt 3 ] ; then
        dispError "This script expects more parameters. Make sure you provide one to specify the full path to the hadoop installation directory and one to specify the full path to the datanode configuration directory.";
      else
        startSecureDataNode $2 $3
      fi
      ;;
    stopSecureDataNode)
      if [ "$#" -lt 3 ] ; then
        dispError "This script expects more parameters. Make sure you provide one to specify the full path to the hadoop installation directory and one to specify the full path to the datanode configuration directory.";
      else
        stopSecureDataNode $2 $3
      fi
      ;;
    processExists)
      if [ "$#" -lt 2 ] ; then
        dispError "ERROR-You must provide one more parameter to tell the script which PID to check for.";
      else
        processExists $2
      fi
      ;;
    killPid)
      if [ "$#" -lt 2 ] ; then
        dispError "ERROR-You must provide one more parameter to tell the script which PID to kill.";
      else
        killPid $2
      fi
      ;;
    setOwnership)
      if [ "$#" -lt 3 ] ; then
        dispError "This script expects more parameters. Make sure you provide one to specify the username to use and one to tell the script which directory or file to set the ownership on.";
      else
        setOwnership $2 $3
      fi
      ;;
    setReadPermission)
      if [ "$#" -lt 2 ] ; then
        dispError "ERROR-You must provide one more parameter to tell the script which directory or file to set the read permissions on.";
      else
        setReadPermission $2
      fi
      ;;
    setWritePermission)
      if [ "$#" -lt 2 ] ; then
        dispError "ERROR-You must provide one more parameter to tell the script which directory or file to set the write permissions on.";
      else
        setWritePermission $2
      fi
      ;;
    copyFile)
      if [ "$#" -lt 3 ] ; then
        dispError "ERROR-You must provide a source and destination.";
      else
        copyFile $2 $3
      fi
      ;;
    copyFileRecursive)
      if [ "$#" -lt 3 ] ; then
        dispError "ERROR-You must provide a source and destination.";
      else
        copyFileRecursive $2 $3
      fi
      ;;
    setupContainerExecutor)
      if [ "$#" -lt 2 ] ; then
        dispError "This script expects more parameters. Make sure you provide one to specify the full path to the hadoop installation directory.";
      else
        setupContainerExecutor $2
      fi
      ;;
    *)
      echo "wrong"
      dispError "Invalid 1st parameter."
      ;;
  esac
fi

exit;
