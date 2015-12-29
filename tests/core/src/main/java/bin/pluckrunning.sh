#!/bin/sh
#
#  Pluckrunning uses the discovery.dat file created by Hydra to find all
#  of the distributed systems in a test and pull stack dumps from them.
#
#  It works by connecting to the system and pulling stack dumps from all
#  members through GemFire messaging, so the processes do not have to be
#  on the same machine.
#
#  The stack dumps are filtered with the PluckStacks utility and written
#  to stdout.
#
#  There are three command line options,
#      -tofile <filename>  directs output to the given file
#      -all-threads        turns off filtering
#      -debug              turns on PluckStacks debug output
#
#  It does not currently work with multicast tests.  The probe into
#  discovery.dat needs to be enhanced to find mcast address and port
#  and pass it to GemFire.
#
#  Author: Bruce Schuchardt, 4/2013
#

while [ x"$1" != x ]; do
  if [ x"$1" == x"-all-threads" ]; then
    allthreads="-all-threads"
  elif [ x"$1" == x"-debug" ]; then
    debug="-J-DPluckStacks.DEBUG=true"
  elif [ x"$1" == x"-tofile" ]; then
    shift 1
    tofile=$1
  else
    echo "unknown option '$1'"
    exit 1
  fi
  shift 1
done
  
locators=`cat discovery.dat | cut -d, -f1`
for x in $locators; do
  ds=`fgrep $x discovery.dat | cut -d, -f2`
  if [ x"$tofile" != x ]; then
    echo "=== Distributed system '$ds' ======================================================" >>$tofile
  else
    echo "=== Distributed system '$ds' ======================================================"
  fi
  gemfire -J-Dgemfire.locators=$x -J-Dgemfire.mcast-port=0 $debug \
    print-stacks $allthreads $tofile
done
