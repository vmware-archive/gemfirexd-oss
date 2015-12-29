#!/bin/sh

#
# pluckstacks.sh is a replacement for pluckstacks.pl.  It pulls full thread
# dumps from files and weeds out uninteresting threads, writing the result
# to stdout.
#
# By default it looks for dumps in bgexec files.  Specify other
# files on the command line to have it look at them instead.
#
# Bruce Schuchardt 4/2013
#

if [ x"$1" = x"-oneStack" ]; then
  oneStack="-DoneStack=true"
  shift 1
fi

files="$@"
if [ x"$files" = x ]; then
  files=bgexec*
fi

#  debug="-DPluckStacks.DEBUG=true"
debug=""

java $debug $oneStack com.gemstone.gemfire.internal.util.PluckStacks $files
