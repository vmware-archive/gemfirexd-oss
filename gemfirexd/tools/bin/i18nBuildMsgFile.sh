#!/bin/bash

#This is a quickly thrown together utility to build the message file that 
# we send off to be translated. 
#It will likely need to be updated each time it is used.
#The output is written to `pwd`/messages_$$.txt

if [ `uname` != "Linux" ]; then
  echo "ERROR: this script is for Linux only."
  exit 1
fi

function process() {
  grep "public static final StringId " $file | perl -ne '$line = $_; chomp($line); $line =~ /new StringId\s*\(\s*(\d+)\s*,\s*(\".*\")\s*\);\s*$/; printf("%-6d = %s\n", $1, $2);' | sort -n
  #End of inlined perl
  if [ "x$?" != "x0" ]; then
    echo "ERROR: error processing file=$file"
    exit 3
  fi
}

base="`echo $0 | xargs dirname | xargs dirname | xargs dirname`/src"
package=com/gemstone/gemfire/internal/i18n
output=`pwd`/messages_$$.txt
files="$base/$package/LocalizedStrings.java $base/$package/JGroupsStrings.java"
for file in $files; do
  if [ ! -f $file ]; then
    echo "ERROR: missing file: base=$base, file=$file, output=$output"
    exit 2
  fi
  process >> $output
done

exit 0
