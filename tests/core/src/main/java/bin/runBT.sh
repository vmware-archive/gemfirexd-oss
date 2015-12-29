#!/bin/bash
set -xv
umask 002

# Runs a battery test using the latest sanctioned build

function usage() {
  echo ""
  echo "** $*"
  echo ""
  echo "usage: runBT.sh version outputDir addressee [-Doption]+ [batteryTest]+"
  echo "  Runs batterytest using the latest sanctioned build."
  echo ""
  echo "--propfile Search the build<platform>.properties file for osbuild.dir"
  echo "--osbuild [arg]  Set osbuild.dir to [arg] "
  echo ""
  echo "  version      GemFire version, as specified in /gcm/where/gemfire/"
  echo "  outputDir    Location to put the test results"
  echo "  addressee    Recipient of emailed test results"
  echo "  option       option passed through to batterytest, beginning with -D"
  echo "  batteryTest  Test(s) to run"
  echo ""

}

#intitialize OVERRIDE_PROPFILE
OVERRIDE_PROPFILE=0

#loop to parse -- style arguments
while ( echo "$1" | grep -- '--'  > /dev/null 2>&1 ); do
  if [ "x$1" = "x--propfile" ]; then
    if [ "x$OVERRIDE_OSBUILD" != "x" ]; then
      echo "ERROR: cannot use --propfile and --osbuild options together"
      exit 1
    fi
    OVERRIDE_PROPFILE=1
  elif [ "x$1" = "x--osbuild" ]; then
    if [ $OVERRIDE_PROPFILE -ne 0 ]; then
      echo "ERROR: cannot use --propfile and --osbuild options together"
      exit 1
    fi
    OVERRIDE_OSBUILD=$2
    shift 
  else
    echo "ERROR: unknown -- option $1"
    exit 1
  fi
  shift
done 

#parse -DtestJVM style arguments
if ( echo "$@" | grep -- '-DtestJVM'  > /dev/null 2>&1 ); then
  alt_jvm=`echo "$@" | sed 's/.*-DtestJVM=\([^ ]*\) .*/\1/' | xargs dirname | xargs dirname`
  if [ -d $alt_jvm/bin ]; then
    export ALT_JAVA_HOME=$alt_jvm
  fi
fi

if [ $# -lt 4 ]; then
  usage "Missing arguments"
  exit 1

else
  version=$1
  outputDir=$2
  addressee=$3
  shift 3
fi
OSTYPE=`uname -a | sed -e 's/ .*//' -e 's/CYG.*/Windows_NT/' -e 's/SunOS/Solaris/'`

TESTBIN=`dirname $0`

# If user build runs this script then use the current osbuild.dir in the
# sanctioned build area to run tests against 
if [ "x${OVERRIDE_OSBUILD}" != "x"  ]; then
  OSBUILD=${OVERRIDE_OSBUILD}
  SOURCES=$TESTBIN/../..
  
elif [ "x$USER" = xbuild -o $OVERRIDE_PROPFILE -eq 1 ]; then 

  # We need to figure out the top of the gemfire checkout so we 
  # can set the sources to this
  TOP=$TESTBIN
  limit=10
  num=0

  # Try and find the filehdr.txt file at the top of the checkout
  while [ true ]; do
    if [ -f $TOP/filehdr.txt ]; then
      break
    fi
    let num=num+1
    TOP=`dirname $TOP`

    if [ $TOP = "." ]; then
      echo "Unable to determine top checkout directory"
      echo "Script must be invoked with full pathname to chkbt.sh"
      exit 1
    elif [ $num -eq $limit ]; then
      echo "Unable to determine top checkout directory"
      echo "Script must be invoked with full pathname to chkbt.sh"
      exit 1
    fi
  done

  if [ $OSTYPE = "Solaris" ]; then
    OSBUILD=`cat $TOP/buildsol.properties | grep osbuild.dir | cut -f 2 -d =`
  elif [ $OSTYPE = "Linux" ]; then
    OSBUILD=`cat $TOP/buildlinux.properties | grep osbuild.dir | cut -f 2 -d =`
  else
    OSBUILD=`cat $TOP/buildwin.properties | grep osbuild.dir | cut -f 2 -d =`
  fi
  
  SOURCES=$TOP

else 

  # Normal testing from latest snapshot takes this path
  latest=`${TESTBIN}/latestSanctionedBuild.sh ${version}`

  if [ `uname` = "SunOS" ] || [ `uname` = "Linux" ]; then

    if [ $? -ne 0 ]; then
      usage "Could not determine latest sanctioned build: ${latest}"
    fi

    OSBUILD=`ls -1d ${latest}/gf*sancout | tail -1`
    here=`pwd`
    cd ${latest}/sources/*
    SOURCES=`pwd`
    cd $here

  else
    # On Windows, we make a copy of the latest sanctioned build
# todo: This needs to be tested. 
    #here=`dirname $0`
    OSBUILD=${outputDir}/gf.latest
    ${TESTBIN}/fetchLatestBuild.sh ${version} ${OSBUILD}

#todo: does this really work? No...
    SOURCES=`echo ${OSBUILD} | sed -e "s/Windows/Linux/g"`/sources/gemfire
  fi
fi

export NO_BUILD_LOG=true
export JPROBE_HOME=/do/not/use/JPROBE
options=''

while [ $# -gt 0 ]; do

  echo $1 | egrep "^\-D" > /dev/null
  if [ $? = 0 ]; then
    options=${options}' '$1
    shift

  else

    testname=`basename $1 .bt`

    # Clean up old results. This attempts to get the result directory
    # and not just the testname. 
    dirlist=`ls -1d ${outputDir}/${testname}/*`
    for dir in $dirlist; do
      find $dir -type d -mtime +5 -exec rm -rf {} \; >/dev/null
    done
   
    if [ "x$ALT_JAVA_HOME" != "x" ]; then
      _JAVA=$ALT_JAVA_HOME/bin/java 
    else  
      _JAVA=`${SOURCES}/build.sh | grep JAVA_HOME | head -1 | awk '{print $NF "/bin/java"}'`
    fi

    if [ $OSTYPE = "Windows_NT" ]; then
      CMD="GF_JAVA=$_JAVA"
      CMD=`echo $CMD | sed 's#\/#\\\\#g'`
      CMD="set $CMD&&${OSBUILD}/product/bin/gemfire.bat version full"
      allversionOutput=`cmd /c $CMD`
      versionOutput=`cmd /c $CMD | grep "Java version"`
    else
      allversionOutput=`GF_JAVA=$_JAVA ${OSBUILD}/product/bin/gemfire version full`
      versionOutput=`GF_JAVA=$_JAVA ${OSBUILD}/product/bin/gemfire version full | grep "Java version"`
    fi

    PRODVER=`echo $versionOutput| cut -f 3 -d " "`
    BLDNUM=`echo $versionOutput | cut -f 5 -d " "`
    regressionExtraPath=/${testname}/`date +%Y-%m-%d-%H-%M-%S`
    testOutputDir=${outputDir}${regressionExtraPath}
    mkdir -p ${testOutputDir}
  
    cd ${SOURCES}
  
    # Relative path
    BT_FILE=${OSBUILD}/tests/classes/$1
  
    if [ ! -f ${BT_FILE} ]; then
      # Use the absolute path
      BT_FILE=$1
    fi
  
    # If the build fails (returns non-zero), the script stops
    (${SOURCES}/build.sh -emacs -Dosbuild.dir=${OSBUILD} \
      -DskipLastUpdate=true \
      -Dtests.results.dir=${testOutputDir} -Dregression.extra.path=${regressionExtraPath} \
      -Dbt.result.dir=${testOutputDir} -Dbt.file=${BT_FILE} ${options} \
      execute-battery-nobuild > ${testOutputDir}/../execute-battery.log 2>&1 || true)

    mv ${testOutputDir}/../execute-battery.log ${testOutputDir}
    cd -

    find $testOutputDir -name core -exec rm -f {} \; >/dev/null

    # Create the mail.txt file
    echo "Running BT file:" > ${testOutputDir}/mail.txt
    echo "  $1" >> ${testOutputDir}/mail.txt
    if [ "$options" != "" ]; then
      echo "Using options:" >> ${testOutputDir}/mail.txt
      echo "  ${options}" >> ${testOutputDir}/mail.txt
    fi
    echo "Output directory: " >> ${testOutputDir}/mail.txt
    echo "  ${testOutputDir}" >> ${testOutputDir}/mail.txt

    # get all allversionOutput; Run cmd on Windows due to wrap
    if [ $OSTYPE = "Windows_NT" ]; then
      cmd /c $CMD >> ${testOutputDir}/mail.txt
    else
      echo "$allversionOutput" >> ${testOutputDir}/mail.txt
    fi

    # Check for oneliner.txt file existing. The intention is to catch
    # critical fatal errors and send that in the email instead of 
    # non-existent results 

    if [ -f ${testOutputDir}/oneliner.txt ]; then

      # Collect the detailed results into resultReport.txt file
      #${SOURCES}/release/build/chkbt.sh ${testOutputDir} >> ${testOutputDir}/resultReport.txt
      # don't send full report, make a result only report to the mail file
      #cat ${testOutputDir}/resultReport.txt >> ${testOutputDir}/mail.txt
      SUBJECT="${OSTYPE} ${PRODVER} build ${BLDNUM} Battery Test results for ${testname} from `hostname` "

      if [ -f ${testOutputDir}/batterytest.log ]; then
         #echo "" >> ${testOutputDir}/mail.txt
         #echo "Full result report available at:" >> ${testOutputDir}/mail.txt
         #echo "${testOutputDir}/resultReport.txt" >> ${testOutputDir}/mail.txt
         echo "" >> ${testOutputDir}/mail.txt
         echo "==== STATUS REPORT ====" >> ${testOutputDir}/mail.txt
         grep "==== STATUS REPORT ====" ${testOutputDir}/batterytest.log | cut -f 9 -d "=" >> ${testOutputDir}/mail.txt
      
         # See if there are any failures
         grep " F " ${testOutputDir}/oneliner.txt >/dev/null 2>&1
         if [ $? -eq 0 ]; then
           echo "" >> ${testOutputDir}/mail.txt
           echo "Tests that failed:" >> ${testOutputDir}/mail.txt
           grep " F " ${testOutputDir}/oneliner.txt | cut -f 1 -d " " >> ${testOutputDir}/mail.txt
         fi

         # See if there are any hangs
         grep " H " ${testOutputDir}/oneliner.txt >/dev/null 2>&1
         if [ $? -eq 0 ]; then
           echo "" >> ${testOutputDir}/mail.txt
           echo "Tests that hung:" >> ${testOutputDir}/mail.txt
           grep " H "  ${testOutputDir}/oneliner.txt | cut -f 1 -d " " >> ${testOutputDir}/mail.txt
         fi

      fi

    else

      SUBJECT="FATAL ERRORS on `hostname` running Battery Test ${testname}"
      echo "" >> ${testOutputDir}/mail.txt
      echo "Unable to locate oneliner.txt indicating hydra never got started." >> ${testOutputDir}/mail.txt
      echo "Contents of execute-battery.log:" >> ${testOutputDir}/mail.txt
      echo "" >> ${testOutputDir}/mail.txt
      cat ${testOutputDir}/execute-battery.log >> ${testOutputDir}/mail.txt
      echo "" >> ${testOutputDir}/mail.txt
      echo "Contents of batterytest.log:" >> ${testOutputDir}/mail.txt
      cat ${testOutputDir}/batterytest.log >> ${testOutputDir}/mail.txt
      echo "" >> ${testOutputDir}/mail.txt

    fi

    perl -I${SOURCES}/release/build/perl_modules \
        ${SOURCES}/release/build/perlSendmail.pl \
        -to $addressee -subject "$SUBJECT" \
        -file "${testOutputDir}/mail.txt"

    # Go run the next bt file if there is another...
    shift

  fi

done
