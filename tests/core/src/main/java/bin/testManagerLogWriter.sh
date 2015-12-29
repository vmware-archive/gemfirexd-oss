#!/bin/bash
# 42563 test cases:
# Run this file. It will automatically execute all the test cases

printUsage()
{
  echo "Usage: testManagerLogWriter.sh [GEMFIRE=\$GEMFIRE]"
  echo "Note: No space is allowed around the '=' sign"
  exit
}

if [ "$1" = "-h" ]; then
  printUsage
fi

if [ $# != 0 ]; then
  for arg in $*
  do
    if [ "$arg" = "=" ]; then
      printUsage
    fi
    export $arg
  done
fi

if [ "$GEMFIRE" = "" ]; then
  echo "GEMFIRE should be defined and addeded into path"
  exit 1
fi

getMainId()
{   
  filetype=$1
  
  smallestid=1000000
  biggestid=0
  for f in `ls ${filetype}-*.*`
  do
    childid=`ls $f | cut -f2 -d'-' | cut -f1 -d'.'`
    if [ $childid -gt $biggestid ]; then
      biggestid=$childid
    fi
    if [ $childid -lt $smallestid ]; then
      smallestid=$childid
    fi
  done
  #echo s=$smallestid b=$biggestid
}

getChildId()
{ 
  filetype=$1
  mainid=$2
  
  smallestid=1000000
  biggestid=0
  for f in `ls ${filetype}-${mainid}-*.*`
  do
    childid=`ls $f | cut -f3 -d'-' | cut -f1 -d'.'`
    if [ $childid -gt $biggestid ]; then
      biggestid=$childid
    fi 
    if [ $childid -lt $smallestid ]; then
      smallestid=$childid
    fi
  done 
}

getChildFileName()
{
  filetype=$1
  prefix=`echo $filetype | cut -f1 -d'.'`
  suffix=`echo $filetype | cut -f2 -d'.'`

  getMainId $prefix
  smallestMainId=$smallestid
  biggestMainId=$biggestid

  getChildId $prefix $smallestMainId
  fss=${prefix}-${smallestMainId}-${smallestid}.${suffix}
  fsb=${prefix}-${smallestMainId}-${biggestid}.${suffix}

  getChildId $prefix $biggestMainId
  fbs=${prefix}-${biggestMainId}-${smallestid}.${suffix}
  fbb=${prefix}-${biggestMainId}-${biggestid}.${suffix}

  #echo $fss $fsb $fbs $fbb
}

grepString()
{
  pattern=$1
  targetfile=$2
  
  grep "$pattern" $targetfile
  if [ $? -eq 0 ]; then
    echo
  else
    echo "Failed. Expect $targetfile to contain: $pattern"
    exit 1
  fi
}

getRandomPort()
{
  random=$RANDOM
  random=`expr $random + 10000`
  random=`expr $random % 65534`
}

# Only need one random number for each run
getRandomPort

#########################################
# Test case 1
caseno=1
echo "Test $caseno: no security log, switching log"
echo "Expect: system-01-01.log and server1-01-01.gfs are created"

rm -rf tmp 2>/dev/null
mkdir tmp
cd tmp

cat << EOL > gemfire.properties
log-level=fine
mcast-port=$random
enable-time-statistics=true
statistic-sample-rate=500
statistic-sampling-enabled=true
statistic-archive-file=server1.gfs

log-file=system.log
EOL

${GEMFIRE}/bin/cacheserver start ; sleep 1; ${GEMFIRE}/bin/cacheserver stop
${GEMFIRE}/bin/cacheserver start ; sleep 1; ${GEMFIRE}/bin/cacheserver stop
echo "Expected following files:"
echo "gemfire.properties server1.gfs system-01-01.log server1-01-01.gfs start_cacheserver.log system.log"

filenum=`ls *.gfs *.log | wc -l`
if [ $filenum -ne 5 ]; then
  ls
  echo "Expected 5 files, but there are $filenum"
  exit 1
fi

if [ -f system-01-01.log -a -f server1-01-01.gfs ]; then
  echo Test $caseno PASSED
else
  ls
  echo "system-01-01.log or server1-01-01.gfs is missing"
  exit 1
fi

caseno=`expr $caseno + 1`
cd ..

#########################################
# Test case 2
echo "Test $caseno: no security log, rolling"
echo "Expected at least following files are created:"
echo "meta-system-01.log system-01-01.log system-01-02.log system.log server1.gfs" 

rm -rf tmp 2>/dev/null
mkdir tmp
cd tmp

cat << EOL > gemfire.properties
log-level=fine
mcast-port=$random
enable-time-statistics=true
statistic-sample-rate=500
statistic-sampling-enabled=true
statistic-archive-file=server1.gfs

log-file=system.log
log-file-size-limit=1
EOL

${GEMFIRE}/bin/cacheserver start -J-Dgemfire.logging.test.fileSizeLimitInKB=true -J-Dgemfire.stats.test.fileSizeLimitInKB=true ; sleep 1; ${GEMFIRE}/bin/cacheserver stop
echo "Expected at least following files:"
echo "meta-system-01.log system-01-01.log system-01-02.log system.log server1.gfs" 

if [ -f meta-system-01.log -a -f system.log -a -f server1.gfs ]; then
  echo 
else
  ls
  echo "one of meta-system-01.log system.log server1.gfs is missing"
  exit 1
fi
if [ -f meta-system-02.log -o -f system-02-*.log ]; then
  echo "Unexpected meta-system-02.log or system-02-*.log"
  exit 1
fi

getChildFileName system.log
if [ "$fss" != "system-01-01.log" ]; then
  echo system-01-01.log is missing
  exit 1
fi
if [ "$fbs" != "system-01-01.log" ]; then
  echo Unexpected $fbs
  exit 1
fi

${GEMFIRE}/bin/cacheserver start -J-Dgemfire.logging.test.fileSizeLimitInKB=true -J-Dgemfire.stats.test.fileSizeLimitInKB=true ; ${GEMFIRE}/bin/cacheserver stop

getChildFileName system.log

if [ "$fbs" != "system-02-01.log" ]; then
  echo system-02-01.log is missing
  exit 1
fi

echo Test $caseno PASSED
caseno=`expr $caseno + 1`
cd ..

#########################################
# Test case 3
echo "Test $caseno: Use default security log. cacheserver restart"
rm -rf tmp 2>/dev/null
mkdir tmp
cd tmp

cat << EOL > gemfire.properties
log-level=fine
mcast-port=$random
enable-time-statistics=true
statistic-sample-rate=500
statistic-sampling-enabled=true
statistic-archive-file=server1.gfs

log-file=system.log
security-log-level=fine
EOL

${GEMFIRE}/bin/cacheserver start ; sleep 1; ${GEMFIRE}/bin/cacheserver stop
${GEMFIRE}/bin/cacheserver start ; sleep 1; ${GEMFIRE}/bin/cacheserver stop
echo "Expected following files:"
echo "gemfire.properties server1.gfs system-01-01.log server1-01-01.gfs start_cacheserver.log system.log"

filenum=`ls *.gfs *.log | wc -l`
if [ $filenum -ne 5 ]; then
  ls
  echo "Expected 5 files, but there are $filenum"
  exit 1
fi

if [ -f system-01-01.log -a -f server1-01-01.gfs ]; then
  echo 
else
  ls
  echo "system-01-01.log or server1-01-01.gfs is missing"
  exit 1
fi

grepString "SecurityLogWriter is created." system.log
grepString "SecurityLogWriter is created." system-01-01.log

echo Test $caseno PASSED
caseno=`expr $caseno + 1`
cd ..

#########################################
# Test case 4
echo "Test $caseno: Use explicit security log. cacheserver restart"
rm -rf tmp 2>/dev/null
mkdir tmp
cd tmp

cat << EOL > gemfire.properties
log-level=fine
mcast-port=$random
enable-time-statistics=true
statistic-sample-rate=500
statistic-sampling-enabled=true
statistic-archive-file=server1.gfs

log-file=system.log
security-log-file=sec_system.log
security-log-level=fine
EOL

${GEMFIRE}/bin/cacheserver start ; sleep 1; ${GEMFIRE}/bin/cacheserver stop
${GEMFIRE}/bin/cacheserver start ; sleep 1; ${GEMFIRE}/bin/cacheserver stop
${GEMFIRE}/bin/cacheserver start ; sleep 1; ${GEMFIRE}/bin/cacheserver stop

grepString "SecurityLogWriter is created." sec_system.log
grepString "SecurityLogWriter is created." sec_system-01-01.log
grepString "SecurityLogWriter is created." sec_system-02-01.log

filenum=`ls *log *.gfs | wc -l`
if [ $filenum -ne 10 ]; then
  ls
  echo "Expected 10 files, but there are $filenum"
  exit 1
fi

if [ -f system.log -a -f system-01-01.log -a -f system-02-01.log -a -f server1.gfs -a -f server1-01-01.gfs -a -f server1-02-01.gfs -a -f sec_system.log -a -f sec_system-01-01.log -a -f sec_system-02-01.log ]; then
  echo Test $caseno PASSED
else
  ls
  echo "some files are missing"
  exit 1
fi

caseno=`expr $caseno + 1`
cd ..

#########################################
# Test case 5
echo "Test $caseno: Let log file's number follow archive file's"
rm -rf tmp 2>/dev/null
mkdir tmp
cd tmp

cat << EOL > gemfire.properties
log-level=fine
mcast-port=$random
enable-time-statistics=true
statistic-sample-rate=500
statistic-sampling-enabled=true
statistic-archive-file=server1.gfs

log-file=system.log
security-log-file=sec_system.log
security-log-level=fine
EOL

${GEMFIRE}/bin/cacheserver start ; sleep 1; ${GEMFIRE}/bin/cacheserver stop
${GEMFIRE}/bin/cacheserver start ; sleep 1; ${GEMFIRE}/bin/cacheserver stop
${GEMFIRE}/bin/cacheserver start ; sleep 1; ${GEMFIRE}/bin/cacheserver stop

rm -f *.log

${GEMFIRE}/bin/cacheserver start ; sleep 1; ${GEMFIRE}/bin/cacheserver stop
${GEMFIRE}/bin/cacheserver start ; sleep 1; ${GEMFIRE}/bin/cacheserver stop

filenum=`ls *.log *.gfs | wc -l`
if [ $filenum -ne 10 ]; then
  ls
  echo "Expected 10 files, but there are $filenum"
  exit 1
fi

getChildFileName system.log
if [ "$fss" != "system-04-01.log" ]; then
  echo system-04-01.log is missing
  exit 1
fi
if [ "$fbb" != "system-04-01.log" ]; then
  echo Unexpected $fbb
  exit 1
fi

getChildFileName sec_system.log
if [ "$fss" != "sec_system-04-01.log" ]; then
  echo sec_system-04-01.log is missing
  exit 1
fi
if [ "$fbb" != "sec_system-04-01.log" ]; then
  echo Unexpected $fbb
  exit 1
fi

getChildFileName server1.gfs
if [ "$fss" != "server1-01-01.gfs" ]; then
  echo server1-01-01.gfs is missing
  exit 1
fi
if [ "$fbb" != "server1-04-01.gfs" ]; then
  echo Unexpected $fbb
  exit 1
fi

echo Test $caseno PASSED
caseno=`expr $caseno + 1`
cd ..

#########################################
# Test case 6
echo "Test $caseno: let archive file's number follow log file's"
rm -rf tmp 2>/dev/null
mkdir tmp
cd tmp

cat << EOL > gemfire.properties
log-level=fine
mcast-port=$random
enable-time-statistics=true
statistic-sample-rate=500
statistic-sampling-enabled=true
statistic-archive-file=server1.gfs

log-file=system.log
security-log-file=sec_system.log
security-log-level=fine
EOL

${GEMFIRE}/bin/cacheserver start ; sleep 1; ${GEMFIRE}/bin/cacheserver stop
${GEMFIRE}/bin/cacheserver start ; sleep 1; ${GEMFIRE}/bin/cacheserver stop
${GEMFIRE}/bin/cacheserver start ; sleep 1; ${GEMFIRE}/bin/cacheserver stop

rm -f *.log

${GEMFIRE}/bin/cacheserver start ; sleep 1; ${GEMFIRE}/bin/cacheserver stop
${GEMFIRE}/bin/cacheserver start ; sleep 1; ${GEMFIRE}/bin/cacheserver stop

filenum=`ls *.gfs *.log | wc -l`
if [ $filenum -ne 10 ]; then
  ls
  echo "Expected 10 files, but there are $filenum"
  exit 1
fi

getChildFileName server1.gfs
if [ "$fss" != "server1-01-01.gfs" ]; then
  echo server1-01-01.gfs is missing
  exit 1
fi
if [ "$fbb" != "server1-04-01.gfs" ]; then
  echo Unexpected $fbb
  exit 1
fi

getChildFileName sec_system.gfs
if [ "$fss" != "sec_system-04-01.gfs" ]; then
  echo sec_system-01-01.gfs is missing
  exit 1
fi
if [ "$fbb" != "sec_system-04-01.gfs" ]; then
  echo Unexpected $fbb
  exit 1
fi

getChildFileName system.gfs
if [ "$fss" != "system-04-01.gfs" ]; then
  echo system-04-01.gfs is missing
  exit 1
fi
if [ "$fbb" != "system-04-01.gfs" ]; then
  echo Unexpected $fbb
  exit 1
fi

echo Test $caseno PASSED
caseno=`expr $caseno + 1`
cd ..

#########################################
# Test case 7 
echo "Test $caseno: rolling log with explicit security log"
  
rm -rf tmp 2>/dev/null
mkdir tmp
cd tmp
  
cat << EOL > gemfire.properties
log-level=fine
mcast-port=$random
enable-time-statistics=true
statistic-sample-rate=500
statistic-sampling-enabled=true
statistic-archive-file=server1.gfs

log-file=system.log
log-file-size-limit=1
security-log-file=sec_system.log 
security-log-level=fine
EOL

${GEMFIRE}/bin/cacheserver start -J-Dgemfire.logging.test.fileSizeLimitInKB=true -J-Dgemfire.stats.test.fileSizeLimitInKB=true ; sleep 1; ${GEMFIRE}/bin/cacheserver stop

if [ -f meta-system-01.log -a -f meta-sec_system-01.log -a -f system.log -a -f server1.gfs -a -f sec_system.log ]; then
  echo 
else
  ls
  echo "one of meta-system-01.log meta-sec_system-01.log system.log server1.gfs sec_system.log is missing"
  exit 1
fi

getChildId system 01
exp_biggestid=`expr $biggestid + 1`

${GEMFIRE}/bin/cacheserver start -J-Dgemfire.logging.test.fileSizeLimitInKB=true -J-Dgemfire.stats.test.fileSizeLimitInKB=true ; sleep 1; ${GEMFIRE}/bin/cacheserver stop

getChildId system 01
if [ $biggestid -ne $exp_biggestid ]; then
  echo "Expect system.log to have become system-01-${exp_biggestid}.log"
  exit 1
fi
getChildId system 02
if [ ! -f system-02-${biggestid}.log ]; then
  echo "Expect the last child log file is system-02-${biggestid}.log" 
  exit 1
fi

if [ -f meta-system-01.log -a -f meta-system-02.log -a -f meta-sec_system-01.log -a -f meta-sec_system-02.log -a -f sec_system-01-01.log ]; then
  echo 
else
  ls
  echo "one of meta-system-01.log meta-system-02.log sec_system-01-01.log meta-sec_system-01.log meta-sec_system-02.log is missing"
  exit 1
fi

echo Test $caseno PASSED
caseno=`expr $caseno + 1`
cd ..

#########################################
# Test case 8 
echo "Test $caseno: rolling log with default security log"
  
rm -rf tmp 2>/dev/null
mkdir tmp
cd tmp
  
cat << EOL > gemfire.properties
log-level=fine
mcast-port=$random
enable-time-statistics=true
statistic-sample-rate=500
statistic-sampling-enabled=true
statistic-archive-file=server1.gfs

log-file=system.log
log-file-size-limit=1
security-log-level=fine
EOL

${GEMFIRE}/bin/cacheserver start -J-Dgemfire.logging.test.fileSizeLimitInKB=true -J-Dgemfire.stats.test.fileSizeLimitInKB=true; sleep 1; ${GEMFIRE}/bin/cacheserver stop

if [ -f meta-system-01.log -a -f system.log -a -f server1.gfs ]; then
  echo 
else
  ls
  echo "one of meta-system-01.log system.log server1.gfs is missing"
  exit 1
fi

getChildId system 01
exp_biggestid=`expr $biggestid + 1`

${GEMFIRE}/bin/cacheserver start -J-Dgemfire.logging.test.fileSizeLimitInKB=true -J-Dgemfire.stats.test.fileSizeLimitInKB=true ; sleep 1; ${GEMFIRE}/bin/cacheserver stop

getChildId system 01
if [ $biggestid -ne $exp_biggestid ]; then
  echo "Expect system.log to have become system-01-${exp_biggestid}.log"
  exit 1
fi
getChildId system 02
if [ ! -f system-02-${biggestid}.log ]; then
  echo "Expect the last child log file is system-02-${biggestid}.log" 
  exit 1
fi

if [ -f meta-system-01.log -a -f meta-system-02.log ]; then
  echo 
else
  ls
  echo "one of meta-system-01.log meta-system-02.log is missing"
  exit 1
fi

echo Test $caseno PASSED
caseno=`expr $caseno + 1`
cd ..

#########################################
# Test case 9
echo "Test $caseno: start locator with default security log"

rm -rf tmp 2>/dev/null
mkdir tmp
cd tmp

cat << EOL > gemfire.properties
log-level=fine
mcast-port=$random

locators=localhost[41234]
log-file=locator.log
security-log-level=fine
EOL

${GEMFIRE}/bin/gemfire start-locator -port=41234; ${GEMFIRE}/bin/gemfire stop-locator -port=41234

if [ -f locator.log -a -f start_locator.log ]; then
  echo 
else
  ls
  echo "one of locator.log start_locator.log is missing"
  exit 1
fi

grepString "SecurityLogWriter for locator is created." locator.log

echo Test $caseno PASSED
caseno=`expr $caseno + 1`
cd ..

#########################################
# Test case 10
echo "Test $caseno: start locator with explicit security log"

rm -rf tmp 2>/dev/null
mkdir tmp
cd tmp

cat << EOL > gemfire.properties
log-level=fine
mcast-port=$random

locators=localhost[41234]
log-file=locator.log
security-log-file=sec_locator.log
security-log-level=fine
EOL

${GEMFIRE}/bin/gemfire start-locator -port=41234; ${GEMFIRE}/bin/gemfire stop-locator -port=41234

if [ -f locator.log -a -f sec_locator.log ]; then
  echo 
else
  ls
  echo "one of locator.log sec_locator.log is missing"
  exit 1
fi

grepString "SecurityLogWriter for locator is created." sec_locator.log

echo Test $caseno PASSED
caseno=`expr $caseno + 1`
cd ..

#########################################
# Test case 11
echo "Test $caseno: specify different archive directory"
rm -rf tmp 2>/dev/null
mkdir -p tmp/gfs
cd tmp

cat << EOL > gemfire.properties
log-level=fine
mcast-port=$random
enable-time-statistics=true
statistic-sample-rate=500
statistic-sampling-enabled=true
statistic-archive-file=gfs/server1.gfs

log-file=system.log
security-log-file=sec_system.log
security-log-level=fine
EOL

${GEMFIRE}/bin/cacheserver start ; sleep 1; ${GEMFIRE}/bin/cacheserver stop
${GEMFIRE}/bin/cacheserver start ; sleep 1; ${GEMFIRE}/bin/cacheserver stop
${GEMFIRE}/bin/cacheserver start ; sleep 1; ${GEMFIRE}/bin/cacheserver stop

rm -f *.log

${GEMFIRE}/bin/cacheserver start ; sleep 1; ${GEMFIRE}/bin/cacheserver stop
${GEMFIRE}/bin/cacheserver start ; sleep 1; ${GEMFIRE}/bin/cacheserver stop

if [ -f sec_system-01-01.log -a -f system.log -a -f sec_system.log -a -f system-01-01.log ]; then
  echo 
else
  ls
  echo "one of sec_system-01-01.log system.log sec_system.log system-01-01.log is missing"
  exit 1
fi

if [ -f gfs/server1-01-01.gfs -a -f gfs/server1-02-01.gfs -a -f gfs/server1-03-01.gfs -a -f gfs/server1-04-01.gfs ]; then
  echo 
else
  ls
  echo "one of server1-01-01.gfs server1-02-01.gfs server1-03-01.gfs server1-04-01.gfs is missing"
  exit 1
fi

echo Test $caseno PASSED
caseno=`expr $caseno + 1`
cd ..

#########################################
# Test case 12
echo "Test $caseno: roll both log file and archive file"

rm -rf tmp 2>/dev/null
mkdir tmp
cd tmp

cat << EOL > gemfire.properties
log-level=fine
mcast-port=$random
enable-time-statistics=true
statistic-sample-rate=500
statistic-sampling-enabled=true
statistic-archive-file=server1.gfs

log-file=system.log
security-log-file=sec_system.log
security-log-level=fine
archive-file-size-limit=1
log-file-size-limit=1
EOL

${GEMFIRE}/bin/cacheserver start -J-Dgemfire.logging.test.fileSizeLimitInKB=true -J-Dgemfire.stats.test.fileSizeLimitInKB=true ; ${GEMFIRE}/bin/cacheserver stop

if [ -f meta-system-01.log -a -f system.log -a -f sec_system.log -a -f server1-01-01.gfs -a -f server1-01-02.gfs ]; then
  echo
else
  ls
  echo "one of meta-system-01.log system.log sec_system.log server1-01-01.gfs server1-01-02.gfs is missing"
  exit 1
fi
if [ -f server1.gfs ]; then
  echo "When used archive-file-size-limit, there should be no server1.gfs"
  exit 1
fi

getChildFileName server1.gfs
if [ "$fss" != "server1-01-01.gfs" ]; then
  echo server1-01-01.gfs is missing
  exit 1
fi

${GEMFIRE}/bin/cacheserver start -J-Dgemfire.logging.test.fileSizeLimitInKB=true -J-Dgemfire.stats.test.fileSizeLimitInKB=true ; ${GEMFIRE}/bin/cacheserver stop

if [ -f meta-system-02.log -a -f sec_system-01-01.log -a -f server1-02-02.gfs ]; then
  echo
else
  ls
  echo "one of meta-system-02.log sec_system-01-01.log server1-02-02.gfs is missing"
  exit 1
fi

getChildFileName server1.gfs
if [ "$fbs" != "server1-02-01.gfs" ]; then
  echo server1-02-01.gfs is missing
  exit 1
fi

getChildFileName system.log
if [ "$fbs" != "system-02-01.log" ]; then
  echo system-02-01.log is missing
  exit 1
fi

echo Test $caseno PASSED
caseno=`expr $caseno + 1`
cd ..

#########################################
# Test case 13
echo "Test $caseno: no security log, log size not big enough to roll"
echo "There should be no force rolling"

rm -rf tmp 2>/dev/null
mkdir tmp
cd tmp

cat << EOL > gemfire.properties
log-level=fine
mcast-port=$random
enable-time-statistics=true
statistic-sample-rate=500
statistic-sampling-enabled=true
statistic-archive-file=server1.gfs

log-file=system.log
log-file-size-limit=1
EOL

${GEMFIRE}/bin/cacheserver start ; sleep 1; ${GEMFIRE}/bin/cacheserver stop
echo "Expected following files:"
echo "meta-system-01.log system.log server1.gfs"

if [ -f meta-system-01.log -a -f system.log -a -f server1.gfs ]; then
  echo
else
  ls
  echo "one of meta-system-01.log system.log server1.gfs is missing"
  exit 1
fi

${GEMFIRE}/bin/cacheserver start ; sleep 1; ${GEMFIRE}/bin/cacheserver stop
echo "Expected following files:"
echo "meta-system-02.log system-01-01.log meta-system-01.log server1-01-01.gfs server1.gfs system.log"

if [ -f meta-system-02.log -a -f system-01-01.log -a -f meta-system-01.log -a -f server1-01-01.gfs -a -f server1.gfs -a -f system.log ]; then
  echo
else
  ls
  echo "one of meta-system-02.log system-01-01.log meta-system-01.log server1-01-01.gfs server1.gfs system.log is missing"
  exit 1
fi

${GEMFIRE}/bin/cacheserver start ; sleep 1; ${GEMFIRE}/bin/cacheserver stop
echo "Expected following files:"
echo "meta-system-03.log meta-system-02.log meta-system-01.log system-02-01.log system-01-01.log system-02-01.log server1-01-01.gfs server1.gfs system.log"

if [ -f meta-system-03.log -a -f meta-system-02.log -a -f meta-system-01.log -a -f system-02-01.log -a -f system-01-01.log -a -f system-02-01.log -a -f server1-01-01.gfs -a -f server1.gfs -a -f system.log ]; then
  echo
else
  ls
  echo "one of meta-system-03.log meta-system-02.log meta-system-01.log system-02-01.log system-01-01.log system-02-01.log server1-01-01.gfs server1.gfs system.log is missing"
  exit 1
fi

echo Test $caseno PASSED
caseno=`expr $caseno + 1`
cd ..

#########################################
# Test case 14
echo "Test $caseno: no security log, rolling"
echo "Expected at least following files are created:"
echo "log/meta-system-01.log log/system-01-01.log log/system-01-02.log log/system.log server1.gfs" 

rm -rf tmp 2>/dev/null
mkdir -p tmp/log
cd tmp

cat << EOL > gemfire.properties
log-level=fine
mcast-port=$random
enable-time-statistics=true
statistic-sample-rate=500
statistic-sampling-enabled=true
statistic-archive-file=server1.gfs

log-file=./log/system.log
log-file-size-limit=1
EOL

${GEMFIRE}/bin/cacheserver start -J-Dgemfire.logging.test.fileSizeLimitInKB=true -J-Dgemfire.stats.test.fileSizeLimitInKB=true ; sleep 1; ${GEMFIRE}/bin/cacheserver stop
echo "Expected at least following files:"
echo "log/meta-system-01.log log/system-01-01.log log/system-01-02.log log/system.log server1.gfs" 

if [ -f log/meta-system-01.log -a -f log/system.log -a -f server1.gfs ]; then
  echo 
else
  ls
  echo "one of log/meta-system-01.log log/system.log server1.gfs is missing"
  exit 1
fi
if [ -f log/meta-system-02.log -o -f log/system-02-*.log ]; then
  echo "Unexpected meta-system-02.log or system-02-*.log"
  exit 1
fi

getChildFileName log/system.log
if [ "$fss" != "log/system-01-01.log" ]; then
  echo log/system-01-01.log is missing
  exit 1
fi
if [ "$fbs" != "log/system-01-01.log" ]; then
  echo Unexpected $fbs
  exit 1
fi

${GEMFIRE}/bin/cacheserver start -J-Dgemfire.logging.test.fileSizeLimitInKB=true -J-Dgemfire.stats.test.fileSizeLimitInKB=true ; sleep 1; ${GEMFIRE}/bin/cacheserver stop

getChildFileName log/system.log

if [ "$fbs" != "log/system-02-01.log" ]; then
  echo log/system-02-01.log is missing
  exit 1
fi

echo Test $caseno PASSED
caseno=`expr $caseno + 1`

#########################################
# Test case 15
echo "Test $caseno: NPE 41474 verification"
echo "Expected at least following files are created:"
echo "log/log/server1-01-01.gfs log/log/server1-01-02.gfs" 

rm -rf tmp 2>/dev/null
mkdir tmp
cd tmp
#mkdir log

cat << EOL > gemfire.properties
log-level=fine
mcast-port=$random
enable-time-statistics=true
statistic-sample-rate=500
statistic-sampling-enabled=true
statistic-archive-file=./log/log/server1.gfs

log-file=
security-log-file=
archive-file-size-limit=1

EOL

${GEMFIRE}/bin/cacheserver start -J-Dgemfire.logging.test.fileSizeLimitInKB=true -J-Dgemfire.stats.test.fileSizeLimitInKB=true ; ${GEMFIRE}/bin/cacheserver stop

if [ -f log/log/server1-01-01.gfs -a -f log/log/server1-01-02.gfs ]; then
  echo 
else
  ls
  echo "one of log/log/server1-01-01.gfs log/log/server1-01-02.gfs is missing"
  exit 1
fi

echo Test $caseno PASSED
caseno=`expr $caseno + 1`

