#!/bin/sh
#set -x

#Create directories for results
mkdir -p ${TESTRESULTDIR}/unixODBC/server
#Starting server in normal mode
${GEMFIREXDPRODUCTDIR}/bin/gfxd server stop -dir=${TESTRESULTDIR}/unixODBC/server
${GEMFIREXDPRODUCTDIR}/bin/gfxd server start -bind-address=localhost -client-port=32451 -dir=${TESTRESULTDIR}/unixODBC/server -gemfirexd.user."test"=persistent123 -mcast-port=0 -log-level=config -classpath=${TESTRESULTDIR}/TestProcedures.jar
sleep 3
#installing jars
#${GEMFIREXDPRODUCTDIR}/bin/gfxd install-jar -file=${TESTRESULTDIR}/TestProcedures.jar -name=TestProcedures -client-port=32451
#sleep 2

#Setting Paths for setting paths for unixODBC files
printf "setting paths for unixODBC files\n"

export ODBCINI=${TESTRESULTDIR}/odbc.ini
export ODBCSYSINI=${TESTRESULTDIR}
if [ $IS64BIT -eq 1 ]; then
  if [ "${GFOS}" = "mac" ]; then
    export DYLD_LIBRARY_PATH=${TESTFRAMEWORKDIR}/lib/xerces-c-3.1.1-x86_64-macosx-gcc-4.0/src/.libs:${TESTFRAMEWORKDIR}/lib/apache-log4cxx-0.10.0_macosx_x64/lib:${GCMDIR}/where/cplusplus/gcc/macosx/4.8.1/lib
  else 
    export LD_LIBRARY_PATH=${TESTRESULTDIR}:${GCMDIR}/where/cplusplus/gcc/linux64/4.5.3/lib:${TESTFRAMEWORKDIR}/lib/xerces-c-3.1.1-x86_64-linux-gcc-3.4/lib:${TESTFRAMEWORKDIR}/lib/log4cxx-x86_64-linux-gcc-3.4/lib
  fi 
else
  if [ ${GFOS} = "mac" ]; then
    export DYLD_LIBRARY_PATH=${TESTFRAMEWORKDIR}/lib/xerces-c-3.1.1-x86-macosx-gcc-4.0/lib:${TESTFRAMEWORKDIR}/lib/apache-log4cxx-0.10.0/lib:${GCMDIR}/where/cplusplus/gcc/macosx/4.8.1/lib:${GCMDIR}/where/cplusplus/gcc/macosx/4.8.1/lib/i386
  else
    export LD_LIBRARY_PATH=${TESTRESULTDIR}:${TESTFRAMEWORKDIR}/lib/xerces-c-3.1.1-x86-linux-gcc-3.4/lib:${TESTFRAMEWORKDIR}/lib/extralib:${TESTFRAMEWORKDIR}/lib/log4cxx-x86-linux-gcc-3.4/lib	
  fi 
fi 

#Installing Driver and creating DSN for unixODBC
printf "Installing Driver and creating DSN for unixODBC\n"
${ODBCLIBDIR}/odbc-installer -s -r -n"testdsn"
${ODBCLIBDIR}/odbc-installer -d -r -n"GemFireXD ODBC Driver"
if [ ${GFOS} = "mac" ]; then
  ${ODBCLIBDIR}/odbc-installer -d -a -n"GemFireXD ODBC Driver" -t"GemFireXD ODBC Driver;Driver=${ODBCLIBDIR}/libgemfirexdodbc.dylib;Setup=${ODBCLIBDIR}/libgemfirexdodbc.dylib;Description=GemFireXD ODBC Driver configuration"
else
  ${ODBCLIBDIR}/odbc-installer -d -a -n"GemFireXD ODBC Driver" -t"GemFireXD ODBC Driver;Driver=${ODBCLIBDIR}/libgemfirexdodbc.so;Setup=${ODBCLIBDIR}/libgemfirexdodbc.so;Description=GemFireXD ODBC Driver configuration"
fi
${ODBCLIBDIR}/odbc-installer -s -a -n"GemFireXD ODBC Driver" -t"DSN=testdsn;Description=DSN for GemFireXD ODBC Driver;APILevel=2;CPTimeout=0;DriverODBCVer=03.51;PORT=32451;SERVER=localhost;LogFile=gfxdDriver.log"
printf "Installation completed\n"

#Setting root directory for framework(Non-Unicode)
printf "Setting root directory for framework Non-Unicode\n"
mkdir -p ${TESTRESULTDIR}/unixODBC/Non-Unicode
cd ${TESTRESULTDIR}/unixODBC/Non-Unicode
cp -r ${TESTFRAMEWORKDIR}/UserConfig/unixODBC/Non-Privilege/Ansii/* ./

#Running Framework for Non-Unicode
printf "Running Framework for Non-Unicode\n"
${TESTFRAMEWORKDIR}/Framework/unixODBC/Ansii/ODBCTestHarnessMaster ${TESTFRAMEWORKDIR}/Framework/unixODBC/Ansii/unixODBC_TestHarness -i ${ITERATION} -d ${TESTFRAMEWORKDIR}/Framework/Non-Privilege_Tests/TestCases/AllTest
sleep 2

#Setting root directory for framework Unicode
printf "Setting root directory for framework Unicode\n"
mkdir -p ${TESTRESULTDIR}/unixODBC/Unicode
cd ${TESTRESULTDIR}/unixODBC/Unicode
cp -r ${TESTFRAMEWORKDIR}/UserConfig/unixODBC/Non-Privilege/Unicode/* ./

#Running Framework for Unicode
printf "Running Framework for Unicode\n"
${TESTFRAMEWORKDIR}/Framework/unixODBC/Unicode/ODBCTestHarnessMaster ${TESTFRAMEWORKDIR}/Framework/unixODBC/Unicode/unixODBC_TestHarness -i ${ITERATION} -d ${TESTFRAMEWORKDIR}/Framework/Non-Privilege_Tests/TestCases_Unicode

#Removing Jars
#printf "removing jars\n"
#${GEMFIREXDPRODUCTDIR}/bin/gfxd remove-jar -name=TestProcedures -client-port=32451

#Stopping server
${GEMFIREXDPRODUCTDIR}/bin/gfxd server stop -dir=${TESTRESULTDIR}/unixODBC/server
sleep 3

#Starting server in Authentication mode
printf "Starting server in Authentication mode\n"
${GEMFIREXDPRODUCTDIR}/bin/gfxd server start -bind-address=localhost -client-port=32451 -dir=${TESTRESULTDIR}/unixODBC/server -auth-provider=BUILTIN -gemfirexd.user.test=persistent123 -user=test -password=persistent123 -gemfirexd.sql-authorization=true -mcast-port=0 -log-level=config
sleep 3

#Providing permissions to tables
printf "Providing permissions to tables\n"
${GEMFIREXDPRODUCTDIR}/bin/gfxd run -file=${TESTRESULTDIR}/testodbc.sql -client-port=32451 -user=test -password=persistent123

#Setting root directory for framework(Non-Unicode)
printf "Setting root directory for framework Non-Unicode/Ansii"
mkdir -p ${TESTRESULTDIR}/unixODBC/Non-Unicode/PrivilegeTesting
cd ${TESTRESULTDIR}/unixODBC/Non-Unicode/PrivilegeTesting 
cp -r ${TESTFRAMEWORKDIR}/UserConfig/unixODBC/Privilege/Ansii/* ./


#Running Framework for Non-Unicode
printf "Running Framework for Non-Unicode with Privilege setting\n"
${TESTFRAMEWORKDIR}/Framework/unixODBC/Ansii/ODBCTestHarnessMaster ${TESTFRAMEWORKDIR}/Framework/unixODBC/Ansii/unixODBC_TestHarness -i ${ITERATION} -d ${TESTFRAMEWORKDIR}/Framework/Privilege_Tests/TestCases/AllTest
sleep 2

#Setting root directory for framework Unicode
printf "Setting root directory for framework Unicode\n"
mkdir -p ${TESTRESULTDIR}/unixODBC/Unicode/PrivilegeTesting
cd ${TESTRESULTDIR}/unixODBC/Unicode/PrivilegeTesting
cp -r ${TESTFRAMEWORKDIR}/UserConfig/unixODBC/Privilege/Unicode/* ./
 
#Running Framework for Unicode
printf "Running Framework for Unicode Privilege setting\n"
${TESTFRAMEWORKDIR}/Framework/unixODBC/Unicode/ODBCTestHarnessMaster ${TESTFRAMEWORKDIR}/Framework/unixODBC/Unicode/unixODBC_TestHarness -i ${ITERATION} -d ${TESTFRAMEWORKDIR}/Framework/Privilege_Tests/TestCases_Unicode


#Stopping server
printf "Stopping server\n"
${GEMFIREXDPRODUCTDIR}/bin/gfxd server stop -dir=${TESTRESULTDIR}/unixODBC/server
sleep 3
${ODBCLIBDIR}/odbc-installer -s -r -n"testdsn"
${ODBCLIBDIR}/odbc-installer -d -r -n"GemFireXD ODBC Driver"
cd

# Starting test for iODBC...
#Starting server in normal mode
printf "starting server in normal mode for idobc\n"
mkdir -p ${TESTRESULTDIR}/iODBC/server
${GEMFIREXDPRODUCTDIR}/bin/gfxd server stop -dir=${TESTRESULTDIR}/iODBC/server
sleep 3
${GEMFIREXDPRODUCTDIR}/bin/gfxd server start -bind-address=localhost -client-port=32451 -dir=${TESTRESULTDIR}/iODBC/server -gemfirexd.user."test"=persistent123 -mcast-port=0 -log-level=config -classpath=${TESTRESULTDIR}/TestProcedures.jar
sleep 3

#installing jars
#${GEMFIREXDPRODUCTDIR}/bin/gfxd install-jar -file=${TESTRESULTDIR}/TestProcedures.jar -name=TestProcedures -client-port=32451
#sleep 2


#Setting Paths for setting paths for iODBC files
printf "setting paths for iODBC files\n"
export ODBCSYSINI=${TESTRESULTDIR}
export ODBCINI=${TESTRESULTDIR}/odbc.ini
export ODBCINSTINI=${TESTRESULTDIR}/odbcinst.ini
if [ $IS64BIT -eq 1 ]; then
  if [ $GFOS = "mac" ]; then
    export DYLD_LIBRARY_PATH=${TESTFRAMEWORKDIR}/lib/xerces-c-3.1.1-x86_64-macosx-gcc-4.0/src/.libs:${TESTFRAMEWORKDIR}/lib/apache-log4cxx-0.10.0_macosx_x64/lib:${GCMDIR}/where/cplusplus/gcc/macosx/4.8.1/lib
  else
    export LD_LIBRARY_PATH=${TESTRESULTDIR}:${GCMDIR}/where/cplusplus/gcc/linux64/4.5.3/lib:${TESTFRAMEWORKDIR}/lib/xerces-c-3.1.1-x86_64-linux-gcc-3.4/lib:${TESTFRAMEWORKDIR}/lib/log4cxx-x86_64-linux-gcc-3.4/lib
  fi
else
  if [ $GFOS = "mac" ]; then
    export DYLD_LIBRARY_PATH=${TESTFRAMEWORKDIR}/lib/xerces-c-3.1.1-x86-macosx-gcc-4.0/lib:${TESTFRAMEWORKDIR}/lib/apache-log4cxx-0.10.0/lib:${TESTFRAMEWORKDIR}/lib/apache-log4cxx-0.10.0/lib:${GCMDIR}/where/cplusplus/gcc/macosx/4.8.1/lib/i386
  else  
    export LD_LIBRARY_PATH=${TESTRESULTDIR}:${TESTFRAMEWORKDIR}/lib/xerces-c-3.1.1-x86-linux-gcc-3.4/lib:${TESTFRAMEWORKDIR}/lib/extralib:${TESTFRAMEWORKDIR}/lib/log4cxx-x86-linux-gcc-3.4/lib
  fi
fi
#Installing Driver and creating DSN for iODBC
printf "Installing Driver and creating DSN for iODBC\n"
${ODBCLIBDIR}/iodbc/iodbc-installer -s -r -n"testdsn"
${ODBCLIBDIR}/iodbc/iodbc-installer -d -r -n"GemFireXD ODBC Driver"
if [ $GFOS = "mac" ]; then
  ${ODBCLIBDIR}/iodbc/iodbc-installer -d -a -n"GemFireXD ODBC Driver" -t"GemFireXD ODBC Driver;Driver=${ODBCLIBDIR}/iodbc/libgemfirexdiodbc.dylib;Setup=${ODBCLIBDIR}/iodbc/libgemfirexdiodbc.dylib;APILevel=2;PORT=32451;DriverODBCVer=03.51;SERVER=127.0.0.1;APILevel=2;CPTimeout=0"
else
  ${ODBCLIBDIR}/iodbc/iodbc-installer -d -a -n"GemFireXD ODBC Driver" -t"GemFireXD ODBC Driver;Driver=${ODBCLIBDIR}/iodbc/libgemfirexdiodbc.so;Setup=${ODBCLIBDIR}/iodbc/libgemfirexdiodbc.so;APILevel=2;PORT=32451;DriverODBCVer=03.51;SERVER=127.0.0.1;APILevel=2;CPTimeout=0"
fi
${ODBCLIBDIR}/iodbc/iodbc-installer -s -a -n"GemFireXD ODBC Driver" -t"DSN=testdsn;Description=DSN for GemFireXD ODBC Driver ;SERVER=localhost;PORT=32451"

#Setting root directory for framework(Non-Unicode)
printf "Setting root directory for framework iODBC Non-Unicode/Ansii\n"
mkdir -p ${TESTRESULTDIR}/iODBC/Non-Unicode
cd ${TESTRESULTDIR}/iODBC/Non-Unicode
cp -r ${TESTFRAMEWORKDIR}/UserConfig/iODBC/Non-Privilege/Ansii/* ./


#Running Framework for Non-Unicode
printf "Running Framework for Non-Unicode\n"
${TESTFRAMEWORKDIR}/Framework/iODBC/Ansii/ODBCTestHarnessMaster ${TESTFRAMEWORKDIR}/Framework/iODBC/Ansii/iODBC_TestHarness -i ${ITERATION} -d ${TESTFRAMEWORKDIR}/Framework/Non-Privilege_Tests/TestCases/AllTest
sleep 2

#Setting root directory for framework Unicode
printf "Setting root directory for framework Unicode\n"
printf "Setting root directory for framework iODBC Unicode\n"
mkdir -p ${TESTRESULTDIR}/iODBC/Unicode
cd ${TESTRESULTDIR}/iODBC/Unicode
cp -r ${TESTFRAMEWORKDIR}/UserConfig/iODBC/Non-Privilege/Unicode/* ./

#Running Framework for Unicode
printf "Running Framework for Unicode\n"
${TESTFRAMEWORKDIR}/Framework/iODBC/Unicode/ODBCTestHarnessMaster ${TESTFRAMEWORKDIR}/Framework/iODBC/Unicode/iODBC_TestHarness -i ${ITERATION} -d ${TESTFRAMEWORKDIR}/Framework/Non-Privilege_Tests/TestCases_Unicode

#Removing Jars
#printf "removing jars\n"
#${GEMFIREXDPRODUCTDIR}/bin/gfxd remove-jar -name=TestProcedures -client-port=32451
sleep 2

#Stopping server
printf "stopping server\n"
${GEMFIREXDPRODUCTDIR}/bin/gfxd server stop -dir=${TESTRESULTDIR}/iODBC/server
sleep 3

#Starting server in Authentication mode
printf "Starting server in Authentication mode\n"
${GEMFIREXDPRODUCTDIR}/bin/gfxd server start -bind-address=localhost -client-port=32451 -dir=${TESTRESULTDIR}/iODBC/server -auth-provider=BUILTIN -gemfirexd.user.test=persistent123 -user=test -password=persistent123 -gemfirexd.sql-authorization=true -mcast-port=0 -log-level=config
sleep 3

#Providing permissions to tables
printf "Providing permissions to tables\n"
${GEMFIREXDPRODUCTDIR}/bin/gfxd run -file=${TESTRESULTDIR}/testodbc.sql -client-port=32451 -user=test -password=persistent123
sleep 2
mkdir -p ${TESTRESULTDIR}/iODBC/Non-Unicode/PrivilegeTesting
cd ${TESTRESULTDIR}/iODBC/Non-Unicode/PrivilegeTesting
cp -r ${TESTFRAMEWORKDIR}/UserConfig/iODBC/Privilege/Ansii/* ./

#Running Framework for Non-Unicode
printf "Running Framework for iODBC Non-Unicode/Ansii\n"
${TESTFRAMEWORKDIR}/Framework/iODBC/Ansii/ODBCTestHarnessMaster ${TESTFRAMEWORKDIR}/Framework/iODBC/Ansii/iODBC_TestHarness -i ${ITERATION} -d ${TESTFRAMEWORKDIR}/Framework/Privilege_Tests/TestCases/AllTest
sleep 2

#Setting root directory for framework Unicode
printf "Setting root directory for framework Unicode\n"
mkdir -p ${TESTRESULTDIR}/iODBC/Unicode/PrivilegeTesting
cd ${TESTRESULTDIR}/iODBC/Unicode/PrivilegeTesting
cp -r ${TESTFRAMEWORKDIR}/UserConfig/iODBC/Privilege/Unicode/* ./

#Running Framework for Unicode
printf "Running Framework for Unicode\n"
${TESTFRAMEWORKDIR}/Framework/iODBC/Unicode/ODBCTestHarnessMaster ${TESTFRAMEWORKDIR}/Framework/iODBC/Unicode/iODBC_TestHarness -i ${ITERATION} -d ${TESTFRAMEWORKDIR}/Framework/Privilege_Tests/TestCases_Unicode

#Stopping server
printf "Stopping server\n"
${GEMFIREXDPRODUCTDIR}/bin/gfxd server stop -dir=${TESTRESULTDIR}/iODBC/server
${ODBCLIBDIR}/iodbc/iodbc-installer -s -r -n"testdsn"
${ODBCLIBDIR}/iodbc/iodbc-installer -d -r -n"GemFireXD ODBC Driver"
cd
sleep 2

erro=0
err_cnt=0
# TODO need to change when progress will be supported
if [ ${ISPROGRESS} = "true" ]; then
#========== Tesing start for progress driver manager for odbc
mkdir -p ${TESTRESULTDIR}/progress/server
#Starting server in normal mode for progress
${GEMFIREXDPRODUCTDIR}/bin/gfxd server stop -dir=${TESTRESULTDIR}/progress/server
${GEMFIREXDPRODUCTDIR}/bin/gfxd server start -bind-address=localhost -client-port=32451 -dir=${TESTRESULTDIR}/progress/server -gemfirexd.user."test"=persistent123 -mcast-port=0 -log-level=config -classpath=${TESTRESULTDIR}/TestProcedures.jar
sleep 3
#installing jars
#${GEMFIREXDPRODUCTDIR}/bin/gfxd install-jar -file=${TESTRESULTDIR}/TestProcedures.jar -name=TestProcedures -client-port=32451 -auth-provider=BUILTIN -user=test -password=persistent123
#sleep 2
cd ${TESTRESULTDIR}/progress
cp ${TESTFRAMEWORKDIR}/progress/odbc.ini ./
sed -i "s|\$DRIVER_NAMED|${ODBCLIBDIR}/libgemfirexdodbc.so|g" odbc.ini
sed -i "s|\$INSTALL_DIR|${TESTFRAMEWORKDIR}/progress/dm|g" odbc.ini
export ODBCINI=${TESTRESULTDIR}/progress/odbc.ini
if [ $IS64BIT -eq 1 ]; then
  export LD_LIBRARY_PATH=${TESTRESULTDIR}:${TESTFRAMEWORKDIR}/lib/xerces-c-3.1.1-x86_64-linux-gcc-3.4/lib:${TESTFRAMEWORKDIR}/lib/log4cxx-x86_64-linux-gcc-3.4/lib:${TESTFRAMEWORKDIR}/progress/dm/lib
else
  export LD_LIBRARY_PATH=${TESTRESULTDIR}:${TESTFRAMEWORKDIR}/lib/xerces-c-3.1.1-x86-linux-gcc-3.4/lib:${TESTFRAMEWORKDIR}/lib/log4cxx-x86-linux-gcc-3.4/lib:${TESTFRAMEWORKDIR}/progress/dm/lib
fi
#Setting root directory for framework(Non-Unicode)
printf "Setting root directory for progress Non-Unicode\n"
mkdir -p ${TESTRESULTDIR}/progress/Non-Unicode
cd ${TESTRESULTDIR}/progress/Non-Unicode
cp -r ${TESTFRAMEWORKDIR}/progress/UserConfig/ODBC/Non-Privilege/Ansii/* ./

#Running Framework for Non-Unicode
printf "Running Framework for progress Non-Unicode\n"
${TESTFRAMEWORKDIR}/progress/Framework/Ansii/ODBCTestHarnessMaster ${TESTFRAMEWORKDIR}/progress/Framework/Ansii/progressODBC_TestHarness -i ${ITERATION} -d ${TESTFRAMEWORKDIR}/progress/Framework/Non-Privilege_Tests/TestCases/AllTest
sleep 2
# TODO: Unicode build is not available. uncomment test when build is available
#Setting root directory for framework Unicode
printf "Setting root directory for progress Unicode\n"
mkdir -p ${TESTRESULTDIR}/progress/Unicode
cd ${TESTRESULTDIR}/progress/Unicode
cp -r ${TESTFRAMEWORKDIR}/progress/UserConfig/ODBC/Non-Privilege/Unicode/* ./

#Running Framework for Unicode
printf "Running Framework for progress Unicode\n"
${TESTFRAMEWORKDIR}/progress/Framework/Unicode/ODBCTestHarnessMaster ${TESTFRAMEWORKDIR}/progress/Framework/Unicode/progressODBC_TestHarness -i ${ITERATION} -d ${TESTFRAMEWORKDIR}/progress/Framework/Non-Privilege_Tests/TestCases_Unicode

#Removing Jars
#printf "removing jars\n"
#${GEMFIREXDPRODUCTDIR}/bin/gfxd remove-jar -name=TestProcedures -client-port=32451 -auth-provider=BUILTIN -user=test -password=persistent123

#Stopping server
${GEMFIREXDPRODUCTDIR}/bin/gfxd server stop -dir=${TESTRESULTDIR}/progress/server
sleep 3

#Starting server in Authentication mode
printf "Starting server in Authentication mode for progress tests\n"
${GEMFIREXDPRODUCTDIR}/bin/gfxd server start -bind-address=localhost -client-port=32451 -dir=${TESTRESULTDIR}/progress/server -auth-provider=BUILTIN -gemfirexd.user.test=persistent123 -user=test -password=persistent123 -gemfirexd.sql-authorization=true -mcast-port=0 -log-level=config
sleep 3

#Providing permissions to tables
printf "Providing permissions to tables\n"
${GEMFIREXDPRODUCTDIR}/bin/gfxd run -file=${TESTRESULTDIR}/testodbc.sql -client-port=32451 -user=test -password=persistent123

#Setting root directory for framework(Non-Unicode)
printf "Setting root directory for progress Non-Unicode/Ansii"
mkdir -p ${TESTRESULTDIR}/progress/Non-Unicode/PrivilegeTesting
cd ${TESTRESULTDIR}/progress/Non-Unicode/PrivilegeTesting 
cp -r ${TESTFRAMEWORKDIR}/progress/UserConfig/ODBC/Privilege/Ansii/* ./

# TODO: Unicode build is not available. uncomment test when build is available

#Running Framework for Non-Unicode
printf "Running Framework for progress Non-Unicode with Privilege setting\n"
${TESTFRAMEWORKDIR}/progress/Framework/Ansii/ODBCTestHarnessMaster ${TESTFRAMEWORKDIR}/progress/Framework/Ansii/progressODBC_TestHarness -i ${ITERATION} -d ${TESTFRAMEWORKDIR}/progress/Framework/Privilege_Tests/TestCases/AllTest
sleep 2

#Setting root directory for framework Unicode
printf "Setting root directory for progress Unicode\n"
mkdir -p ${TESTRESULTDIR}/progress/Unicode/PrivilegeTesting
cd ${TESTRESULTDIR}/progress/Unicode/PrivilegeTesting
cp -r ${TESTFRAMEWORKDIR}/progress/UserConfig/ODBC/Privilege/Unicode/* ./
 
#Running Framework for progress Unicode
printf "Running Framework for progress Unicode Privilege setting\n"
${TESTFRAMEWORKDIR}/progress/Framework/Unicode/ODBCTestHarnessMaster ${TESTFRAMEWORKDIR}/progress/Framework/Unicode/progressODBC_TestHarness -i ${ITERATION} -d ${TESTFRAMEWORKDIR}/progress/Framework/Privilege_Tests/TestCases_Unicode


#Stopping server
printf "Stopping server for progress\n"
${GEMFIREXDPRODUCTDIR}/bin/gfxd server stop -dir=${TESTRESULTDIR}/progress/server
cd ${TESTRESULTDIR}
err_cnt=`grep -c 'FAIL' ${TESTRESULTDIR}/progress/Non-Unicode/ODBCTH_Run_*/SummaryReport.csv`
if [ ${err_cnt:-0} -gt 0 ]; then
echo Error: $err_cnt Tests failed for progress Non-Unicode Tests. see csv file for detail in ${TESTRESULTDIR}/progress/Non-Unicode/ODBCTH_Run_*/SummaryReport.csv
err=$err_cnt
fi
err_cnt=`grep -c 'FAIL' ${TESTRESULTDIR}/progress/Non-Unicode/PrivilegeTesting/ODBCTH_Run_*/SummaryReport.csv`
if [ ${err_cnt:-0} -gt 0 ]; then
echo Error: $err_cnt Tests failed for progress Non-Unicode Privilege Tests. see csv file for detail in ${TESTRESULTDIR}/progress/Non-Unicode/PrivilegeTesting/ODBCTH_Run_*/SummaryReport.csv
err=$err_cnt
fi
err_cnt=`grep -c 'FAIL' ${TESTRESULTDIR}/progress/Unicode/ODBCTH_Run_*/SummaryReport.csv`
if [ ${err_cnt:-0} -gt 0 ]; then
echo Error: $err_cnt Tests failed for progress Unicode Tests. see csv file for detail in ${TESTRESULTDIR}/progress/Unicode/ODBCTH_Run_*/SummaryReport.csv
err=$err_cnt
fi
err_cnt=`grep -c 'FAIL' ${TESTRESULTDIR}/progress/Unicode/PrivilegeTesting/ODBCTH_Run_*/SummaryReport.csv`
if [ ${err_cnt:-0} -gt 0 ]; then
echo Error: $err_cnt Tests failed for progress Unicode Privilege Tests. see csv file for detail in ${TESTRESULTDIR}/progress/Unicode/PrivilegeTesting/ODBCTH_Run_*/SummaryReport.csv
err=$err_cnt
fi
#======================End Testing===============================
fi

err_cnt=`grep -c 'FAIL' ${TESTRESULTDIR}/unixODBC/Non-Unicode/ODBCTH_Run_*/SummaryReport.csv`
if [ ${err_cnt:-0} -gt 0 ]; then
echo Error: $err_cnt Tests failed for unixODBC Non-Unicode Tests. see csv file for detail in ${TESTRESULTDIR}/unixODBC/Non-Unicode/ODBCTH_Run_*/SummaryReport.csv
err=$err_cnt
fi
err_cnt=`grep -c 'FAIL' ${TESTRESULTDIR}/unixODBC/Non-Unicode/PrivilegeTesting/ODBCTH_Run_*/SummaryReport.csv`
if [ ${err_cnt:-0} -gt 0 ]; then
echo Error: $err_cnt Tests failed for unixODBC Non-Unicode Privilege Tests. see csv file for detail in ${TESTRESULTDIR}/unixODBC/Non-Unicode/PrivilegeTesting/ODBCTH_Run_*/SummaryReport.csv
err=$err_cnt
fi
err_cnt=`grep -c 'FAIL' ${TESTRESULTDIR}/unixODBC/Unicode/ODBCTH_Run_*/SummaryReport.csv`
if [ ${err_cnt:-0} -gt 0 ]; then
echo Error: $err_cnt Tests failed for unixODBC Unicode Tests. see csv file for detail in $${TESTRESULTDIR}/unixODBC/Unicode/ODBCTH_Run_*/SummaryReport.csv
err=$err_cnt
fi
err_cnt=`grep -c 'FAIL' ${TESTRESULTDIR}/unixODBC/Unicode/PrivilegeTesting/ODBCTH_Run_*/SummaryReport.csv`
if [ ${err_cnt:-0} -gt 0 ]; then
echo Error: $err_cnt Tests failed for unixODBC Unicode Privilege Tests. see csv file for detail in ${TESTRESULTDIR}/unixODBC/Unicode/PrivilegeTesting/ODBCTH_Run_*/SummaryReport.csv
err=$err_cnt
fi
err_cnt=`grep -c 'FAIL' ${TESTRESULTDIR}/iODBC/Non-Unicode/ODBCTH_Run_*/SummaryReport.csv`
if [ ${err_cnt:-0} -gt 0 ]; then
echo Error: $err_cnt Tests failed for iODBC Non-Unicode Tests. see csv file for detail in ${TESTRESULTDIR}/iODBC/Non-Unicode/ODBCTH_Run_*/SummaryReport.csv
err=$err_cnt
fi
err_cnt=`grep -c 'FAIL' ${TESTRESULTDIR}/iODBC/Non-Unicode/PrivilegeTesting/ODBCTH_Run_*/SummaryReport.csv`
if [ ${err_cnt:-0} -gt 0 ]; then
echo Error: $err_cnt Tests failed for iODBC Non-Unicode Privilege Tests. see csv file for detail in {TESTRESULTDIR}/iODBC/Non-Unicode/PrivilegeTesting/ODBCTH_Run_*/SummaryReport.csv
err=$err_cnt
fi
err_cnt=`grep -c 'FAIL' ${TESTRESULTDIR}/iODBC/Unicode/ODBCTH_Run_*/SummaryReport.csv`
if [ ${err_cnt:-0} -gt 0 ]; then
echo Error: $err_cnt Tests failed for iODBC Unicode Tests. see csv file for detail in ${TESTRESULTDIR}/iODBC/Unicode/ODBCTH_Run_*/SummaryReport.csv
err=$err_cnt
fi
err_cnt=`grep -c 'FAIL' ${TESTRESULTDIR}/iODBC/Unicode/PrivilegeTesting/ODBCTH_Run_*/SummaryReport.csv`
if [ ${err_cnt:-0} -gt 0 ]; then
echo Error: $err_cnt Tests failed for iODBC Unicode Privilege Tests. see csv file for detail in ${TESTRESULTDIR}/iODBC/Unicode/PrivilegeTesting/ODBCTH_Run_*/SummaryReport.csv
err=$err_cnt
fi
if [ ${err:-0} -gt 0 ]; then
exit $err
fi
