@echo off
echo Cleaning all
rmdir /S /Q C:\ODBCTH_RESULT
mkdir C:\ODBCTH_RESULT
echo Cleaning server directory

cd %GEMFIREXDPRODUCTDIR%\bin
if not exist "gfxd.bat" (
xcopy /E /Y %BASEDIR%\release\scripts\gfxd.bat .
)
cd %TESTRESULTDIR%
mkdir server
call %GEMFIREXDPRODUCTDIR%\bin\gfxd.bat server stop -dir=%TESTRESULTDIR%/server
call %GEMFIREXDPRODUCTDIR%\bin\gfxd.bat server start -dir=%TESTRESULTDIR%/server -mcast-port=0 -bind-address=localhost -client-port=32451 -log-level=config -classpath=%TESTRESULTDIR%\TestProcedures.jar

rem echo Installing Jars
rem call %GEMFIREXDPRODUCTDIR%\bin\gfxd.bat install-jar -file=%TESTRESULTDIR%/TestProcedures.jar -name=TestProcedures -client-port=32451
%ODBCLIBDIR%\odbc-installer -s -r -n"testdsn"
%ODBCLIBDIR%\odbc-installer -d -r -n"GemFireXD ODBC Driver"
%ODBCLIBDIR%\odbc-installer -d -a -n"GemFireXD ODBC Driver" -t"GemFireXD ODBC Driver;Driver=%ODBCLIBDIR%\gemfirexdodbc.dll;Setup=%ODBCLIBDIR%\gemfirexdodbcSetup.dll;Description=GemFireXD ODBC Driver configuration"
%ODBCLIBDIR%\odbc-installer -ss -a -n"GemFireXD ODBC Driver" -t"DSN=testdsn;Description=DSN for GemFireXD ODBC Driver;APILevel=2;CPTimeout=0;DriverODBCVer=03.51;PORT=32451;SERVER=127.0.0.1;LogFile=gfxdDriver.log"
set PATH=%TESTFRAMEWORKDIR%/lib/xerces-c-3.0.0-x86-windows-vc-8.0/lib;%PATH%
mkdir Ansii
mkdir  C:\ODBCTH_RESULT\Ansii
cd Ansii
xcopy /E /Y "%TESTFRAMEWORKDIR%\UserConfig\Non-Privilege\Ansii\*" .
echo Running ODBC Framework for Non-Unicode
start /B /WAIT  %TESTFRAMEWORKDIR%\Framework\Ansii\ODBCTestHarnessMaster.exe "%TESTFRAMEWORKDIR%\Framework\Ansii\ODBC_TestHarness.exe" -i %ITERATION%  -d  %TESTFRAMEWORKDIR%\Framework\Non-Privilege_Tests\TestCases\AllTest
cd %TESTRESULTDIR%
mkdir  Unicode
mkdir  C:\ODBCTH_RESULT\Unicode
cd Unicode
xcopy /E /Y "%TESTFRAMEWORKDIR%\UserConfig\Non-Privilege\Unicode\*" .
echo Running ODBC Framework for Unicode
start /B /WAIT  %TESTFRAMEWORKDIR%\Framework\Unicode\ODBCTestHarnessMaster.exe "%TESTFRAMEWORKDIR%\Framework\Unicode\ODBC_TestHarness.exe" -i %ITERATION%  -d  %TESTFRAMEWORKDIR%\Framework\Non-Privilege_Tests\TestCases_Unicode


cd %TESTRESULTDIR%
rem echo Removing Jars
rem call %GEMFIREXDPRODUCTDIR%\bin\gfxd.bat remove-jar -bind-address=localhost -client-port=32451 -name=TestProcedures

echo Stopping server
call %GEMFIREXDPRODUCTDIR%\bin\gfxd.bat server stop -dir=%TESTRESULTDIR%/server
rem start privilage tests
echo Starting server in authentication mode
call %GEMFIREXDPRODUCTDIR%\bin\gfxd.bat server start -auth-provider=BUILTIN -dir=%TESTRESULTDIR%\server -bind-address=localhost -client-port=32451 -gemfirexd.user.test=persistent123 -user=test -password=persistent123 -gemfirexd.gfxd-authorization=true -mcast-port=0 -log-level=all 

call %GEMFIREXDPRODUCTDIR%\bin\gfxd.bat run -file=%TESTRESULTDIR%/testodbc.sql -bind-address=localhost -client-port=32451 -user=test -password=persistent123
mkdir  Ansii\PrivilegeTest
mkdir  C:\ODBCTH_RESULT\Ansii\PrivilegeTest
cd Ansii\PrivilegeTest
xcopy /E /Y "%TESTFRAMEWORKDIR%\UserConfig\Privilege\Ansii\*" .
echo Running ODBC Framework for Non-Unicode Privilege
start /B /WAIT %TESTFRAMEWORKDIR%\Framework\Ansii\ODBCTestHarnessMaster.exe "%TESTFRAMEWORKDIR%\Framework\Ansii\ODBC_TestHarness.exe" -i %ITERATION% -d %TESTFRAMEWORKDIR%\Framework\Privilege_Tests\TestCases\AllTest

cd %TESTRESULTDIR%
mkdir  Unicode\PrivilegeTest
mkdir  C:\ODBCTH_RESULT\Unicode\PrivilegeTest
cd Unicode\PrivilegeTest
xcopy /E  /Y "%TESTFRAMEWORKDIR%\UserConfig\Privilege\Unicode\*" .
echo Running ODBC Framework for Unicode Privilege
start /B /WAIT %TESTFRAMEWORKDIR%\Framework\Unicode\ODBCTestHarnessMaster.exe "%TESTFRAMEWORKDIR%\Framework\Unicode\ODBC_TestHarness.exe" -i %ITERATION%  -d %TESTFRAMEWORKDIR%\Framework\Privilege_Tests\TestCases_Unicode


echo Stopping server
cd %TESTRESULTDIR%
call %GEMFIREXDPRODUCTDIR%\bin\gfxd.bat server stop -dir=%TESTRESULTDIR%\server
%ODBCLIBDIR%\odbc-installer -s -r -n"testdsn"
%ODBCLIBDIR%\odbc-installer -d -r -n"GemFireXD ODBC Driver"
del /F %GEMFIREXDPRODUCTDIR%\bin\gfxd.bat
set /a fwkcnt=0
set fwkerror=
C:\Windows\System32\find /C /I "FAIL" C:\ODBCTH_RESULT\Ansii\ODBCTH~1\SummaryReport.csv
if %ERRORLEVEL% == 0 (
set fwkerror=Error: Tests failed for Non-Unicode Tests. see csv file for detail in C:\ODBCTH_RESULT\Ansii\ODBCTH~1\SummaryReport.csv
set /a fwkcnt=%fwkcnt% +1
)

C:\Windows\System32\find /C /I "FAIL" C:\ODBCTH_RESULT\Unicode\ODBCTH~1\SummaryReport.csv
if %ERRORLEVEL% == 0 (
set fwkerror=Error: Tests failed for Unicode Tests. see csv file for detail in C:\ODBCTH_RESULT\Unicode\ODBCTH~1\SummaryReport.csv
set /a fwkcnt=%fwkcnt% +1
)

C:\Windows\System32\find /C /I "FAIL" C:\ODBCTH_RESULT\Ansii\PrivilegeTest\ODBCTH~1\SummaryReport.csv
if %ERRORLEVEL% == 0 (
set fwkerror=Error: Tests failed for Non-Unicode Privilege Tests. see csv file for detail in C:\ODBCTH_RESULT\Ansii\PrivilegeTest\ODBCTH~1\SummaryReport.csv
set /a fwkcnt=%fwkcnt% +1
)

C:\Windows\System32\find /C /I "FAIL" C:\ODBCTH_RESULT\Unicode\PrivilegeTest\ODBCTH~1\SummaryReport.csv
if %ERRORLEVEL% == 0 (
set fwkerror=Error: Tests failed for Unicode Privilege Tests. see csv file for detail in C:\ODBCTH_RESULT\Ansii\PrivilegeTest\ODBCTH~1\SummaryReport.csv
set /a fwkcnt=%fwkcnt% +1
)
if %fwkcnt% GTR 0 (
echo %fwkerror%
set fwkerror=
set /a fwkcnt=0
Exit
)

