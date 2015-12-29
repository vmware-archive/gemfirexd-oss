@echo off
rem This script is used by the unit test framework . Instead
rem of invoking java directly, this script is invoked to redirect
rem java 's stdout and stderr to a log file, so that failures
rem are debuggable.  If junit is used for the redirection ,
rem we can't see output until the test finishes.

set ERRORSDIR=%1
shift

rem JRE_JAVA is the complete path to the java vm, to allow specifying
rem the 32 bit or 64 bit VM 
set JRE_JAVA=%1
shift

rem shift does effect %* so store it in a variable and account for
rem the shifting on our own
set args=%*
set args=%args:* =%
set args=%args:* =%

rem # Get argument following the JUnitTestRunner class name
set tmp=%args:* org.apache.tools.ant.taskdefs.optional.junit.JUnitTestRunner=%
FOR %%i IN (%tmp%) do set TestName=%%i && goto setup_complete
:setup_complete
rem removed colons from time output since otherwise Xcode interprets line as
rem error in emacs format
set tstamp=%TIME::=%
echo %tstamp:~0,6%: %TestName%

rem # This :~0,-1 magic is to remove a trailing space
set MYLOG="%TestName:~0,-1%.txt"
rem set JUNIT_VMARGS="-Xdebug -Xrunjdwp:transport=dt_socket,address=8008,suspend=y,server=y"
echo tests\bin\runtest: %JRE_JAVA% %JUNIT_VMARGS% %args% > %MYLOG%
%JRE_JAVA% %JUNIT_VMARGS% %args% >> %MYLOG% 2>&1
if %ERRORLEVEL% EQU 0 goto done
set ERR=%ERRORLEVEL%
copy %MYLOG% %ERRORSDIR%
exit %ERR% 
:done
