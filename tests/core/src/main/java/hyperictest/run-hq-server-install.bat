@echo off
SET HOST_NAME=%1
SET GEMFIRE_BUILD=Q:/hydra/build-artifacts/win
SET GEMFIRE=%GEMFIRE_BUILD%/product
SET LD_LIBRARY_PATH=%GEMFIRE%/../hidden/lib:%GEMFIRE%/lib
SET JTESTS=%GEMFIRE_BUILD%/tests/classes
SET CLASSPATH=%GEMFIRE%/lib/gemfire.jar;%JTESTS%
SET JAVA_HOME=Q:/hydra/gcmdir/where/jdk/1.6.0_17/x86.Windows_NT
echo ""
echo "Running %JTESTS%/hyperictest/hq-server-install.bt with %JTESTS%/hyperictest/hypericServerRemoteInstaller.conf..."
echo "%JAVA_HOME%/bin/java -server -classpath %CLASSPATH% -DresultDir=%HOST_NAME%/c$/tmp -DGEMFIRE=%GEMFIRE% -DJTESTS=%JTESTS% -DmoveRemoteDirs=true -DtestFileName=hq-server-install.bt batterytest.BatteryTest"
%JAVA_HOME%/bin/java -server -classpath %CLASSPATH% -DresultDir=%HOST_NAME%/c$/tmp -DGEMFIRE=%GEMFIRE% -DJTESTS=%JTESTS% -DmoveRemoteDirs=true -DtestFileName=hq-server-install.bt batterytest.BatteryTest
