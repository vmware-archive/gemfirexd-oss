@setlocal enableextensions
@set scriptdir=%~dp0
@set gf=%scriptdir:\bin\=%
@if exist "%gf%\lib\gemfire.jar" @goto gfok
@echo Could not determine GEMFIRE location
@verify other 2>nul
@goto done
:gfok
@set GEMFIRE=%gf%

@set GEMFIRE_JARS=%GEMFIRE%\lib\server-dependencies.jar
@if defined CLASSPATH set GEMFIRE_JARS=%GEMFIRE_JARS%;%CLASSPATH%

@if not defined GF_JAVA (
@REM %GF_JAVA% is not defined, assume it is on the PATH
@set GF_JAVA=java
)

@"%GF_JAVA%" %JAVA_ARGS% -classpath "%GEMFIRE_JARS%" com.gemstone.gemfire.internal.SystemAdmin %*
:done
@set scriptdir=
@set gf=
@set GEMFIRE_JARS=

