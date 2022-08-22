@ECHO OFF

:: ---------------------------------------------------------------------
:: Locate a JDK installation directory which will be used to run the Perfmon.
:: Try (in order): ..\jre, JDK_HOME, JAVA_HOME.
:: ---------------------------------------------------------------------
IF EXIST "%~dp0\..\jre" SET JDK=%~dp0\..\jre
IF NOT "%JDK%" == "" GOTO jdk
IF EXIST "%JDK_HOME%" SET JDK=%JDK_HOME%
IF NOT "%JDK%" == "" GOTO jdk
IF EXIST "%JAVA_HOME%" SET JDK=%JAVA_HOME%
IF "%JDK%" == "" GOTO error

:jdk
SET JAVA_EXE=%JDK%\bin\java.exe
IF NOT EXIST "%JAVA_EXE%" SET JAVA_EXE=%JDK%\jre\bin\java.exe
IF NOT EXIST "%JAVA_EXE%" GOTO error

SET JRE=%JDK%
IF EXIST "%JRE%\jre" SET JRE=%JDK%\jre
SET BITS=
IF EXIST "%JRE%\lib\amd64" SET BITS=64

:: ---------------------------------------------------------------------
:: Ensure PERFMON_HOME points to the directory where the Perfmon is installed.
:: ---------------------------------------------------------------------
SET PERFMON_BIN_DIR=%~dp0
SET PERFMON_HOME=%PERFMON_BIN_DIR%\..

ECHO "BITS: %BITS%"

SET MEM_OPTS=-Xmx512m -Xms512m -Xmn96m -Xss1m -XX:+OptimizeStringConcat -XX:AutoBoxCacheMax=100000
IF %BITS%==64 SET MEM_OPTS=-server -XX:+UseParNewGC -XX:MaxNewSize=192m -XX:NewSize=192m -Xmx512m -Xms512m -XX:SurvivorRatio=128 -XX:MaxTenuringThreshold=0 -XX:+UseTLAB -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled -XX:+OptimizeStringConcat -XX:AutoBoxCacheMax=100000


:: ---------------------------------------------------------------------
:: Run the Perfmon.
:: ---------------------------------------------------------------------
SET OLD_PATH=%PATH%
SET PATH=%PERFMON_BIN_DIR%;%PATH%
SET CLASS_PATH=%PERFMON_HOME%/lib/*

"%JAVA_EXE%" %MEM_OPTS% -cp "%CLASS_PATH%" Bootstrap

SET PATH=%OLD_PATH%
GOTO end

:error
ECHO ERROR: cannot start Perfmon.
ECHO No JDK found. Please validate either, JDK_HOME or JAVA_HOME points to valid JDK installation.
ECHO
GOTO end

:end
PAUSE
