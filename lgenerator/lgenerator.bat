@ECHO ON

IF "%JAVA_HOME%" == "" GOTO error
IF NOT "%JAVA_HOME%" == "" GOTO jdk

:jdk
SET JAVA_EXE=%JAVA_HOME%\bin\javaw.exe
IF NOT EXIST "%JAVA_EXE%" SET JAVA_EXE=%JAVA_HOME%\jre\bin\javaw.exe
IF NOT EXIST "%JAVA_EXE%" GOTO error

SET MAIN_CLASS_NAME=LGenForm
SET CP=lib/*

ECHO "%JAVA_EXE%"
start "LGenerator" /B "%JAVA_EXE%" %ALL_VM_ARGS% -cp "%CP%" %MAIN_CLASS_NAME% %*

GOTO end

:error
ECHO ERROR: cannot start LGenerator.
ECHO No JAVA found. Please validate that JAVA_HOME points to valid JRE installation.
PAUSE

:end