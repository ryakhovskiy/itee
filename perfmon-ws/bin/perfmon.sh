#!/bin/sh

message()
{
  TITLE="Cannot start Perfmon"
  if [ -n "`which zenity`" ]; then
    zenity --error --title="$TITLE" --text="$1"
  elif [ -n "`which kdialog`" ]; then
    kdialog --error --title "$TITLE" "$1"
  elif [ -n "`which xmessage`" ]; then
    xmessage -center "ERROR: $TITLE: $1"
  elif [ -n "`which notify-send`" ]; then
    notify-send "ERROR: $TITLE: $1"
  else
    echo "ERROR: $TITLE\n$1"
  fi
}

UNAME=`which uname`
GREP=`which egrep`
GREP_OPTIONS=""
CUT=`which cut`
READLINK=`which readlink`
XARGS=`which xargs`
DIRNAME=`which dirname`
MKTEMP=`which mktemp`
RM=`which rm`

if [ -z "$UNAME" -o -z "$GREP" -o -z "$CUT" -o -z "$MKTEMP" -o -z "$RM" ]; then
  message "Required tools are missing - check beginning of \"$0\" file for details."
  exit 1
fi

OS_TYPE=`"$UNAME" -s`

# Check Java is Installed or Not
if [ -n "$JDK_HOME" -a -x "$JDK_HOME/bin/java" ]; then
  JDK="$JDK_HOME"
elif [ -n "$JAVA_HOME" -a -x "$JAVA_HOME/bin/java" ]; then
  JDK="$JAVA_HOME"
else
  JAVA_BIN_PATH=`which java`
  if [ -n "$JAVA_BIN_PATH" ]; then
    if [ "$OS_TYPE" = "FreeBSD" -o "$OS_TYPE" = "MidnightBSD" ]; then
      JAVA_LOCATION=`JAVAVM_DRYRUN=yes java | "$GREP" '^JAVA_HOME' | "$CUT" -c11-`
      if [ -x "$JAVA_LOCATION/bin/java" ]; then
        JDK="$JAVA_LOCATION"
      fi
    elif [ "$OS_TYPE" = "SunOS" ]; then
      JAVA_LOCATION="/usr/jdk/latest"
      if [ -x "$JAVA_LOCATION/bin/java" ]; then
        JDK="$JAVA_LOCATION"
      fi
    elif [ "$OS_TYPE" = "Darwin" ]; then
      JAVA_LOCATION=`/usr/libexec/java_home`
      if [ -x "$JAVA_LOCATION/bin/java" ]; then
        JDK="$JAVA_LOCATION"
      fi
    fi

    if [ -z "$JDK" -a -x "$READLINK" -a -x "$XARGS" -a -x "$DIRNAME" ]; then
      JAVA_LOCATION=`"$READLINK" -f "$JAVA_BIN_PATH"`
      case "$JAVA_LOCATION" in
        */jre/bin/java)
          JAVA_LOCATION=`echo "$JAVA_LOCATION" | "$XARGS" "$DIRNAME" | "$XARGS" "$DIRNAME" | "$XARGS" "$DIRNAME"`
          if [ ! -d "$JAVA_LOCATION/bin" ]; then
            JAVA_LOCATION="$JAVA_LOCATION/jre"
          fi
          ;;
        *)
          JAVA_LOCATION=`echo "$JAVA_LOCATION" | "$XARGS" "$DIRNAME" | "$XARGS" "$DIRNAME"`
          ;;
      esac
      if [ -x "$JAVA_LOCATION/bin/java" ]; then
        JDK="$JAVA_LOCATION"
      fi
    fi
  fi
fi

JAVA_BIN="$JDK/bin/java"
if [ ! -x "$JAVA_BIN" ]; then
  JAVA_BIN="$JDK/jre/bin/java"
fi

if [ -z "$JDK" ] || [ ! -x "$JAVA_BIN" ]; then
  message "No JDK found. Please validate either JDK_HOME or JAVA_HOME environment variable points to valid JDK/JRE installation."
  exit 1
fi

# Check Current Version of Java
VERSION_LOG=`"$MKTEMP" -t java.version.log.XXXXXX`
JAVA_TOOL_OPTIONS= "$JAVA_BIN" -version 2> "$VERSION_LOG"
"$GREP" "64-Bit|x86_64|amd64" "$VERSION_LOG" > /dev/null
BITS=$?
"$RM" -f "$VERSION_LOG"
if [ $BITS -eq 0 ]; then
  BITS="64"
else
  BITS=""
fi

# Get Home Directory
SCRIPT_LOCATION=$0
if [ -x "$READLINK" ]; then
  while [ -L "$SCRIPT_LOCATION" ]; do
    SCRIPT_LOCATION=`"$READLINK" -e "$SCRIPT_LOCATION"`
  done
fi
export PERFMON_HOME=`dirname "$SCRIPT_LOCATION"`/..
PERFMON_BIN_HOME=`dirname "$SCRIPT_LOCATION"`

# setting Memory arguments and Classpath
if [ $BITS="64" ]; then
    mem_opts="-server -XX:+UseParNewGC -XX:MaxNewSize=256m -XX:NewSize=256m -Xmx768m -XX:SurvivorRatio=128 -XX:MaxTenuringThreshold=0 -XX:+UseTLAB -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled -XX:+OptimizeStringConcat -XX:+UseStringCache -XX:AutoBoxCacheMax=100000"
else
    mem_opts="-Xmx768m -Xmn128m -Xss1m -XX:+OptimizeStringConcat -XX:+UseStringCache -XX:AutoBoxCacheMax=100000"
fi

classpath="$PERFMON_HOME/lib/*"

# run jar file
$JAVA_BIN $mem_opts -cp "$classpath" org.kr.itee.perfmon.ws.Bootstrap
