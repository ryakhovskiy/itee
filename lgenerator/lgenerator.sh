#!/bin/sh

if [ -z "$JAVA_HOME" ]; then
  echo "No Java found. Please validate JAVA_HOME environment variable points to valid JAVA installation."
  exit 1
fi

CLASSPATH='-cp "lib/*" LGenForm'

eval "$JAVA_HOME/bin/javaw" "$CLASSPATH" "$@"
