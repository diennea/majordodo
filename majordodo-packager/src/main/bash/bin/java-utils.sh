#!/bin/bash
if [ $# -lt 1 ];
then
  echo "USAGE: $0 [-daemon] [jvmargs]"
  exit 1
fi

if [ -z "$BASE_DIR" ];
then
  BASE_DIR="`dirname \"$0\"`"
  BASE_DIR="`( cd \"$BASE_DIR/..\" && pwd )`"
fi

cd $BASE_DIR
. $BASE_DIR/bin/setenv.sh

CLASSPATH=

for file in $BASE_DIR/lib/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

if [ -z "$JAVA_OPTS" ]; then
  JAVA_OPTS=""
fi

JAVA="$JAVA_HOME/bin/java"

while [ $# -gt 0 ]; do
  COMMAND=$1
  case $COMMAND in
    -daemon)
      DAEMON_MODE="true"
      shift
      ;;
    *)
      break
      ;;
  esac
done
if [ "x$DAEMON_MODE" = "xtrue" ]; then
  CONSOLE_OUTPUT_FILE=$BASE_DIR/service.log
  nohup $JAVA -cp $CLASSPATH $JAVA_OPTS "$@" > "$CONSOLE_OUTPUT_FILE" 2>&1 < /dev/null &
  RETVAL=$?
else
  exec $JAVA -cp $CLASSPATH $JAVA_OPTS "$@"
  RETVAL=$?
fi
