#!/bin/bash
# MajorDodo daemon start/stop script
#
# Comments to support chkconfig
# chkconfig: 2345 95 05
# description: Distributed Task Broker
#
# Configure BASE_DIR if you add this script to init.d
# If you move the service to a different directory or you edit
# the BASE_DIR/PIDFILENAME variables please remember to edit
# this script and/or the systemd init script

#BASE_DIR=
PIDFILENAME=java.pid
MAINCLASS=dodo.broker.BrokerMain


RETVAL=0

# BASE_DIR discovery
if [ -z "$BASE_DIR" ];
then
  BASE_DIR="`dirname \"$0\"`"
  BASE_DIR="`( cd \"$BASE_DIR\" && pwd )`"
fi

# Exit if the script is launched by init.d and BASE_DIR is not configured
if [ "$BASE_DIR" = "/etc/init.d" ];
then
  echo "To run as init.d script you need to configure BASE_DIR inside the script"
  RETVAL=1
fi

if [ -z "$BASE_DIR" ];
then
  echo "The path is not accessible to the script"
  RETVAL=1
fi

cd $BASE_DIR

# Commands

start() {
  echo "Starting service"
  if [ -e $BASE_DIR/$PIDFILENAME ];
  then
    PROCESS_PID=$(cat $BASE_DIR/$PIDFILENAME)
    if [ -n "$(ps h -p $PROCESS_PID)" ];
    then
      echo "Process $PROCESS_PID already running"
      RETVAL=1
    else
      rm $BASE_DIR/$PIDFILENAME
      echo "Removed stale pid file $BASE_DIR/$PIDFILENAME"
      exec $BASE_DIR/bin/java-utils.sh -daemon -Dpidfile=$PIDFILENAME $MAINCLASS
      RETVAL=$?
    fi
  else
    exec $BASE_DIR/bin/java-utils.sh -daemon -Dpidfile=$PIDFILENAME $MAINCLASS
    RETVAL=$?
  fi
}

stop() {
  if [ -e $BASE_DIR/$PIDFILENAME ];
  then
    PROCESS_PID=$(cat $BASE_DIR/$PIDFILENAME)
    echo "Stopping service, pid $PROCESS_PID"
    if [ -n "$(ps h -p $PROCESS_PID)" ];
    then
      command kill -SIGTERM $PROCESS_PID
      RETVAL=$?
    else
      rm $BASE_DIR/$PIDFILENAME
      echo "Removed stale pid file $BASE_DIR/$PIDFILENAME"
    fi
    i=10
    while [ -n "$(ps h -p $PROCESS_PID)" -a "$i" -gt "0" ];
    do
      echo "Waiting for $PROCESS_PID to die..."
      sleep 1
      i=$[$i-1]
    done
    if [ -n "$(ps h -p $PROCESS_PID)" ];
    then
      echo "Process $PROCESS_PID did not exist on request. Killing"
      command kill -SIGKILL $PROCESS_PID
      RETVAL=$?
      rm $BASE_DIR/$PIDFILENAME
      echo "Removed stale pid file $BASE_DIR/$PIDFILENAME"
    fi
  else
    echo "Pidfile $BASE_DIR/$PIDFILENAME does not exist"
    RETVAL=1
  fi
}

console() {
  if [ -e $BASE_DIR/$PIDFILENAME ];
  then
    PROCESS_PID=$(cat $BASE_DIR/$PIDFILENAME)
    if [ -n "$(ps h -p $PROCESS_PID)" ];
    then
      echo "Process $PROCESS_PID already running"
      RETVAL=1
    else
      exec $BASE_DIR/bin/java-utils.sh -Dpidfile=$PIDFILENAME $MAINCLASS
      RETVAL=$?
    fi
  else
    exec $BASE_DIR/bin/java-utils.sh -Dpidfile=$PIDFILENAME $MAINCLASS
    RETVAL=$?
  fi
}

kill() {
  if [ -e $BASE_DIR/$PIDFILENAME ];
  then
    PROCESS_PID=$(cat $BASE_DIR/$PIDFILENAME)
    echo "Killing service, pid $PROCESS_PID"
    command kill -SIGKILL $PROCESS_PID
    RETVAL=$?
    rm $BASE_DIR/$PIDFILENAME
    echo "Removed stale pid file $BASE_DIR/$PIDFILENAME"
  else
    echo "Pidfile $BASE_DIR/$PIDFILENAME does not exist"
    RETVAL=1
  fi
}

stacktrace() {
  if [ -e $BASE_DIR/$PIDFILENAME ];
  then
    PROCESS_PID=$(cat $BASE_DIR/$PIDFILENAME)
    echo "Requesting stacktrace dump, pid $PROCESS_PID"
    command kill -SIGQUIT $PROCESS_PID
    RETVAL=$?
  else
    echo "Pidfile $BASE_DIR/$PIDFILENAME does not exist"
    RETVAL=1
  fi
}

status() {
  if [ -e $BASE_DIR/$PIDFILENAME ];
  then
    PROCESS_PID=$(cat $BASE_DIR/$PIDFILENAME)
    if [ -n "$(ps h -p $PROCESS_PID)" ];
    then
      echo "Process is running, pid $PROCESS_PID"
    else
      echo "Pidfile $BASE_DIR/$PIDFILENAME does exist, but process is not running"
      rm $BASE_DIR/$PIDFILENAME
      echo "Removed stale Pidfile"
      RETVAL=1
    fi
  else
    echo "Process is not running"
    RETVAL=1
  fi
}

case "$1" in
        start)
                start
                ;;
        stop)
                stop
                ;;
        restart)
                stop
                start
                ;;
        console)
                console
                ;;
        kill)
                kill
                ;;
        stacktrace)
                stacktrace
                ;;
        status)
                status
                ;;
        *)
                echo $"Usage: $0 {start|stop|restart|console|kill|stacktrace|status}"
                RETVAL=1
esac
exit $RETVAL
