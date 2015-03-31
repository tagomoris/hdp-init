#!/bin/bash

# inspired by init scripts of CDH4

# Starts a Hadoop hivemetastore
#
# chkconfig: 345 85 15
# description: Hadoop hivemetastore
#
### BEGIN INIT INFO
# Provides:          hdp-hivemetastore
# Short-Description: Hadoop hivemetastore
# Default-Start:     3 4 5
# Default-Stop:      0 1 2 6
# Required-Start:    $syslog $remote_fs
# Required-Stop:     $syslog $remote_fs
# Should-Start:
# Should-Stop:
### END INIT INFO

. /lib/lsb/init-functions
. /etc/default/hadoop

if [ -f /etc/default/hadoop-custom ] ; then
  . /etc/default/hadoop-custom
fi
if [ -f /etc/default/hive-metastore ] ; then
  . /etc/default/hive-metastore
fi

HIVE_COMMAND="metastore"
HIVE_IDENT_STRING="hive"
HIVE_LOGLEVEL=${HIVE_LOGLEVEL:-"INFO"}

##

RETVAL_SUCCESS=0

STATUS_RUNNING=0
STATUS_DEAD=1
STATUS_DEAD_AND_LOCK=2
STATUS_NOT_RUNNING=3
STATUS_OTHER_ERROR=102

ERROR_PROGRAM_NOT_INSTALLED=5
ERROR_PROGRAM_NOT_CONFIGURED=6

RETVAL=0
SLEEP_TIME=5
PROC_NAME="java"

HIVE_COMMAND_LONG_NAME="hive-"$HIVE_IDENT_STRING"-"$HIVE_COMMAND

DAEMON="hdp-"$HIVE_COMMAND
DESC="Hive "$HIVE_COMMAND
EXEC_PATH="/usr/bin/hive"
SVC_USER=$HIVE_IDENT_STRING
DAEMON_FLAGS=$HIVE_COMMAND
CONF_DIR=$HIVE_CONF_DIR
PIDFILE=$HIVE_PID_DIR"/"$HIVE_COMMAND_LONG_NAME".pid"
LOCKDIR="/var/lock/subsys"
LOCKFILE="$LOCKDIR/"$HIVE_COMMAND_LONG_NAME
WORKING_DIR="/var/lib/hive/metastore"

install -d -m 0755 -o $HIVE_USER -g $HADOOP_GROUP "$HIVE_PID_DIR" 1>/dev/null 2>&1 || :
[ -d "$LOCKDIR" ] || install -d -m 0755 $LOCKDIR 1>/dev/null 2>&1 || :

start() {
  [ -x $EXEC_PATH ] || exit $ERROR_PROGRAM_NOT_INSTALLED
  [ -d $CONF_DIR ] || exit $ERROR_PROGRAM_NOT_CONFIGURED
  log_success_msg "Starting ${DESC}: "

  LOG_FILE=$HIVE_LOG_DIR"/hive-"$HIVE_COMMAND".out"
  exec_env="HADOOP_OPTS=\"-Dhive.log.dir=$HIVE_LOG_DIR -Dhive.log.file=hive-$HIVE_COMMAND.log -Dhive.log.threshold=$HIVE_LOGLEVEL\""
  su -s /bin/sh $SVC_USER -c "cd $WORKING_DIR & $exec_env nohup nice -n 0 \
        $EXEC_PATH --service $HIVE_COMMAND $PORT \
            > $LOG_FILE 2>&1 < /dev/null & "'echo $! '"> $PIDFILE"

  # Some processes are slow to start
  sleep $SLEEP_TIME
  checkstatusofproc
  RETVAL=$?

  [ $RETVAL -eq $RETVAL_SUCCESS ] && touch $LOCKFILE
  return $RETVAL
}

stop() {
  log_success_msg "Stopping ${DESC}: "
  killproc -p $PIDFILE $PROC_NAME
  RETVAL=$?

  [ $RETVAL -eq $RETVAL_SUCCESS ] && rm -f $LOCKFILE $PIDFILE
}

restart() {
  stop
  start
}

checkstatusofproc(){
  pidofproc -p $PIDFILE $PROC_NAME > /dev/null
}

checkstatus(){
  checkstatusofproc
  status=$?

  case "$status" in
    $STATUS_RUNNING)
      log_success_msg "${DESC} is running"
      ;;
    $STATUS_DEAD)
      log_failure_msg "${DESC} is dead and pid file exists"
      ;;
    $STATUS_DEAD_AND_LOCK)
      log_failure_msg "${DESC} is dead and lock file exists"
      ;;
    $STATUS_NOT_RUNNING)
      log_failure_msg "${DESC} is not running"
      ;;
    *)
      log_failure_msg "${DESC} status is unknown"
      ;;
  esac
  return $status
}

condrestart(){
  [ -e $LOCKFILE ] && restart || :
}

check_for_root() {
  if [ $(id -ur) -ne 0 ]; then
    echo 'Error: root user required'
    echo
    exit 1
  fi
}

service() {
  case "$1" in
    start)
      check_for_root
      start
      ;;
    stop)
      check_for_root
      stop
      ;;
    status)
      checkstatus
      RETVAL=$?
      ;;
    restart)
      check_for_root
      restart
      ;;
    *)
      echo $"Usage: $0 {start|stop|status|restart}"
      exit 1
  esac
}

service "$1"

exit $RETVAL
