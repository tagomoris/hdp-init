#!/bin/bash

# inspired by init scripts of CDH4

# Starts a Hadoop historyserver
#
# chkconfig: 345 85 15
# description: Hadoop historyserver
#
### BEGIN INIT INFO
# Provides:          hdp-historyserver
# Short-Description: Hadoop historyserver
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
# HADOOP_CONF_DIR is in hadoop-custom
MAPRED_LOG_DIR="/var/log/hadoop/mapreduce"
MAPRED_PID_DIR="/var/run/hadoop/mapreduce"
HADOOP_MAPRED_LOG_DIR=$MAPRED_LOG_DIR
HADOOP_MAPRED_PID_DIR=$MAPRED_PID_DIR

HADOOP_MAPRED_IDENT_STRING=$MAPRED_USER

# HADOOP_JHS_LOGGER  Hadoop JobSummary logger.
### export HADOOP_MAPRED_ROOT_LOGGER=${HADOOP_MAPRED_ROOT_LOGGER:-INFO,RFA}
### export HADOOP_JHS_LOGGER=${HADOOP_JHS_LOGGER:-INFO,JSA}

# HADOOP_MAPRED_NICENESS The scheduling priority for daemons. Defaults to 0.

MAPRED_COMMAND="historyserver"

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

DAEMON="hadoop-mapreduce-"$MAPRED_COMMAND
DESC="Hadoop historyserver"
EXEC_PATH="/usr/lib/hadoop-mapreduce/sbin/mr-jobhistory-daemon.sh"
SVC_USER=$MAPRED_USER
DAEMON_FLAGS=$MAPRED_COMMAND
CONF_DIR=$HADOOP_CONF_DIR
PIDFILE=$HADOOP_MAPRED_PID_DIR/mapred-$HADOOP_MAPRED_IDENT_STRING-$MAPRED_COMMAND.pid
LOCKDIR="/var/lock/subsys"
LOCKFILE="$LOCKDIR/hadoop-mapreduce-historyserver"
WORKING_DIR="/var/lib/hadoop-mapreduce"

install -d -m 0755 -o $MAPRED_USER -g $HADOOP_GROUP $HADOOP_MAPRED_PID_DIR 1>/dev/null 2>&1 || :
[ -d "$LOCKDIR" ] || install -d -m 0755 $LOCKDIR 1>/dev/null 2>&1 || :

start() {
  [ -x $EXEC_PATH ] || exit $ERROR_PROGRAM_NOT_INSTALLED
  [ -d $CONF_DIR ] || exit $ERROR_PROGRAM_NOT_CONFIGURED
  log_success_msg "Starting ${DESC}: "

  su -s /bin/bash $SVC_USER -c "cd $WORKING_DIR && $EXEC_PATH --config '$CONF_DIR' start $DAEMON_FLAGS"

  # Some processes are slow to start
  sleep $SLEEP_TIME
  checkstatusofproc
  RETVAL=$?

  [ $RETVAL -eq $RETVAL_SUCCESS ] && touch $LOCKFILE
  return $RETVAL
}


stop() {
  log_success_msg "Stopping ${DESC}: "
  su -s /bin/bash $SVC_USER -c "$EXEC_PATH --config '$CONF_DIR' stop $DAEMON_FLAGS"
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
    condrestart|try-restart)
      check_for_root
      condrestart
      ;;
    *)
      echo $"Usage: $0 {start|stop|status|restart|try-restart|condrestart}"
      exit 1
  esac
}

service "$1"

exit $RETVAL
