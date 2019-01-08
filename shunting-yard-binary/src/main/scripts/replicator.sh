#!/usr/bin/env bash

set -e


if [[ -z ${CIRCUS_TRAIN_HOME-} ]]; then
  echo "Environment variable CIRCUS_TRAIN_HOME is not set"
  exit -1
fi

if [[ -z $SHUNTING_YARD_HOME ]]; then
  #work out the script location
  SOURCE="${BASH_SOURCE[0]}"
  while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
    SCRIPT_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
    SOURCE="$(readlink "$SOURCE")"
    [[ $SOURCE != /* ]] && SOURCE="$SCRIPT_DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
  done
  SHUNTING_YARD_HOME="$( cd -P "$( dirname "$SOURCE" )" && cd .. && pwd )"
fi
echo "Using $SHUNTING_YARD_HOME"

if [[ -z ${HIVE_LIB-} ]]; then
  export HIVE_LIB=/usr/hdp/current/hive-client/lib
fi
if [[ -z ${HCAT_LIB-} ]]; then
  export HCAT_LIB=/usr/hdp/current/hive-webhcat/share/hcatalog
fi
if [[ -z ${HIVE_CONF_PATH-} ]]; then
  export HIVE_CONF_PATH=/etc/hive/conf
fi

LIBFB303_JAR=`ls $HIVE_LIB/libfb303-*.jar | tr '\n' ':'`

SHUNTING_YARD_LIBS=$SHUNTING_YARD_HOME/lib/*\
:$HIVE_LIB/hive-exec.jar\
:$HIVE_LIB/hive-metastore.jar\
:$LIBFB303_JAR\
:$HIVE_CONF_PATH/

if [[ -z ${SHUNTING_YARD_CLASSPATH-} ]]; then
  export SHUNTING_YARD_CLASSPATH=$SHUNTING_YARD_LIBS
else
  export SHUNTING_YARD_CLASSPATH=$SHUNTING_YARD_CLASSPATH:$SHUNTING_YARD_LIBS
fi

if [[ -z ${HADOOP_CLASSPATH-} ]]; then
  export HADOOP_CLASSPATH=`hadoop classpath`
fi

java -cp $SHUNTING_YARD_CLASSPATH:$HADOOP_CLASSPATH \
  com.hotels.shunting.yard.replicator.exec.MetaStoreEventReplication \
  "$@"

exit
