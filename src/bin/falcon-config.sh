#!/bin/bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License. See accompanying LICENSE file.
#

# resolve links - $0 may be a softlink
PRG="${0}"

while [ -h "${PRG}" ]; do
  ls=`ls -ld "${PRG}"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '/.*' > /dev/null; then
    PRG="$link"
  else
    PRG=`dirname "${PRG}"`/"$link"
  fi
done

BASEDIR=`dirname ${PRG}`
BASEDIR=`cd ${BASEDIR}/..;pwd`

if [ -z "$FALCON_CONF" ]; then
  FALCON_CONF=${BASEDIR}/conf
fi
export FALCON_CONF

if [ -f "${FALCON_CONF}/falcon-env.sh" ]; then
  . "${FALCON_CONF}/falcon-env.sh"
fi

if test -z ${JAVA_HOME}
then
    JAVA_BIN=`which java`
    JAR_BIN=`which jar`
else
    JAVA_BIN=${JAVA_HOME}/bin/java
    JAR_BIN=${JAVA_HOME}/bin/jar
fi
export JAVA_BIN

if [ ! -e $JAVA_BIN ] || [ ! -e $JAR_BIN ]; then
  echo "$JAVA_BIN and/or $JAR_BIN not found on the system. Please make sure java and jar commands are available."
  exit 1
fi

# default the heap size to 1GB
DEFAULT_JAVA_HEAP_MAX=-Xmx1024m
FALCON_OPTS="$DEFAULT_JAVA_HEAP_MAX $FALCON_OPTS"

type="$1"
shift
case $type in
  client)
    # set the client class path
    FALCONCPPATH="$FALCON_CONF:${BASEDIR}/client/lib/*"
    for i in `ls ${BASEDIR}/server/webapp`; do
      FALCONCPPATH="${FALCONCPPATH}:${i}/WEB-INF/lib/*"
    done
    FALCON_OPTS="$FALCON_OPTS $FALCON_CLIENT_OPTS $FALCON_CLIENT_HEAP"
  ;;
  server)
    app="$1"
    if [ 'prism' == "$app" ]; then
      FALCON_OPTS="$FALCON_OPTS $FALCON_PRISM_OPTS $FALCON_PRISM_HEAP"
    elif [ 'falcon' == "$app" ]; then
      FALCON_OPTS="$FALCON_OPTS $FALCON_SERVER_OPTS $FALCON_SERVER_HEAP"
    else
      echo "Invalid option for app: ${app}. Valid choices are falcon and prism"
      exit 1
    fi
    FALCONCPPATH="$FALCON_CONF" 
    HADOOPDIR=`which hadoop`
    if [ "$HADOOPDIR" != "" ]; then
      echo "Hadoop is installed, adding hadoop classpath to falcon classpath"
      FALCONCPPATH="${FALCONCPPATH}:`hadoop classpath`"
    elif [ "$HADOOP_HOME" != "" ]; then
      echo "Hadoop home is set, adding ${HADOOP_HOME}/lib/* into falcon classpath"
      FALCONCPPATH="${FALCONCPPATH}:${HADOOP_HOME}/lib/*"
    else
      echo "Could not find installed hadoop and HADOOP_HOME is not set."
      echo "Using the default jars bundled in ${BASEDIR}/hadooplibs/"
      FALCONCPPATH="${FALCONCPPATH}:${BASEDIR}/hadooplibs/*"
    fi
    FALCON_EXPANDED_WEBAPP_DIR=${FALCON_EXPANDED_WEBAPP_DIR:-${BASEDIR}/server/webapp}
    export FALCON_EXPANDED_WEBAPP_DIR
    # set the server classpath
    if [ ! -d ${FALCON_EXPANDED_WEBAPP_DIR}/$app/WEB-INF ]; then
      mkdir -p ${FALCON_EXPANDED_WEBAPP_DIR}/$app
      cd ${FALCON_EXPANDED_WEBAPP_DIR}/$app
      $JAR_BIN -xf ${BASEDIR}/server/webapp/$app.war
      cd -
    fi
    FALCONCPPATH="${FALCONCPPATH}:${FALCON_EXPANDED_WEBAPP_DIR}/$app/WEB-INF/classes"
    FALCONCPPATH="${FALCONCPPATH}:${FALCON_EXPANDED_WEBAPP_DIR}/$app/WEB-INF/lib/*:${BASEDIR}/libext/*"
    
    # log and pid dirs for applications
    FALCON_LOG_DIR="${FALCON_LOG_DIR:-$BASEDIR/logs}"
    export FALCON_LOG_DIR
    FALCON_PID_DIR="${FALCON_PID_DIR:-$BASEDIR/logs}"
    # create the pid dir if its not there
    [ -w "$FALCON_PID_DIR" ] ||  mkdir -p "$FALCON_PID_DIR"
    export FALCON_PID_DIR
    FALCON_PID_FILE=${FALCON_PID_DIR}/${app}.pid
    export FALCON_PID_FILE
    FALCON_DATA_DIR=${FALCON_DATA_DIR:-${BASEDIR}/data}
    FALCON_HOME_DIR="${FALCON_HOME_DIR:-$BASEDIR}"
    export FALCON_HOME_DIR
  ;;
  *)
    echo "Invalid option for type: $type"
    exit 1
  ;;
esac
export FALCONCPPATH
export FALCON_OPTS
