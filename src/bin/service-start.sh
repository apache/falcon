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

set -e

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


mkdir -p ${BASEDIR}/logs

if test -z ${JAVA_HOME}
then
    JAVA_BIN=java
else
    JAVA_BIN=${JAVA_HOME}/bin/java
fi

pushd ${BASEDIR} > /dev/null

APP_TYPE=$1
if [ ! -d ${BASEDIR}/server/webapp/$APP_TYPE/WEB-INF ]; then
  mkdir -p ${BASEDIR}/server/webapp/$APP_TYPE
  cd ${BASEDIR}/server/webapp/$APP_TYPE
  jar -xf ../$APP_TYPE.war
  cd -
fi

FALCONCPPATH="$FALCON_CONF:${BASEDIR}/conf:${BASEDIR}/server/webapp/$APP_TYPE/WEB-INF/classes:"
for i in "${BASEDIR}/server/webapp/$APP_TYPE/WEB-INF/lib/"*.jar; do
  FALCONCPPATH="${FALCONCPPATH}:$i"
done

HADOOPDIR=`which hadoop`
if [ "$HADOOPDIR" != "" ]; then
  echo "Hadoop is installed, adding hadoop classpath to falcon classpath"
  FALCONCPPATH="${FALCONCPPATH}:`hadoop classpath`"
elif [ "$HADOOP_HOME" != "" ]; then
  echo "Hadoop home is set, adding ${HADOOP_HOME}/lib/* into falcon classpath"
  for i in "${HADOOP_HOME}/lib/"*.jar; do
    FALCONCPPATH="${FALCONCPPATH}:$i"
  done
else
  echo "Could not find installed hadoop and HADOOP_HOME is not set."
  echo "Using the default jars bundled in ${BASEDIR}/hadooplibs/"
  for i in "${BASEDIR}/hadooplibs/"*.jar; do
    FALCONCPPATH="${FALCONCPPATH}:$i"
  done
fi

if [ -z "$FALCON_CONF" ]; then
  CONF_PATH=${BASEDIR}/conf
else
  CONF_PATH=$FALCON_CONF
fi
 
JAVA_PROPERTIES="$FALCON_OPTS $FALCON_PROPERTIES -Dfalcon.embeddedmq.data=${BASEDIR}/logs/data -Dfalcon.home=${BASEDIR} -Dconfig.location=$CONF_PATH"
shift

while [[ ${1} =~ ^\-D ]]; do
  JAVA_PROPERTIES="${JAVA_PROPERTIES} ${1}"
  shift
done
TIME=`date +%Y%m%d%H%M%s`


nohup ${JAVA_BIN} ${JAVA_PROPERTIES} -cp ${FALCONCPPATH} org.apache.falcon.Main -app ${BASEDIR}/server/webapp/*.war $* 2> ${BASEDIR}/logs/$APP_TYPE.out.$TIME &
echo $! > ${BASEDIR}/logs/$APP_TYPE.pid
popd > /dev/null

echo "Falcon started using hadoop version: " `${JAVA_BIN} ${JAVA_PROPERTIES} -cp ${FALCONCPPATH} org.apache.hadoop.util.VersionInfo | head -1`
