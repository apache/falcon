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
APP_TYPE=$1
. ${BASEDIR}/bin/falcon-config.sh 'server' "$APP_TYPE"

# make sure the process is not running
if [ -f $FALCON_PID_FILE ]; then
  if kill -0 `cat $FALCON_PID_FILE` > /dev/null 2>&1; then
    echo $APP_TYPE running as process `cat $FALCON_PID_FILE`.  Stop it first.
    exit 1
  fi
fi

mkdir -p $FALCON_LOG_DIR

pushd ${BASEDIR} > /dev/null

JAVA_PROPERTIES="$FALCON_OPTS $FALCON_PROPERTIES -Dfalcon.log.dir=$FALCON_LOG_DIR -Dfalcon.embeddedmq.data=$FALCON_DATA_DIR -Dfalcon.home=${FALCON_HOME_DIR} -Dconfig.location=$FALCON_CONF -Dfalcon.app.type=$APP_TYPE -Dfalcon.catalog.service.enabled=$CATALOG_ENABLED"
shift

while [[ ${1} =~ ^\-D ]]; do
  JAVA_PROPERTIES="${JAVA_PROPERTIES} ${1}"
  shift
done
TIME=`date +%Y%m%d%H%M%s`

nohup ${JAVA_BIN} ${JAVA_PROPERTIES} -cp ${FALCONCPPATH} org.apache.falcon.FalconServer -app ${BASEDIR}/server/webapp/${APP_TYPE} $* > "${FALCON_LOG_DIR}/$APP_TYPE.out.$TIME" 2>&1 < /dev/null &
echo $! > $FALCON_PID_FILE
popd > /dev/null

echo "$APP_TYPE started using hadoop version: " `${JAVA_BIN} -cp ${FALCONCPPATH} org.apache.hadoop.util.VersionInfo 2> /dev/null | head -1`
