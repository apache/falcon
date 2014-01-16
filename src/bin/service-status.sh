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

# test if the process is running
if [ -f $FALCON_PID_FILE ]; then
  if kill -0 `cat $FALCON_PID_FILE` > /dev/null 2>&1; then
    . ${BASEDIR}/bin/falcon admin -status
    if [ $? -eq 0 ]; then
        echo "$APP_TYPE process: `cat $FALCON_PID_FILE`"
        exit `cat $FALCON_PID_FILE`
    else
        exit -2
    fi
  fi
fi

echo $APP_TYPE is not running.
exit -1