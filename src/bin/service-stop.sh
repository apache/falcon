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

STOP_TIMEOUT=10
if [ -f $FALCON_PID_FILE ]
then
   PID=`cat $FALCON_PID_FILE`
   echo "Stopping $APP_TYPE running as $PID"
   kill -15 $PID
   WAIT=0
   while [ $(ps -p$PID -o pid=) > /dev/null ] && [ $WAIT -lt $STOP_TIMEOUT ]; do
             echo -n "."
             sleep 1
             (( WAIT++ ))
   done
   if [ $(ps -p$PID -o pid=) > /dev/null ]; then
     echo -e "\nWARN: $APP_TYPE did not stop after $STOP_TIMEOUT seconds : Killing with kill -9"
     kill -9 $PID
   fi
   rm -f $FALCON_PID_FILE
   echo "Stopped $APP_TYPE !"
else
   echo "pid file $FALCON_PID_FILE not present"
fi

