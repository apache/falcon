#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

HDFS=$1
if [ "${HDFS}"x == "x" ]
then
  echo "Usage ${0} <<hdfs-end-point>>"
  exit 1
fi
echo "HDFS end point:" $HDFS

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
BASEDIR=`cd ${BASEDIR};pwd`

rm -rf generated-data
YEAR=`date +%Y`
MONTH=`date +m`
DAY=`date +%d`
HOUR=`date +%H`

DELIM='\t'
input=(first second third fourth fifth)
for MINUTE in `seq -w 00 59`
do
    mkdir -p generated-data/00/$MINUTE/
    word=${input[$RANDOM % 5]}
    cnt=`expr $RANDOM % 10`
    echo -e "$word$DELIM$cnt" > generated-data/00/$MINUTE/data
done

hadoop fs -fs $HDFS -mkdir /data/in/2013/11/15/
hadoop fs -put generated-data/00 /data/in/2013/11/15/ 
rm -rf generated-data
