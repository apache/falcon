#!/bin/sh

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

${BASEDIR}/generate.sh

hcat -e "DROP TABLE IF EXISTS in_table"
hcat -e "DROP TABLE IF EXISTS out_table"
hcat -e "CREATE TABLE in_table (word STRING, cnt INT) PARTITIONED BY (ds STRING);"
hcat -e "CREATE TABLE out_table (word STRING, cnt INT) PARTITIONED BY (ds STRING);"
for MINUTE in `seq -w 00 59`
do
    hcat -e "ALTER TABLE in_table ADD PARTITION (ds='2013-11-15-00-$MINUTE') LOCATION '/data/in/2013/11/15/00/$MINUTE';"
done
