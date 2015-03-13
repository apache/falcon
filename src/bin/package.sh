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

set -e
set -x

if [ $# -ne 2 ]
then
  echo "Usage ${0} <<hadoop-version>> <<oozie-version>>"
  exit 1
fi

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
BASEDIR=`cd ${BASEDIR};pwd`

FALCON_SRC=${BASEDIR}/../..

HADOOP_VERSION=$1
OOZIE_VERSION=$2

HADOOP_PROFILE="hadoop-""$(echo ${HADOOP_VERSION} | cut -d'.' -f 1)"

pushd ${FALCON_SRC}
echo "Builing oozie-el-extension and oozie"
mvn clean install -P $HADOOP_PROFILE -pl build-tools,hadoop-dependencies,oozie-el-extensions -am -Dhadoop.version=$HADOOP_VERSION -Doozie.version=$OOZIE_VERSION -Doozie.forcebuild=true -DskipTests
pushd target/oozie-$OOZIE_VERSION
bin/mkdistro.sh -DjavaVersion=1.7 -DtargetJavaVersion=1.6 -DskipTests
pushd distro/target/oozie-*
mkdir -p WEB-INF/lib
cp ${FALCON_SRC}/oozie-el-extensions/target/falcon-oozie-el-extension*.jar WEB-INF/lib/
jar uvf oozie-*/oozie.war WEB-INF/lib/*.jar
mkdir libext
cp ${FALCON_SRC}/hadoop-dependencies/target/dependency/*.jar libext
tar -zcvf ${FALCON_SRC}/target/oozie-$OOZIE_VERSION-distro.tar.gz oozie-*

popd
popd
mvn assembly:assembly -P $HADOOP_PROFILE -Dhadoop.version=$HADOOP_VERSION -Doozie.version=$OOZIE_VERSION -Doozie.forcebuild=true -DskipTests -DskipCheck=true

echo "Falcon pacakge is available in ${FALCON_SRC}/target/falcon-<<version>>-bin.tar.gz"
echo "Oozie pacakge is available in ${FALCON_SRC}/target/oozie-$OOZIE_VERSION/distro/target/oozie-$OOZIE_VERSION-distro.tar.gz"
popd
