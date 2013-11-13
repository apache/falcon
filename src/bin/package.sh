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

if [ "${1}"x == "x" ]
then
  echo "Usage ${0} <<hadoop-version>>"
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
PACKAGE_HOME=${FALCON_SRC}/target/package

pushd ${FALCON_SRC}
echo "Building falcon & falcon-oozie-el-extensions ....."
mvn clean assembly:assembly -Dhadoop.version=${1} -DskipTests -DskipCheck=true > /dev/null
popd

mkdir -p ${PACKAGE_HOME}
pushd ${PACKAGE_HOME}
rm -rf oozie-*
echo "Getting oozie release tar ball of version 4.0.0 ..."
curl "http://www.apache.org/dist/oozie/4.0.0/oozie-4.0.0.tar.gz" -o oozie-4.0.0.tgz
tar -xzvf oozie-4.0.0.tgz 2> /dev/null
rm oozie-4.0.0.tgz
cd oozie-4.0.0

echo "Patching oozie with falcon extensions and marking version as 4.0.0 ..."
patch -p0 < ${FALCON_SRC}/build-tools/src/patch/oozie-1551-hadoop-2-profile.patch
patch -p0 < ${FALCON_SRC}/build-tools/src/patch/oozie-4.0.0-falcon.patch
patch -p0 < ${FALCON_SRC}/build-tools/src/patch/oozie-bundle-el-extension.patch

echo "Building oozie & creating tar ball ..."
bin/mkdistro.sh -DskipTests > /dev/null

echo "Falcon pacakge is available in ${FALCON_SRC}/target/falcon-<<version>>/falcon-<<version>>.tar.gz"
echo "Oozie pacakge is available in ${FALCON_SRC}/target/package/oozie-4.0.0/distro/target/oozie-4.0.0-distro.tar.gz"
popd
