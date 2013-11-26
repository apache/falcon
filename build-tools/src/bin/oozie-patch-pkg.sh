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

if [ -z "${MAVEN_HOME}" ]
then
    export MVN_CMD=`which mvn`;
else
    export MVN_CMD=${MAVEN_HOME}/bin/mvn;
fi
echo "Using maven from " $MVN_CMD

if [ -d `$MVN_CMD help:effective-settings | grep localRepository | cut -d\> -f2 | cut -d\< -f1`/org/apache/oozie/oozie-core/4.0.0-falcon ]
then
    echo "Oozie already setup. skipping";
    exit 0;
fi

mkdir -p ../target
pushd ../target
rm -rf oozie-4.0.0*
curl -v "http://www.apache.org/dist/oozie/4.0.0/oozie-4.0.0.tar.gz" -o oozie-4.0.0.tgz
tar -xzvf oozie-4.0.0.tgz
cd oozie-4.0.0
pwd

patch -p1 < ../../build-tools/src/patch/oozie-1551-hadoop-2-profile.patch
patch -p0 < ../../build-tools/src/patch/oozie-4.0.0-falcon.patch

$MVN_CMD clean install -DskipTests
cd ..
rm -rf oozie-4.0.0*
popd
