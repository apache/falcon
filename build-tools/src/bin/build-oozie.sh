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

VERSION=$1
BUILD_VERSION=$2
FORCE_BUILD=$3

echo "oozie version $OOZIE_VERSION"

if [ -z "${MAVEN_HOME}" ]
then
    export MVN_CMD=`which mvn`;
else
    export MVN_CMD=${MAVEN_HOME}/bin/mvn;
fi
echo "Using maven from " $MVN_CMD

if [[ ($FORCE_BUILD == 'false') && ( -f `$MVN_CMD help:effective-settings | grep localRepository | cut -d\> -f2 | cut -d\< -f1`/org/apache/oozie/oozie-webapp/$BUILD_VERSION/oozie-webapp-$BUILD_VERSION.war) ]]
then
    echo "Oozie already setup. skipping";
    exit 0;
fi

PKG_URL="http://archive.apache.org/dist/oozie/$VERSION/oozie-$VERSION.tar.gz"
PKG=oozie-$VERSION

mkdir -p ../target
pushd ../target
rm -rf oozie-*

curl -v $PKG_URL -o oozie-$VERSION.tgz
tar -xzvf oozie-$VERSION.tgz
rm oozie-$VERSION.tgz
cd $PKG

sed -i.bak s/$VERSION\<\\/version\>/$BUILD_VERSION\<\\/version\>/g pom.xml */pom.xml */*/pom.xml
patch -p0 < ../../build-tools/src/patches/oozie-site.patch

case $VERSION in
4.1.0 )
    ;;
4.2.0 )
    patch -p1 < ../../build-tools/src/patches/oozie-hadoop2-profile.patch
    ;;
esac

rm `find . -name 'pom.xml.bak'`

$MVN_CMD clean source:jar install -DjavaVersion=1.7 -DtargetJavaVersion=1.6 -DskipTests -Phadoop-2

popd
