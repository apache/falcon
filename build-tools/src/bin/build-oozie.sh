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
if [ $VERSION == '3.1.3-incubating' ]
then
    PKG_URL="http://archive.apache.org/dist/oozie/$VERSION/oozie-$VERSION-src.tar.gz"
fi

PKG=oozie-$VERSION

mkdir -p ../target
pushd ../target
rm -rf oozie-*

curl -v $PKG_URL -o oozie-$VERSION.tgz
tar -xzvf oozie-$VERSION.tgz
rm oozie-$VERSION.tgz
cd $PKG

case $VERSION in
3.2.0-incubating )
    sed -i.bak s/$VERSION\<\\/version\>/$BUILD_VERSION\<\\/version\>/g pom.xml */pom.xml */*/pom.xml
    patch -p0 < ../../build-tools/src/patches/oozie-site.patch
    patch -p0 < ../../build-tools/src/patches/OOZIE-674-v6-3.2.0.patch
    patch -p0 < ../../build-tools/src/patches/OOZIE-1465.patch
    patch -p0 < ../../build-tools/src/patches/OOZIE-882.patch
    ;;
3.3.0 )
    sed -i.bak s/$VERSION\<\\/version\>/$BUILD_VERSION\<\\/version\>/g pom.xml */pom.xml */*/pom.xml
    patch -p0 < ../../build-tools/src/patches/oozie-site.patch
    patch -p0 < ../../build-tools/src/patches/OOZIE-674-v6-3.2.0.patch
    patch -p0 < ../../build-tools/src/patches/OOZIE-1465.patch
    ;;
3.3.1 )
    sed -i.bak s/$VERSION\<\\/version\>/$BUILD_VERSION\<\\/version\>/g pom.xml */pom.xml */*/pom.xml
    patch -p0 < ../../build-tools/src/patches/oozie-site.patch
    patch -p0 < ../../build-tools/src/patches/OOZIE-674-v6-3.2.0.patch
    patch -p0 < ../../build-tools/src/patches/OOZIE-1465.patch
    ;;
3.3.2 )
    sed -i.bak s/$VERSION\<\\/version\>/$BUILD_VERSION\<\\/version\>/g pom.xml */pom.xml */*/pom.xml
    patch -p0 < ../../build-tools/src/patches/oozie-site.patch
    patch -p0 < ../../build-tools/src/patches/OOZIE-674-v6.patch
    patch -p0 < ../../build-tools/src/patches/OOZIE-1465-3.3.2.patch
    ;;
4* )
    sed -i.bak s/$VERSION\<\\/version\>/$BUILD_VERSION\<\\/version\>/g pom.xml */pom.xml */*/pom.xml
    patch -p0 < ../../build-tools/src/patches/oozie-site.patch
    patch -p1 --verbose < ../../build-tools/src/patches/OOZIE-1551-4.0.patch
    ;;
esac

rm `find . -name 'pom.xml.bak'`

$MVN_CMD clean install -DskipTests

popd
