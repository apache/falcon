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

if [ -d `mvn help:effective-settings | grep localRepository | cut -d\> -f2 | cut -d\< -f1`/org/apache/oozie/oozie-core/3.2.2 ]
then
    echo "Oozie already setup. skipping";
    exit 0;
fi

mkdir -p ../target
pushd ../target
rm -rf oozie-3.2.0-incubating*
curl -v "http://www.gtlib.gatech.edu/pub/apache/oozie/3.2.0-incubating/oozie-3.2.0-incubating.tar.gz" -o oozie-3.2.0-incubating.tgz
tar -xzvf oozie-3.2.0-incubating.tgz
cd oozie-3.2.0-incubating
pwd
patch -p0 < ../../oozie-3.2.0-incubating-el.patch
mvn clean install -DskipTests
rm -rf oozie-3.2.0-incubating*
popd

