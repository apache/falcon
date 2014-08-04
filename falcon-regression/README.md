
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.


falcon-regression
=================
This project had 2 modules : 

1. merlin
2. merlin-core

merlin had all the test for apache falcon project
merlin-core has al the utils used by merlin

Build Command : 
------------------

Fast Build : mvn clean install -DskipTests -DskipCheck=true -Phadoop-1
Regression build : mvn clean install -Phadoop-1
Profiles Supported: hadoop-1,hadoop-2
(hadoop-1 is by default for chd repo)
