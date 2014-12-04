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


Falcon Regression
=================
This project had 2 modules : 

1. merlin: it has all the system test for falcon
2. merlin-core: it has all the utils used by merlin

Requirements
------------
In addition to falcon server and prism, running full falcon regression requires three clusters.
Each of these clusters must have:

- hadoop
- oozie
- hive
- hcat
For specific tests it may be possible to run it without all clusters and components.

Prior to running tests Merlin.properties needs to be created and populated with cluster details.

Configuring Merlin.properties
-----------------------------
Merlin.properties must be created before running falcon regression tests.
The file must be created at the location:

    falcon/falcon-regression/merlin/src/main/resources/Merlin.properties

Populate it with prism related properties:

    #prism properties
    prism.oozie_url = http://node-1.example.com:11000/oozie/
    prism.oozie_location = /usr/lib/oozie/bin
    prism.qa_host = node-1.example.com
    prism.service_user = falcon
    prism.hadoop_url = node-1.example.com:8020
    prism.hadoop_location = /usr/lib/hadoop/bin/hadoop
    prism.hostname = http://node-1.example.com:15000
    prism.storeLocation = hdfs://node-1.example.com:8020/apps/falcon

Specify the clusters that you would be using for testing:

    servers = cluster1,cluster2,cluster3

For each cluster specify properties:

    #cluster1 properties
    cluster1.oozie_url = http://node-1.example.com:11000/oozie/
    cluster1.oozie_location = /usr/lib/oozie/bin
    cluster1.qa_host = node-1.example.com
    cluster1.service_user = falcon
    cluster1.password = rgautam
    cluster1.hadoop_url = node-1.example.com:8020
    cluster1.hadoop_location = /usr/lib/hadoop/bin/hadoop
    cluster1.hostname = http://node-1.example.com:15000
    cluster1.cluster_readonly = webhdfs://node-1.example.com:50070
    cluster1.cluster_execute = node-1.example.com:8032
    cluster1.cluster_write = hdfs://node-1.example.com:8020
    cluster1.activemq_url = tcp://node-1.example.com:61616?daemon=true
    cluster1.storeLocation = hdfs://node-1.example.com:8020/apps/falcon
    cluster1.colo = default
    cluster1.namenode.kerberos.principal = nn/node-1.example.com@none
    cluster1.hive.metastore.kerberos.principal = hive/node-1.example.com@none
    cluster1.hcat_endpoint = thrift://node-1.example.com:9083
    cluster1.service_stop_cmd = /usr/lib/falcon/bin/falcon-stop
    cluster1.service_start_cmd = /usr/lib/falcon/bin/falcon-start

Setting up HDFS Dirs
--------------------
On all cluster as user that started falcon server do:

    hdfs dfs -mkdir -p  /tmp/falcon-regression-staging
    hdfs dfs -chmod 777 /tmp/falcon-regression-staging
    hdfs dfs -mkdir -p  /tmp/falcon-regression-working
    hdfs dfs -chmod 755 /tmp/falcon-regression-working

Running Tests
-------------
After creating Merlin.properties file. You can run the following commands to run the tests.

    cd falcon-regression
    mvn clean test -Phadoop-2

Profiles Supported: hadoop-2

To run a specific test:

    mvn clean test -Phadoop-2 -Dtest=EmbeddedPigScriptTest

If you want to use specific version of any component, they can be specified using -D, for eg:

    mvn clean test -Phadoop-2 -Doozie.version=4.1.0 -Dhadoop.version=2.6.0

Security Tests:
---------------
ACL tests require multiple user account setup:

    other.user.name=root
    falcon.super.user.name=falcon
    falcon.super2.user.name=falcon2

ACL tests also require group name of the current user:

    current_user.group.name=users

For testing with kerberos set keytabs properties for different users:

    current_user_keytab=/home/qa/hadoopqa/keytabs/qa.headless.keytab
    falcon.super.user.keytab=/home/qa/hadoopqa/keytabs/falcon.headless.keytab
    falcon.super2.user.keytab=/home/qa/hadoopqa/keytabs/falcon2.headless.keytab
    other.user.keytab=/home/qa/hadoopqa/keytabs/root.headless.keytab

Testing on Windows
------------------
Some tests switch user to run commands as a different user. Location of binary to switch user is
configurable:

    windows.su.binary=ExecuteAs.exe

Automatic capture of oozie logs
-------------------------------
For full falcon regression runs. It might be desirable to pull all oozie job
info and logs at the end of the test. This can be done by configuring Merlin.properties:

    log.capture.oozie = true
    log.capture.oozie.skip_info = false
    log.capture.oozie.skip_log = true
    log.capture.location = ../
