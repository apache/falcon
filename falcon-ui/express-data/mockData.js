/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
(function () {
  'use strict';

  function findByNameInList(type, name) {
    var i;
    for (i = 0; i < entitiesList[type].entity.length; i++) {
      if (entitiesList[type].entity[i]["name"] === name) {
        return i;
      }
    }
  };

  function findByStartEnd(type, start, end) {
    for (var i = 0; i < instancesList[type].length; i++) {
      if (instancesList[type][i].startTime === start && instancesList[type][i].endTime === end) {
        return i;
      }
    }
  };

  var entitiesList = {
    cluster : {
      "entity":[
        {"type":"cluster","name":"completeCluster","status":"SUBMITTED","tags":{"tag":["uruguay=mvd","argentina=bsas","mexico=mex", "usa=was"]}},
        {"type":"cluster","name":"primaryCluster","status":"SUBMITTED"}
      ]
    },
    feed: {
      "entity":[
        {"type":"FEED","name":"feedOne","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}, "clusters": {"cluster":"primaryCluster"}},
        {"type":"FEED","name":"feedTwo","status":"RUNNING","tags":{"tag":["owner=USMarketing","classification=Secure","externalSource=USProdEmailServers","externalTarget=BITools"]}, "clusters": {"cluster":["SampleCluster1","primaryCluster"]}},
        {"type":"FEED","name":"feedTree","status":"SUBMITTED","tags":{"tag":"externalSystem=USWestEmailServers"}},
        {"type":"FEED","name":"feedFour","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feedFive","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feedSix","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feedSeven","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feedEight","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feedNine","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feedTen","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feed11","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feed12","status":"RUNNING","tags":{"tag":["owner=USMarketing","classification=Secure"]}},
        {"type":"FEED","name":"feed13","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feed14","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feed15","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feed16","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feed17","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feed18","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feed19","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feed20","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feed21","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feed22","status":"RUNNING","tags":{"tag":["owner=USMarketing"]}},
        {"type":"FEED","name":"feed23","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feed24","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feed25","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feed26","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feed27","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feed28","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feed29","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feed30","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feed31","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feed32","status":"RUNNING","tags":{"tag":["owner=USMarketing"]}},
        {"type":"FEED","name":"feed33","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feed34","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feed35","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feed36","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feed37","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feed38","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feed39","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feed40","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feed41","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feed42","status":"RUNNING","tags":{"tag":["owner=USMarketing"]}},
        {"type":"FEED","name":"feed43","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feed44","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feed45","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feed46","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feed47","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feed48","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feed49","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feed50","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feed51","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feed52","status":"RUNNING","tags":{"tag":["owner=USMarketing"]}},
        {"type":"FEED","name":"feed53","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feed54","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feed55","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feed56","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feed57","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feed58","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feed59","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feed60","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feed61","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feed62","status":"RUNNING","tags":{"tag":["owner=USMarketing"]}},
        {"type":"FEED","name":"feed63","status":"SUBMITTED","tags":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}}
      ]
    },
    process:{"entity":[
      {"type":"process","name":"processOne","status":"SUBMITTED","tags":{"tag":["pipeline=churnAnalysisDataPipeline","owner=ETLGroup","montevideo=mvd"]}},
      {"type":"process","name":"processTwo","status":"SUBMITTED","tags":{"tag":["pipeline=churnAnalysisDataPipeline","owner=ETLGroup","externalSystem=USWestEmailServers"]}},
      {"type":"process","name":"hdfs-mirror-test","status":"SUBMITTED","tags":{"tag":["_falcon_mirroring_type=HDFS"]}},
      {"type":"process","name":"hdfs-azure-mirror-test","status":"SUBMITTED","tags":{"tag":["_falcon_mirroring_type=HDFS","runsOn=target"]}},
      {"type":"process","name":"hdfs-s3-mirror-test","status":"SUBMITTED","tags":{"tag":["_falcon_mirroring_type=HDFS","runsOn=target"]}},
      {"type":"process","name":"hive-databases-mirror-test","status":"SUBMITTED","tags":{"tag":["_falcon_mirroring_type=HIVE","runsOn=target"]}},
      {"type":"process","name":"hive-db-tables-mirror-test","status":"SUBMITTED","tags":{"tag":["_falcon_mirroring_type=HIVE","runsOn=source"]}},
      {"type":"process","name":"mirror4","status":"SUBMITTED","tags":{"tag":["_falcon_mirroring_type=HDFS","runsOn=source"]}}
    ]}
  },
    definitions = {
      CLUSTER: {
        completeCluster : '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' +
          '<cluster name="completeCluster" description="desc" colo="colo" xmlns="uri:falcon:cluster:0.1">' +
            '<tags>uruguay=mvd,argentina=bsas</tags>' +
            '<interfaces>' +
              '<interface type="readonly" endpoint="hftp://sandbox.hortonworks.com:50070" version="2.2.0"/>' +
              '<interface type="write" endpoint="hdfs://sandbox.hortonworks.com:8020" version="2.2.0"/>' +
              '<interface type="execute" endpoint="sandbox.hortonworks.com:8050" version="2.2.0"/>' +
              '<interface type="workflow" endpoint="http://sandbox.hortonworks.com:11000/oozie/" version="4.0.0"/>' +
              '<interface type="messaging" endpoint="tcp://sandbox.hortonworks.com:61616?daemon=true" version="5.1.6"/>' +
              '<interface type="registry" endpoint="thrift://localhost:9083" version="0.11.0"/>' +
            '</interfaces>' +
            '<locations>' +
              '<location name="staging" path="/apps/falcon/backupCluster/staging"/>' +
              '<location name="temp" path="/tmp"/>' +
              '<location name="working" path="/apps/falcon/backupCluster/working"/>' +
            '</locations>' +
            '<ACL owner="ambari-qa" group="users" permission="0755"/>' +
            '<properties>' +
              '<property name="this" value="property"/>' +
            '</properties>' +
          '</cluster>',
        primaryCluster: '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' +
          '<cluster name="primaryCluster" description="oregonHadoopCluster" colo="USWestOregon" xmlns="uri:falcon:cluster:0.1">' +
            '<interfaces>' +
              '<interface type="readonly" endpoint="hftp://sandbox.hortonworks.com:50070" version="2.2.0"/>' +
              '<interface type="write" endpoint="hdfs://sandbox.hortonworks.com:8020" version="2.2.0"/>' +
              '<interface type="execute" endpoint="sandbox.hortonworks.com:8050" version="2.2.0"/>' +
              '<interface type="workflow" endpoint="http://sandbox.hortonworks.com:11000/oozie/" version="4.0.0"/>' +
              '<interface type="messaging" endpoint="tcp://sandbox.hortonworks.com:61616?daemon=true" version="5.1.6"/>' +
            '</interfaces>' +
            '<locations>' +
              '<location name="staging" path="/apps/falcon/primaryCluster/staging"/>' +
              '<location name="temp" path="/tmp"/>' +
              '<location name="working" path="/apps/falcon/primaryCluster/working"/>' +
            '</locations>' +
          '</cluster>'
      },
      FEED: {
        feedOne: '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' +
          '<feed name="feedOne" description="Raw customer email feed" xmlns="uri:falcon:feed:0.1">' +
            '<tags>externalSystem=USWestEmailServers,classification=secure</tags>' +
            '<frequency>hours(1)</frequency>' +
            '<timezone>GMT+00:00</timezone>' +
            '<late-arrival cut-off="hours(4)"/>' +
            '<clusters>' +
              '<cluster name="primaryCluster" type="source">' +
                '<validity start="2014-02-28T00:00Z" end="2016-04-01T00:00Z"/>' +
                '<retention limit="days(90)" action="delete"/>' +
                '<locations>' +
                  '<location type="data" path="/"/>' +
                  '<location type="stats" path="/"/>' +
                  '<location type="meta" path="/"/>' +
                '</locations>' +
              '</cluster>' +
            '</clusters>' +
            '<locations>' +
              '<location type="data" path="hdfs://sandbox.hortonworks.com:8020/"/>' +
              '<location type="stats" path="/none"/>' +
              '<location type="meta" path="/none"/>' +
            '</locations>' +
            '<ACL owner="ambari-qa" group="users" permission="0755"/>' +
            '<schema location="/none" provider="none"/>' +
            '<properties>' +
              '<property name="queueName" value="default"/>' +
              '<property name="jobPriority" value="NORMAL"/>' +
              '<property name="timeout" value="hours(1)"/>' +
              '<property name="parallel" value="3"/>' +
              '<property name="maxMaps" value="8"/>' +
              '<property name="mapBandwidthKB" value="1024"/>' +
            '</properties>' +
          '</feed>',
        feedTwo: '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' +
          '<feed name="feedTwo" description="Cleansed customer emails" xmlns="uri:falcon:feed:0.1">' +
            '<tags>owner=USMarketing,classification=Secure,externalSource=USProdEmailServers,externalTarget=BITools</tags>' +
            '<groups>churnAnalysisDataPipeline</groups>' +
            '<frequency>hours(1)</frequency>' +
            '<timezone>GMT+00:00</timezone>' +
            '<clusters>' +
              '<cluster name="primaryCluster" type="source">' +
                '<validity start="2014-02-28T00:00Z" end="2016-04-01T00:00Z"/>' +
                '<retention limit="days(90)" action="delete"/>' +
                '<locations>' +
                  '<location type="data" path="/user/ambari-qa/falcon/demo/primary/processed/enron/${YEAR}-${MONTH}-${DAY}-${HOUR}"/>' +
                '</locations>' +
              '</cluster>' +
              '<cluster name="backupCluster" type="target">' +
                '<validity start="2014-02-28T00:00Z" end="2016-04-01T00:00Z"/>' +
                '<retention limit="months(36)" action="delete"/>' +
                '<locations>' +
                  '<location type="data" path="/falcon/demo/bcp/processed/enron/${YEAR}-${MONTH}-${DAY}-${HOUR}"/>' +
                '</locations>' +
              '</cluster>' +
            '</clusters>' +
            '<locations>' +
              '<location type="data" path="/user/ambari-qa/falcon/demo/processed/enron/${YEAR}-${MONTH}-${DAY}-${HOUR}"/>' +
            '</locations>' +
            '<ACL owner="ambari-qa" group="users" permission="0755"/>' +
            '<schema location="/none" provider="none"/>' +
            '<properties>' +
              '<property name="queueName" value="default"/>' +
              '<property name="jobPriority" value="NORMAL"/>' +
              '<property name="timeout" value="hours(1)"/>' +
              '<property name="parallel" value="3"/>' +
              '<property name="maxMaps" value="8"/>' +
              '<property name="mapBandwidthKB" value="1024"/>' +
            '</properties>' +
          '</feed>'
      },
      PROCESS: {
        processOne: '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' +
          '<process name="processOne" xmlns="uri:falcon:process:0.1">' +
            '<tags>pipeline=churnAnalysisDataPipeline,owner=ETLGroup,montevideo=mvd</tags>' +
            '<clusters>' +
              '<cluster name="primaryCluster">' +
                '<validity start="2014-02-28T00:00Z" end="2016-04-01T00:00Z"/>' +
              '</cluster>' +
            '</clusters>' +
            '<parallel>1</parallel>' +
            '<order>FIFO</order>' +
            '<frequency>hours(1)</frequency>' +
            '<timezone>GMT-12:00</timezone>' +
            '<inputs>' +
              '<input name="input" feed="rawEmailFeed" start="now(0,0)" end="now(0,0)"/>' +
            '</inputs>' +
            '<outputs>' +
              '<output name="output" feed="cleansedEmailFeed" instance="now(0,0)"/>' +
            '</outputs>' +
            '<workflow name="emailCleanseWorkflow2" version="pig-0.10.0" engine="pig" path="/user/ambari-qa/falcon/demo/apps/pig/id.pig"/>' +
            '<retry policy="periodic" delay="minutes(15)" attempts="3"/>' +
          '</process>',
        processTwo: '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' +
          '<process name="processTwo" xmlns="uri:falcon:process:0.1">' +
            '<tags>pipeline=churnAnalysisDataPipeline,owner=ETLGroup,externalSystem=USWestEmailServers</tags>' +
            '<clusters>' +
              '<cluster name="primaryCluster">' +
                '<validity start="2014-02-28T00:00Z" end="2016-03-31T00:00Z"/>' +
              '</cluster>' +
            '</clusters>' +
            '<parallel>1</parallel>' +
            '<order>FIFO</order>' +
            '<frequency>hours(1)</frequency>' +
            '<timezone>GMT+00:00</timezone>' +
            '<outputs>' +
              '<output name="output" feed="rawEmailFeed" instance="now(0,0)"/>' +
            '</outputs>' +
            '<workflow name="emailIngestWorkflow" version="2.0.0" engine="oozie" path="/user/ambari-qa/falcon/demo/apps/ingest/fs"/>' +
            '<retry policy="periodic" delay="minutes(15)" attempts="3"/>' +
          '</process>',

        'hdfs-mirror-test': "<?xml version='1.0' encoding='UTF-8' standalone='yes'?><process xmlns='uri:falcon:process:0.1' name='hdfs-mirror-test'><tags>_falcon_mirroring_type=HDFS</tags><clusters><cluster name='completeCluster'><validity start='2015-03-28T07:32:00.000Z' end='2015-12-31T20:00:00.000Z'/></cluster></clusters><parallel>1</parallel><order>LAST_ONLY</order><frequency>days(7)</frequency><timezone>GMT+00:00</timezone><properties><property name='oozie.wf.subworkflow.classpath.inheritance' value='true'></property><property name='distcpMaxMaps' value='50'></property><property name='distcpMapBandwidth' value='1100'></property><property name='drSourceDir' value='first-path/source/cluster'></property><property name='drTargetDir' value='second-path/source/cluster'></property><property name='drTargetClusterFS' value='hdfs://sandbox.hortonworks.com:8020'></property><property name='drSourceClusterFS' value='hdfs://sandbox.hortonworks.com:8020'></property><property name='drNotifyEmail' value='first-test@hdfs.com'></property><property name='targetCluster' value='primaryCluster'></property><property name='sourceCluster' value='completeCluster'></property></properties><workflow name='hdfs-mirror-test2-WF' engine='oozie' path='hdfs://node-1.example.com:8020/apps/falcon/recipe/hdfs-replication/resources/runtime/hdfs-replication-workflow.xml' lib=''/><retry policy='EXPONENTIAL_BACKOFF' delay='days(60)' attempts='7'/><ACL owner='ambari-qa' group='users2' permission='0x755'/></process>",

        'hdfs-azure-mirror-test': "<?xml version='1.0' encoding='UTF-8' standalone='yes'?><process xmlns='uri:falcon:process:0.1' name='hdfs-azure-mirror-test'><tags>_falcon_mirroring_type=HDFS,runsOn=target</tags><clusters><cluster name='completeCluster'><validity start='2015-03-28T06:32:00.000Z' end='2015-12-31T21:00:00.000Z'/></cluster></clusters><parallel>1</parallel><order>LAST_ONLY</order><frequency>hours(7)</frequency><timezone>GMT+00:00</timezone><properties><property name='oozie.wf.subworkflow.classpath.inheritance' value='true'></property><property name='distcpMaxMaps' value='340'></property><property name='distcpMapBandwidth' value='2200'></property><property name='drSourceDir' value='first-path/azure/something'></property><property name='drTargetDir' value='second-path/target/cluster'></property><property name='drTargetClusterFS' value='hdfs://sandbox.hortonworks.com:8020'></property><property name='drSourceClusterFS' value='http://some.addres.blob.core.windows.net'></property><property name='drNotifyEmail' value='first-test@hdfs.com,second-test@azure.com'></property><property name='targetCluster' value='completeCluster'></property><property name='sourceCluster' value=''></property></properties><workflow name='hdfs-azure-mirror-test-WF' engine='oozie' path='hdfs://node-1.example.com:8020/apps/falcon/recipe/hdfs-replication/resources/runtime/hdfs-replication-workflow.xml' lib=''/><retry policy='EXPONENTIAL_BACKOFF' delay='days(60)' attempts='1'/><ACL owner='ambari-qa' group='users2' permission='0x755'/></process>",

        'hdfs-s3-mirror-test': "<?xml version='1.0' encoding='UTF-8' standalone='yes'?><process xmlns='uri:falcon:process:0.1' name='hdfs-s3-mirror-test'><tags>_falcon_mirroring_type=HDFS,runsOn=target</tags><clusters><cluster name='completeCluster'><validity start='2015-04-03T00:42:00.000Z' end='2015-12-08T21:00:00.000Z'/></cluster></clusters><parallel>1</parallel><order>LAST_ONLY</order><frequency>months(7)</frequency><timezone>GMT+00:00</timezone><properties><property name='oozie.wf.subworkflow.classpath.inheritance' value='true'></property><property name='distcpMaxMaps' value='3340'></property><property name='distcpMapBandwidth' value='5500'></property><property name='drSourceDir' value='first-path/s3/something'></property><property name='drTargetDir' value='third-path/target/cluster'></property><property name='drTargetClusterFS' value='hdfs://sandbox.hortonworks.com:8020'></property><property name='drSourceClusterFS' value='s3://some.address..amazonaws.com'></property><property name='drNotifyEmail' value='second-test@s3.com'></property><property name='targetCluster' value='completeCluster'></property><property name='sourceCluster' value=''></property></properties><workflow name='hdfs-s3-mirror-test-WF' engine='oozie' path='hdfs://node-1.example.com:8020/apps/falcon/recipe/hdfs-replication/resources/runtime/hdfs-replication-workflow.xml' lib=''/><retry policy='FINAL' delay='months(60)' attempts='15'/><ACL owner='ambari-qa' group='users' permission='0x755'/></process>",

        'hive-databases-mirror-test': "<?xml version='1.0' encoding='UTF-8' standalone='yes'?><process xmlns='uri:falcon:process:0.1' name='hive-databases-mirror-test'><tags>_falcon_mirroring_type=HIVE,runsOn=target</tags><clusters><cluster name='completeCluster'><validity start='2015-04-14T15:42:00.000Z' end='2016-01-22T14:04:00.000Z'/></cluster></clusters><parallel>1</parallel><order>LAST_ONLY</order><frequency>months(7)</frequency><timezone>GMT+00:00</timezone><properties><property name='oozie.wf.subworkflow.classpath.inheritance' value='true'></property><property name='distcpMaxMaps' value='111'></property><property name='distcpMapBandwidth' value='678'></property><property name='targetCluster' value='primaryCluster'></property><property name='sourceCluster' value='completeCluster'></property><property name='targetHiveServer2Uri' value='some/path/staging'></property><property name='sourceHiveServer2Uri' value='thrift://localhost:10000'></property><property name='sourceStagingPath' value='/apps/falcon/backupCluster/staging'></property><property name='targetStagingPath' value='*'></property><property name='targetNN' value='hdfs://sandbox.hortonworks.com:8020'></property><property name='sourceNN' value='hdfs://sandbox.hortonworks.com:8020'></property><property name='sourceServicePrincipal' value='hive'></property><property name='targetServicePrincipal' value='hive'></property><property name='targetMetastoreUri' value=''></property><property name='sourceMetastoreUri' value='thrift://localhost:9083'></property><property name='sourceTable' value='*'></property><property name='sourceDatabase' value='db1,db2,db3'></property><property name='maxEvents' value='-10'></property><property name='replicationMaxMaps' value='52'></property><property name='clusterForJobRun' value='primaryCluster'></property><property name='clusterForJobRunWriteEP' value='hdfs://sandbox.hortonworks.com:8020'></property><property name='drJobName' value='hive-databases-mirror-test'></property><property name='drNotifyEmail' value='hive-databases@test.com'></property></properties><workflow name='falcon-dr-hive-workflow' engine='oozie' path='hdfs://node-1.example.com:8020/apps/falcon/recipe/hive-disaster-recovery/resources/runtime/hive-disaster-recovery-workflow.xml' lib=''/><retry policy='PERIODIC' delay='months(60)' attempts='15'/><ACL owner='ambari-qa' group='users' permission='0x755'/></process>",

        'hive-db-tables-mirror-test': "<?xml version='1.0' encoding='UTF-8' standalone='yes'?><process xmlns='uri:falcon:process:0.1' name='hive-db-tables-mirror-test'><tags>_falcon_mirroring_type=HIVE,runsOn=source</tags><clusters><cluster name='completeCluster'><validity start='2015-05-01T09:42:00.000Z' end='2016-04-09T09:04:00.000Z'/></cluster></clusters><parallel>1</parallel><order>LAST_ONLY</order><frequency>months(7)</frequency><timezone>GMT+00:00</timezone><properties><property name='oozie.wf.subworkflow.classpath.inheritance' value='true'></property><property name='distcpMaxMaps' value='111'></property><property name='distcpMapBandwidth' value='678'></property><property name='targetCluster' value='primaryCluster'></property><property name='sourceCluster' value='completeCluster'></property><property name='targetHiveServer2Uri' value='some/path/staging'></property><property name='sourceHiveServer2Uri' value='thrift://localhost:10000'></property><property name='sourceStagingPath' value='/apps/falcon/backupCluster/staging'></property><property name='targetStagingPath' value='/apps/falcon/some/staging'></property><property name='targetNN' value='hdfs://sandbox.hortonworks.com:8020'></property><property name='sourceNN' value='hdfs://sandbox.hortonworks.com:8020'></property><property name='sourceServicePrincipal' value='hive'></property><property name='targetServicePrincipal' value='hive'></property><property name='targetMetastoreUri' value=''></property><property name='sourceMetastoreUri' value='thrift://localhost:9083'></property><property name='sourceTable' value='table1,table2,table3'></property><property name='sourceDatabase' value='database1'></property><property name='maxEvents' value='-10'></property><property name='replicationMaxMaps' value='52'></property><property name='clusterForJobRun' value='completeCluster'></property><property name='clusterForJobRunWriteEP' value='hdfs://sandbox.hortonworks.com:8020'></property><property name='drJobName' value='hive-db-tables-mirror-test'></property><property name='drNotifyEmail' value='hive-tables@test.com'></property></properties><workflow name='falcon-dr-hive-workflow' engine='oozie' path='hdfs://node-1.example.com:8020/apps/falcon/recipe/hive-disaster-recovery/resources/runtime/hive-disaster-recovery-workflow.xml' lib=''/><retry policy='PERIODIC' delay='months(60)' attempts='15'/><ACL owner='ambari-qa' group='users' permission='0x755'/></process>"

      }

    },
    instancesList = {
      FEED: [
        {
          "details": "",
          "endTime": "2014-10-21T14:40:26-07:00",
          "startTime": "2015-10-21T14:39:56-07:00",
          "cluster": "primary-cluster",
          "logFile": "http:\/\/localhost:11000\/oozie?job=0000070-131021115933395-oozie-rgau-W",
          "status": "SUCCEEDED",
          "instance": "2012-04-01T07:00Z"
        }, {
          "details": "",
          "endTime": "2014-10-21T14:42:26-07:00",
          "startTime": "2015-10-21T14:41:56-07:00",
          "cluster": "primary-cluster",
          "logFile": "http:\/\/localhost:11000\/oozie?job=0000070-131021115933397-oozie-rgau-W",
          "status": "RUNNING",
          "instance": "2012-04-02T08:00Z"
        }, {
          "details": "",
          "endTime": "2014-10-21T14:42:26-07:00",
          "startTime": "2015-10-21T14:41:56-07:00",
          "cluster": "primary-cluster",
          "logFile": "http:\/\/localhost:11000\/oozie?job=0000070-131021115933397-oozie-rgau-W",
          "status": "SUSPENDED",
          "instance": "2012-04-03T08:00Z"
        }, {
          "details": "",
          "endTime": "2014-10-21T14:42:26-07:00",
          "startTime": "2015-10-21T14:41:56-07:00",
          "cluster": "primary-cluster",
          "logFile": "http:\/\/localhost:11000\/oozie?job=0000070-131021115933397-oozie-rgau-W",
          "status": "KILLED",
          "instance": "2012-04-04T08:00Z"
        }, {
          "details": "",
          "endTime": "2014-10-21T14:42:26-07:00",
          "startTime": "2015-10-21T14:41:56-07:00",
          "cluster": "primary-cluster",
          "logFile": "http:\/\/localhost:11000\/oozie?job=0000070-131021115933397-oozie-rgau-W",
          "status": "SUCCEEDED",
          "instance": "2012-04-05T08:00Z"
        }, {
          "details": "",
          "endTime": "2014-10-21T14:42:26-07:00",
          "startTime": "2015-10-21T14:41:56-07:00",
          "cluster": "primary-cluster",
          "logFile": "http:\/\/localhost:11000\/oozie?job=0000070-131021115933397-oozie-rgau-W",
          "status": "WAITING",
          "instance": "2012-04-06T08:00Z"
        }, {
          "details": "",
          "endTime": "2014-10-21T14:42:26-07:00",
          "startTime": "2015-10-21T14:41:56-07:00",
          "cluster": "primary-cluster",
          "logFile": "http:\/\/localhost:11000\/oozie?job=0000070-131021115933397-oozie-rgau-W",
          "status": "FAILED",
          "instance": "2012-04-07T08:00Z"
        }, {
          "details": "",
          "endTime": "2014-10-21T14:42:26-07:00",
          "startTime": "2015-10-21T14:41:56-07:00",
          "cluster": "primary-cluster",
          "logFile": "http:\/\/localhost:11000\/oozie?job=0000070-131021115933397-oozie-rgau-W",
          "status": "SUCCEEDED",
          "instance": "2012-04-08T08:00Z"
        }, {
          "details": "",
          "endTime": "2014-10-21T14:42:26-07:00",
          "startTime": "2015-10-21T14:41:56-07:00",
          "cluster": "primary-cluster",
          "logFile": "http:\/\/localhost:11000\/oozie?job=0000070-131021115933397-oozie-rgau-W",
          "status": "SUSPENDED",
          "instance": "2012-04-09T08:00Z"
        },{
          "details": "",
          "endTime": "2014-10-21T14:40:26-07:00",
          "startTime": "2015-10-21T14:39:56-07:00",
          "cluster": "primary-cluster",
          "logFile": "http:\/\/localhost:11000\/oozie?job=0000070-131021115933395-oozie-rgau-W",
          "status": "SUSPENDED",
          "instance": "2012-04-10T07:00Z"
        }, {
          "details": "",
          "endTime": "2014-10-21T14:42:26-07:00",
          "startTime": "2015-10-21T14:41:56-07:00",
          "cluster": "primary-cluster",
          "logFile": "http:\/\/localhost:11000\/oozie?job=0000070-131021115933397-oozie-rgau-W",
          "status": "RUNNING",
          "instance": "2012-04-11T08:00Z"
        }, {
          "details": "",
          "endTime": "2014-10-21T14:42:26-07:00",
          "startTime": "2015-10-21T14:41:56-07:00",
          "cluster": "primary-cluster",
          "logFile": "http:\/\/localhost:11000\/oozie?job=0000070-131021115933397-oozie-rgau-W",
          "status": "SUSPENDED",
          "instance": "2012-04-12T08:00Z"
        }, {
          "details": "",
          "endTime": "2014-10-21T14:42:26-07:00",
          "startTime": "2015-10-21T14:41:56-07:00",
          "cluster": "primary-cluster",
          "logFile": "http:\/\/localhost:11000\/oozie?job=0000070-131021115933397-oozie-rgau-W",
          "status": "KILLED",
          "instance": "2012-04-13T08:00Z"
        }, {
          "details": "",
          "endTime": "2014-10-21T14:42:26-07:00",
          "startTime": "2015-10-21T14:41:56-07:00",
          "cluster": "primary-cluster",
          "logFile": "http:\/\/localhost:11000\/oozie?job=0000070-131021115933397-oozie-rgau-W",
          "status": "SUCCEEDED",
          "instance": "2012-04-14T08:00Z"
        }, {
          "details": "",
          "endTime": "2014-10-21T14:42:26-07:00",
          "startTime": "2015-10-21T14:41:56-07:00",
          "cluster": "primary-cluster",
          "logFile": "http:\/\/localhost:11000\/oozie?job=0000070-131021115933397-oozie-rgau-W",
          "status": "SUCCEEDED",
          "instance": "2012-04-15T08:00Z"
        }, {
          "details": "",
          "endTime": "2014-10-21T14:42:26-07:00",
          "startTime": "2015-10-21T14:41:56-07:00",
          "cluster": "primary-cluster",
          "logFile": "http:\/\/localhost:11000\/oozie?job=0000070-131021115933397-oozie-rgau-W",
          "status": "SUCCEEDED",
          "instance": "2012-04-16T08:00Z"
        }, {
          "details": "",
          "endTime": "2014-10-21T14:42:26-07:00",
          "startTime": "2015-10-21T14:41:56-07:00",
          "cluster": "primary-cluster",
          "logFile": "http:\/\/localhost:11000\/oozie?job=0000070-131021115933397-oozie-rgau-W",
          "status": "SUCCEEDED",
          "instance": "2012-04-17T08:00Z"
        }, {
          "details": "",
          "endTime": "2014-10-21T14:42:26-07:00",
          "startTime": "2015-10-21T14:41:56-07:00",
          "cluster": "primary-cluster",
          "logFile": "http:\/\/localhost:11000\/oozie?job=0000070-131021115933397-oozie-rgau-W",
          "status": "SUCCEEDED",
          "instance": "2012-04-18T08:00Z"
        }, {
          "details": "",
          "endTime": "2014-10-21T14:42:26-07:00",
          "startTime": "2015-10-21T14:41:56-07:00",
          "cluster": "primary-cluster",
          "logFile": "http:\/\/localhost:11000\/oozie?job=0000070-131021115933397-oozie-rgau-W",
          "status": "SUCCEEDED",
          "instance": "2012-04-19T08:00Z"
        }, {
          "details": "",
          "endTime": "2014-10-21T14:42:26-07:00",
          "startTime": "2015-10-21T14:41:56-07:00",
          "cluster": "primary-cluster",
          "logFile": "http:\/\/localhost:11000\/oozie?job=0000070-131021115933397-oozie-rgau-W",
          "status": "SUCCEEDED",
          "instance": "2012-04-20T08:00Z"
        }, {
          "details": "",
          "endTime": "2014-10-21T14:42:26-07:00",
          "startTime": "2015-10-21T14:41:56-07:00",
          "cluster": "primary-cluster",
          "logFile": "http:\/\/localhost:11000\/oozie?job=0000070-131021115933397-oozie-rgau-W",
          "status": "SUCCEEDED",
          "instance": "2012-04-21T08:00Z"
        }, {
          "details": "",
          "endTime": "2014-10-21T14:42:26-07:00",
          "startTime": "2015-10-21T14:41:56-07:00",
          "cluster": "primary-cluster",
          "logFile": "http:\/\/localhost:11000\/oozie?job=0000070-131021115933397-oozie-rgau-W",
          "status": "SUCCEEDED",
          "instance": "2012-04-22T08:00Z"
        }, {
          "details": "",
          "endTime": "2014-10-21T14:42:26-07:00",
          "startTime": "2015-10-21T14:41:56-07:00",
          "cluster": "primary-cluster",
          "logFile": "http:\/\/localhost:11000\/oozie?job=0000070-131021115933397-oozie-rgau-W",
          "status": "SUCCEEDED",
          "instance": "2012-04-18T08:00Z"
        }, {
          "details": "",
          "endTime": "2014-10-21T14:42:26-07:00",
          "startTime": "2015-10-21T14:41:56-07:00",
          "cluster": "primary-cluster",
          "logFile": "http:\/\/localhost:11000\/oozie?job=0000070-131021115933397-oozie-rgau-W",
          "status": "SUCCEEDED",
          "instance": "2012-04-19T08:00Z"
        }, {
          "details": "",
          "endTime": "2014-10-21T14:42:26-07:00",
          "startTime": "2015-10-21T14:41:56-07:00",
          "cluster": "primary-cluster",
          "logFile": "http:\/\/localhost:11000\/oozie?job=0000070-131021115933397-oozie-rgau-W",
          "status": "SUCCEEDED",
          "instance": "2012-04-20T08:00Z"
        }, {
          "details": "",
          "endTime": "2014-10-21T14:42:26-07:00",
          "startTime": "2015-10-21T14:41:56-07:00",
          "cluster": "primary-cluster",
          "logFile": "http:\/\/localhost:11000\/oozie?job=0000070-131021115933397-oozie-rgau-W",
          "status": "SUCCEEDED",
          "instance": "2012-04-21T08:00Z"
        }, {
          "details": "",
          "endTime": "2014-10-21T14:42:26-07:00",
          "startTime": "2015-10-21T14:41:56-07:00",
          "cluster": "primary-cluster",
          "logFile": "http:\/\/localhost:11000\/oozie?job=0000070-131021115933397-oozie-rgau-W",
          "status": "SUCCEEDED",
          "instance": "2012-04-22T08:00Z"
        }, {
          "details": "",
          "endTime": "2014-10-21T14:42:26-07:00",
          "startTime": "2015-10-21T14:41:56-07:00",
          "cluster": "primary-cluster",
          "logFile": "http:\/\/localhost:11000\/oozie?job=0000070-131021115933397-oozie-rgau-W",
          "status": "SUCCEEDED",
          "instance": "2012-04-18T08:00Z"
        }, {
          "details": "",
          "endTime": "2014-10-21T14:42:26-07:00",
          "startTime": "2015-10-21T14:41:56-07:00",
          "cluster": "primary-cluster",
          "logFile": "http:\/\/localhost:11000\/oozie?job=0000070-131021115933397-oozie-rgau-W",
          "status": "SUCCEEDED",
          "instance": "2012-04-19T08:00Z"
        }, {
          "details": "",
          "endTime": "2014-10-21T14:42:26-07:00",
          "startTime": "2015-10-21T14:41:56-07:00",
          "cluster": "primary-cluster",
          "logFile": "http:\/\/localhost:11000\/oozie?job=0000070-131021115933397-oozie-rgau-W",
          "status": "SUCCEEDED",
          "instance": "2012-04-20T08:00Z"
        }, {
          "details": "",
          "endTime": "2014-10-21T14:42:26-07:00",
          "startTime": "2015-10-21T14:41:56-07:00",
          "cluster": "primary-cluster",
          "logFile": "http:\/\/localhost:11000\/oozie?job=0000070-131021115933397-oozie-rgau-W",
          "status": "SUCCEEDED",
          "instance": "2012-04-20T08:00Z"
        }
      ],
      PROCESS: [
        {
          "details": "",
          "endTime": "2014-10-21T14:40:26-07:00",
          "startTime": "2015-10-21T14:39:56-07:00",
          "cluster": "primary-cluster",
          "logFile": "http:\/\/localhost:11000\/oozie?job=0000070-131021115933395-oozie-rgau-W",
          "status": "SUCCEEDED",
          "instance": "2012-04-03T07:00Z"
        },
        {
          "details": "",
          "endTime": "2014-10-21T14:42:26-07:00",
          "startTime": "2015-10-21T14:41:56-07:00",
          "cluster": "primary-cluster",
          "logFile": "http:\/\/localhost:11000\/oozie?job=0000070-131021115933397-oozie-rgau-W",
          "status": "SUCCEEDED",
          "instance": "2012-04-03T08:00Z"
        }
      ]
    },
    entityDependencies = {
      "entity": [
        {
          "name": "SampleInput",
          "type": "feed",
          "tag": ["Input"]
          //"list": {"tag": ["Input"]}
        },
        {
          "name": "SampleOutput",
          "type": "feed",
          "tag": ["Output"]
          //"list": {"tag": ["Output"]}
        },
        {
          "name": "primary-cluster",
          "type": "cluster"
        }
      ]
    },
    vertices = {
      "results":[{"timestamp":"2015-03-26T20:24Z","name":"SampleProcess3\/2014-11-01T23:00Z","type":"process-instance","version":"1.0","_id":40108,"_type":"vertex"}],"totalSize":1
    },
    verticesDirection = [
      {"results":[{"_id":"2wm3-aqU-4u","_type":"edge","_outV":40108,"_inV":16,"_label":"owned-by"},{"_id":"2wm1-aqU-5o","_type":"edge","_outV":40108,"_inV":4,"_label":"runs-on"},{"_id":"2wm7-apq-5w","_type":"edge","_outV":40016,"_inV":40108,"_label":"input"},{"_id":"2wm5-aqU-5E","_type":"edge","_outV":40108,"_inV":40012,"_label":"output"},{"_id":"2wlZ-aqU-86","_type":"edge","_outV":40108,"_inV":32,"_label":"instance-of"}],"totalSize":5},
      {"results":[{"timestamp":"2015-03-26T02:45Z","name":"ambari-qa","type":"user","_id":16,"_type":"vertex"},{"timestamp":"2015-03-26T02:42Z","name":"primaryCluster","type":"cluster-entity","_id":4,"_type":"vertex"},{"timestamp":"2015-03-26T20:21Z","name":"SampleFeed1\/yearno=2014\/monthno=11","type":"feed-instance","_id":40016,"_type":"vertex"},{"timestamp":"2015-03-26T20:21Z","name":"SampleFeed6\/yearno=2014\/monthno=11","type":"feed-instance","_id":40012,"_type":"vertex"},{"timestamp":"2015-03-26T02:46Z","name":"SampleProcess3","type":"process-entity","version":"1.0","_id":32,"_type":"vertex"}],"totalSize":5},
      {"results":[{"_id":"2w9d-apm-4u","_type":"edge","_outV":40012,"_inV":16,"_label":"owned-by"},{"_id":"2w9b-apm-4S","_type":"edge","_outV":40012,"_inV":4,"_label":"stored-in"},{"_id":"2whH-aqo-5E","_type":"edge","_outV":40076,"_inV":40012,"_label":"output"},{"_id":"2wif-aqs-5E","_type":"edge","_outV":40080,"_inV":40012,"_label":"output"},{"_id":"2w9f-api-5E","_type":"edge","_outV":40008,"_inV":40012,"_label":"output"},{"_id":"2wiN-aqw-5E","_type":"edge","_outV":40084,"_inV":40012,"_label":"output"},{"_id":"2wa1-apu-5E","_type":"edge","_outV":40020,"_inV":40012,"_label":"output"},{"_id":"2wjl-aqA-5E","_type":"edge","_outV":40088,"_inV":40012,"_label":"output"},{"_id":"2waz-apy-5E","_type":"edge","_outV":40024,"_inV":40012,"_label":"output"},{"_id":"2wjT-aqE-5E","_type":"edge","_outV":40092,"_inV":40012,"_label":"output"},{"_id":"2wb7-apC-5E","_type":"edge","_outV":40028,"_inV":40012,"_label":"output"},{"_id":"2wkr-aqI-5E","_type":"edge","_outV":40096,"_inV":40012,"_label":"output"},{"_id":"2wbF-apG-5E","_type":"edge","_outV":40032,"_inV":40012,"_label":"output"},{"_id":"2wkZ-aqM-5E","_type":"edge","_outV":40100,"_inV":40012,"_label":"output"},{"_id":"2wcd-apK-5E","_type":"edge","_outV":40036,"_inV":40012,"_label":"output"},{"_id":"2wlx-aqQ-5E","_type":"edge","_outV":40104,"_inV":40012,"_label":"output"},{"_id":"2wcL-apO-5E","_type":"edge","_outV":40040,"_inV":40012,"_label":"output"},{"_id":"2wm5-aqU-5E","_type":"edge","_outV":40108,"_inV":40012,"_label":"output"},{"_id":"2wdj-apS-5E","_type":"edge","_outV":40044,"_inV":40012,"_label":"output"},{"_id":"2wdR-apW-5E","_type":"edge","_outV":40048,"_inV":40012,"_label":"output"},{"_id":"2wep-aq0-5E","_type":"edge","_outV":40052,"_inV":40012,"_label":"output"},{"_id":"2weX-aq4-5E","_type":"edge","_outV":40056,"_inV":40012,"_label":"output"},{"_id":"2wfv-aq8-5E","_type":"edge","_outV":40060,"_inV":40012,"_label":"output"},{"_id":"2wg3-aqc-5E","_type":"edge","_outV":40064,"_inV":40012,"_label":"output"},{"_id":"2wgB-aqg-5E","_type":"edge","_outV":40068,"_inV":40012,"_label":"output"},{"_id":"2wh9-aqk-5E","_type":"edge","_outV":40072,"_inV":40012,"_label":"output"},{"_id":"2w99-apm-86","_type":"edge","_outV":40012,"_inV":28,"_label":"instance-of"}],"totalSize":27},
      {"results":[{"_id":"2w9t-apq-4u","_type":"edge","_outV":40016,"_inV":16,"_label":"owned-by"},{"_id":"2w9r-apq-4S","_type":"edge","_outV":40016,"_inV":4,"_label":"stored-in"},{"_id":"2wih-apq-5w","_type":"edge","_outV":40016,"_inV":40080,"_label":"input"},{"_id":"2wiP-apq-5w","_type":"edge","_outV":40016,"_inV":40084,"_label":"input"},{"_id":"2wa3-apq-5w","_type":"edge","_outV":40016,"_inV":40020,"_label":"input"},{"_id":"2wjn-apq-5w","_type":"edge","_outV":40016,"_inV":40088,"_label":"input"},{"_id":"2waB-apq-5w","_type":"edge","_outV":40016,"_inV":40024,"_label":"input"},{"_id":"2w9v-apq-5w","_type":"edge","_outV":40016,"_inV":40008,"_label":"input"},{"_id":"2wjV-apq-5w","_type":"edge","_outV":40016,"_inV":40092,"_label":"input"},{"_id":"2wb9-apq-5w","_type":"edge","_outV":40016,"_inV":40028,"_label":"input"},{"_id":"2wkt-apq-5w","_type":"edge","_outV":40016,"_inV":40096,"_label":"input"},{"_id":"2wbH-apq-5w","_type":"edge","_outV":40016,"_inV":40032,"_label":"input"},{"_id":"2wl1-apq-5w","_type":"edge","_outV":40016,"_inV":40100,"_label":"input"},{"_id":"2wcf-apq-5w","_type":"edge","_outV":40016,"_inV":40036,"_label":"input"},{"_id":"2wlz-apq-5w","_type":"edge","_outV":40016,"_inV":40104,"_label":"input"},{"_id":"2wcN-apq-5w","_type":"edge","_outV":40016,"_inV":40040,"_label":"input"},{"_id":"2wm7-apq-5w","_type":"edge","_outV":40016,"_inV":40108,"_label":"input"},{"_id":"2wdl-apq-5w","_type":"edge","_outV":40016,"_inV":40044,"_label":"input"},{"_id":"2wdT-apq-5w","_type":"edge","_outV":40016,"_inV":40048,"_label":"input"},{"_id":"2wer-apq-5w","_type":"edge","_outV":40016,"_inV":40052,"_label":"input"},{"_id":"2weZ-apq-5w","_type":"edge","_outV":40016,"_inV":40056,"_label":"input"},{"_id":"2wfx-apq-5w","_type":"edge","_outV":40016,"_inV":40060,"_label":"input"},{"_id":"2wg5-apq-5w","_type":"edge","_outV":40016,"_inV":40064,"_label":"input"},{"_id":"2wgD-apq-5w","_type":"edge","_outV":40016,"_inV":40068,"_label":"input"},{"_id":"2whb-apq-5w","_type":"edge","_outV":40016,"_inV":40072,"_label":"input"},{"_id":"2whJ-apq-5w","_type":"edge","_outV":40016,"_inV":40076,"_label":"input"},{"_id":"2w9p-apq-86","_type":"edge","_outV":40016,"_inV":12,"_label":"instance-of"}],"totalSize":27}
    ],
    verticesProps = {
      "results":
      {
        "timestamp":"2014-04-25T22:20Z",
        "name":"local",
        "type":"cluster-entity"
      },
      "totalSize":3
    },
    server = {
      //"properties":[{key: "authentication", value: "kerberos"}]
      "properties":[{key: "authentication", value: "simple"}]
    };

  exports.findByNameInList = findByNameInList;
  exports.findByStartEnd = findByStartEnd;
  exports.entitiesList = entitiesList;
  exports.definitions = definitions;
  exports.instancesList = instancesList;
  exports.entityDependencies = entityDependencies;
  exports.vertices = vertices;
  exports.verticesDirection = verticesDirection;
  exports.verticesProps = verticesProps;
  exports.server = server;

})();