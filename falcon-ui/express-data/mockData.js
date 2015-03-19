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
  }

  var entitiesList = {
    cluster : {
      "entity":[
        {"type":"cluster","name":"completeCluster","status":"SUBMITTED","list":{"tag":["uruguay=mvd","argentina=bsas","mexico=mex", "usa=was"]}},
        {"type":"cluster","name":"primaryCluster","status":"SUBMITTED"}
      ]
    },
    feed: {
      "entity":[
        {"type":"FEED","name":"feedOne","status":"SUBMITTED","list":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},
        {"type":"FEED","name":"feedTwo","status":"RUNNING","list":{"tag":["owner=USMarketing","classification=Secure","externalSource=USProdEmailServers","externalTarget=BITools"]}}
      ]
    },
    process:{"entity":[
      {"type":"process","name":"processOne","status":"SUBMITTED","list":{"tag":["pipeline=churnAnalysisDataPipeline","owner=ETLGroup","montevideo=mvd"]}},
      {"type":"process","name":"processTwo","status":"SUBMITTED","list":{"tag":["pipeline=churnAnalysisDataPipeline","owner=ETLGroup","externalSystem=USWestEmailServers"]}}]}
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
            '<timezone>UTC</timezone>' +
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
            '<timezone>UTC</timezone>' +
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
            '<timezone>UTC</timezone>' +
            '<outputs>' +
              '<output name="output" feed="rawEmailFeed" instance="now(0,0)"/>' +
            '</outputs>' +
            '<workflow name="emailIngestWorkflow" version="2.0.0" engine="oozie" path="/user/ambari-qa/falcon/demo/apps/ingest/fs"/>' +
            '<retry policy="periodic" delay="minutes(15)" attempts="3"/>' +
          '</process>'
        }
      };
  
  exports.findByNameInList = findByNameInList;
  exports.entitiesList = entitiesList;
  exports.definitions = definitions;
  
})();