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
  var module = angular.module('app.services.entity.model', ['ngCookies']);

  module.factory('EntityModel', ["X2jsService", "$cookieStore", function(X2jsService, $cookieStore) {

    var EntityModel = {}, userName;

    EntityModel.json = null;
    EntityModel.detailsPageModel = null;

    EntityModel.identifyType = function(json) {
      if(json && json.feed) { EntityModel.type = "feed"; }
      else if(json && json.cluster) { EntityModel.type = "cluster"; }
      else if(json && json.process) { EntityModel.type = "process"; }
      else { EntityModel.type = 'Type not recognized'; }
    };

    EntityModel.getJson = function(xmlString) {
      EntityModel.json = X2jsService.xml_str2json( xmlString );
      return EntityModel.identifyType(EntityModel.json);
    };

    if($cookieStore.get('userToken')){
      userName = $cookieStore.get('userToken').user;
    } else {
      userName = "";
    }

    EntityModel.defaultValues = {
      cluster: {
        cluster:{
          tags: "",
          interfaces:{
            interface:[
              {
                _type:"readonly",
                _endpoint:"hftp://sandbox.hortonworks.com:50070",
                _version:"2.2.0"
              },
              {
                _type:"write",
                _endpoint:"hdfs://sandbox.hortonworks.com:8020",
                _version:"2.2.0"

              },
              {
                _type:"execute",
                _endpoint:"sandbox.hortonworks.com:8050",
                _version:"2.2.0"

              },
              {
                _type:"workflow",
                _endpoint:"http://sandbox.hortonworks.com:11000/oozie/",
                _version:"4.0.0"

              },
              {
                _type:"messaging",
                _endpoint:"tcp://sandbox.hortonworks.com:61616?daemon=true",
                _version:"5.1.6"

              }
            ]
          },
          locations:{
            location:[
              {_name: "staging", _path: ""},
              {_name: "temp", _path: ""},
              {_name: "working", _path: ""},
              {"_name":"","_path":""} //>> to compare
            ]
          },
          ACL: {
            _owner: userName,
            _group: "users",
            _permission: "0x755"
          },
          properties: {
            property: [
              { _name: "", _value: ""}
            ]
          },
          _xmlns:"uri:falcon:cluster:0.1",
          _name: undefined,
          _description: undefined,
          _colo: undefined
        }
      },
      MirrorUIModel: {
        name: undefined,
        tags: {
          newTag: { value:"", key:"" },
          tagsArray: [{ key:"_falcon_mirroring_type", value:"HDFS" }],
          tagsString: ""
        },
        formType: "HDFS",
        runOn: "target",
        source: {
          location: "HDFS",
          cluster: "",
          url: "",
          path: "",
          hiveDatabaseType: "databases",
          hiveDatabases: "",
          hiveDatabase: "",
          hiveTables: ""
        },
        target: {
          location: "HDFS",
          cluster: "",
          url: "",
          path: ""
        },
        alerts: {
          alert: { email: "" },
          alertsArray: []
        },
        validity: {
          start: (function () { var d = new Date(); d.setHours(0); d.setMinutes(0); d.setSeconds(0); return d; }()),
          startTime: new Date(),
          end: "",
          endTime: new Date(),
          tz: "GMT+00:00",
          startISO: "",
          endISO: ""
        },
        frequency: {
          number: 5,
          unit: 'minutes'
        },
        allocation: {
          hdfs:{
            maxMaps: "5",
            maxBandwidth: "100"
          },
          hive:{
            maxMapsDistcp: "1",
            maxMapsMirror: "5",
            maxMapsEvents: "-1",
            maxBandwidth: "100"
          }
        },
        hiveOptions: {
          source:{
            stagingPath: "",
            hiveServerToEndpoint: ""
          },
          target:{
            stagingPath: "",
            hiveServerToEndpoint: ""
          }
        },
        retry: {
          policy:"periodic",
          delay: {
            unit: "minutes",
            number: 30
          },
          attempts: 3
        },
        acl: {
          owner: userName,
          group: "users",
          permissions: "0x755"
        }
      }
    };
    EntityModel.clusterModel = {}; //>> gets copied at bottom from defaultValues

    EntityModel.feedModel = {
      feed: {
        tags: "",
        groups: "",
        frequency: "",
        /*timezone: "GMT+00:00",*/
        "late-arrival": {
          "_cut-off": ""
        },
        clusters: [{
          "cluster": {
            validity: {
              _start: "",
              _end: ""
            },
            retention: {
              _limit: "",
              _action: ""
            },
            _name: "",
            _type: "source"
          }
        }],
        locations: {
          location: [{
            _type: "data",
            _path: "/none"
          }, {
            _type: "stats",
            _path: "/none"
          }, {
            _type: "meta",
            _path: "/none"
          }]
        },
        ACL: {
          _owner: userName,
          _group: "users",
          _permission: "0x755"
        },
        schema: {
          _location: "/none",
          _provider: "none"
        },
        _xmlns: "uri:falcon:feed:0.1",
        _name: "",
        _description: ""
      }
    };

    EntityModel.datasetModel = {
      toImportModel: undefined,
      UIModel: {},
      HDFS: {
        process: {
          tags: "",
          clusters: {
            cluster: [{
              validity: {
                _start: "2015-03-13T00:00Z",
                _end: "2016-12-30T00:00Z"
              },
              _name: "primaryCluster"
            }]
          },
          parallel: "1",
          order: "LAST_ONLY",
          frequency: "minutes(5)",
          timezone: "UTC",
          properties: {
            property: [
              {
                _name: "oozie.wf.subworkflow.classpath.inheritance",
                _value: "true"
              },
              {
                _name: "distcpMaxMaps",
                _value: "5"
              },
              {
                _name: "distcpMapBandwidth",
                _value: "100"
              },
              {
                _name: "drSourceDir",
                _value: "/user/hrt_qa/dr/test/srcCluster/input"
              },
              {
                _name: "drTargetDir",
                _value: "/user/hrt_qa/dr/test/targetCluster/input"
              },
              {
                _name: "drTargetClusterFS",
                _value: "hdfs://240.0.0.10:8020"
              },
              {
                _name: "drSourceClusterFS",
                _value: "hdfs://240.0.0.10:8020"
              },
              {
                _name: "drNotificationReceivers",
                _value: ""
              },
              {
                _name: "targetCluster",
                _value: ""
              },
              {
                _name: "sourceCluster",
                _value: ""
              }
            ]
          },
          workflow: {
            _name: "hdfs-dr-workflow",
            _engine: "oozie",
            _path: "/apps/data-mirroring/workflows/hdfs-replication-workflow.xml",
            _lib: ""
          },
          retry: {
            _policy: "periodic",
            _delay: "minutes(30)",
            _attempts: "3"
          },
          ACL: {
            _owner: "hrt_qa",
            _group: "users",
            _permission: "0x755"
          },
          _xmlns: "uri:falcon:process:0.1",
          _name: "hdfs-replication-adtech"
        }
      },
      HIVE: {
        process: {
          tags: "",
          clusters: {
            cluster: [{
              validity: {
                _start: "2015-03-14T00:00Z",
                _end: "2016-12-30T00:00Z"
              },
              _name: "primaryCluster"
            }]
          },
          parallel: "1",
          order: "LAST_ONLY",
          frequency: "minutes(3)",
          timezone: "UTC",
          properties: {
            property: [
              {
                _name: "oozie.wf.subworkflow.classpath.inheritance",
                _value: "true"
              },
              {
                _name: "distcpMaxMaps",
                _value: "1"
              },
              {
                _name: "distcpMapBandwidth",
                _value: "100"
              },
              {
                _name: "targetCluster",
                _value: "backupCluster"
              },
              {
                _name: "sourceCluster",
                _value: "primaryCluster"
              },
              {
                _name: "targetHiveServer2Uri",
                _value: "hive2://240.0.0.11:10000"
              },
              {
                _name: "sourceHiveServer2Uri",
                _value: "hive2://240.0.0.10:10000"
              },
              {
                _name: "sourceStagingPath",
                _value: "/apps/falcon/primaryCluster/staging"
              },
              {
                _name: "targetStagingPath",
                _value: "/apps/falcon/backupCluster/staging"
              },
              {
                _name: "targetNN",
                _value: "hdfs://240.0.0.11:8020"
              },
              {
                _name: "sourceNN",
                _value: "hdfs://240.0.0.10:8020"
              },
              {
                _name: "sourceServicePrincipal",
                _value: "hive"
              },
              {
                _name: "targetServicePrincipal",
                _value: "hive"
              },
              {
                _name: "targetMetastoreUri",
                _value: "thrift://240.0.0.11:9083"
              },
              {
                _name: "sourceMetastoreUri",
                _value: "thrift://240.0.0.10:9083"
              },
              {
                _name: "sourceTable",
                _value: ""
              },
              {
                _name: "sourceDatabase",
                _value: ""
              },
              {
                _name: "maxEvents",
                _value: "-1"
              },
              {
                _name: "replicationMaxMaps",
                _value: "5"
              },
              {
                _name: "clusterForJobRun",
                _value: "primaryCluster"
              },
              {
                _name: "clusterForJobRunWriteEP",
                _value: "hdfs://240.0.0.10:8020"
              },
              {
                _name: "drJobName",
                _value: "hive-disaster-recovery-sowmya-1"
              },
              {
                _name: "drNotificationReceivers",
                _value: "NA"
              }
            ]
          },
          workflow: {
            _name: "falcon-dr-hive-workflow",
            _engine: "oozie",
            _path: "/apps/data-mirroring/workflows/hive-disaster-recovery-workflow.xml",
            _lib: ""
          },
          retry: {
            _policy: "periodic",
            _delay: "minutes(30)",
            _attempts: "3"
          },
          ACL: {
            _owner: "hrt_qa",
            _group: "users",
            _permission: "0x755"
          },
          _xmlns: "uri:falcon:process:0.1",
          _name: "hive-disaster-recovery-sowmya-1"
        }
      }

    };

    angular.copy(EntityModel.defaultValues.cluster, EntityModel.clusterModel);
    angular.copy(EntityModel.defaultValues.MirrorUIModel, EntityModel.datasetModel.UIModel);

    return EntityModel;

  }]);

})();
