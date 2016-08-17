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
  var module = angular.module('app.services.entity.factory', []);

  var userName;

  module.factory('EntityFactory', ["$cookieStore", function ($cookieStore) {


    if($cookieStore.get('userToken') !== null &&$cookieStore.get('userToken') !== undefined ){
      userName = $cookieStore.get('userToken').user;
    }else{
      userName = "";
    }

    return {
      newFeed: function () {
        return new Feed();
      },

      newFeedProperties: function () {
        return feedProperties();
      },

      newFeedCustomProperties: function () {
        return feedCustomProperties();
      },

      newFrequency: function (quantity, unit) {
        return new Frequency(quantity, unit);
      },

      newLocation: function (type, path) {
        return new Location(type, path);
      },

      newCluster: function (type, selected, name, partition) {
        return new Cluster(type, selected, name, partition);
      },

      newPartition: function (name) {
        return new Partition(name);
      },

      newEntry: function (key, value) {
        return new Entry(key, value);
      },

      newProperty: function (name, value) {
        return new Property(name, value);
      },

      newProcess: function () {
        return new Process();
      },

      newInput: function () {
        return new Input();
      },

      newOutput: function () {
        return new Output();
      },

      newClusterEntity: function() {
        return new ClusterEntity();
      },

      newClusterLocation: function(name, path) {
        return new ClusterLocation(name, path);
      },

      newClusterInterface: function(type, endpoint, version) {
        return new ClusterInterface(type, endpoint, version);
      },

      newSnapshot: function() {
        return new Snapshot();
      },

      newSparkAttributes: function() {
        return new SparkAttributes();
      },

      newCredential: function() {
        return new Credential();
      },

      newDatasourceInterface: function() {
        return new DatasourceInterface();
      },

      newDatasource: function() {
        return new Datasource();
      },

      newClusterFileSystem: function() {
        return new clusterFileSystem();
      },

      newEntity: function (type) {
        if (type === 'feed') {
          return this.newFeed();
        } else if (type === 'process') {
          return this.newProcess();
        } else if (type === 'cluster') {
          return this.newClusterEntity();
        } else if (type === 'snapshot') {
          return this.newSnapshot();
        } else if (type === 'datasource') {
          return this.newDatasource();
        }
      }

    };
  }]);

  function Feed() {
//    this.name = null;
    this.name = "";
    this.description = null;
    this.groups = null;
    this.tags = [new Entry(null, null)];
    this.ACL = new ACL();
    this.schema = new Schema();
    this.frequency = new Frequency(1, 'hours');
    this.lateArrival = new LateArrival();
    this.availabilityFlag = '_success';
    this.properties = feedProperties();
    this.customProperties = [new Entry(null, null)];
    this.storage = new Storage();
    this.clusters = [];
    this.timezone = 'UTC';
    this.partitions = [];
    this.retentionFrequency = new Frequency(20, 'minutes');
    this.validity = new Validity();
    this.enableFeedReplication = false;
    this.dataTransferType = '';
  }


  function ACL() {
    this.owner = userName;
    this.group = 'users';
    this.permission = '0x755';
  }

  function Schema() {
    this.location = '/none';
    this.provider = '/none';
  }

  function feedProperties() {
    return [
      new Entry('queueName', 'default'),
      new Entry('jobPriority', 'NORMAL'),
      new Entry('parallel', ''),
      new Entry('maxMaps', ''),
      new Entry('mapBandwidthKB', '')
    ];
  }

  /*new Entry('queueName', 'default'),
    new Entry('jobPriority', ''),
    new Entry('timeout', new Frequency(1, 'hours')),
    new Entry('parallel', 3),
    new Entry('maxMaps', 8),
    new Entry('mapBandwidthKB', 1024)*/

  function feedCustomProperties() {
    return [
      new Entry(null, null)
    ];
  }

  function LateArrival() {
    this.active = true;
    this.cutOff = new Frequency(4, 'hours');
  }

  function Frequency(quantity, unit) {
    this.quantity = quantity;
    this.unit = unit;
  }

  function Entry(key, value) {
    this.key = key;
    this.value = value;
  }

  function Storage() {
    this.fileSystem = new FileSystem();
    this.catalog = new Catalog();
  }
  function clusterStorage(type) {
    if(type === 'hdfs'){
      this.fileSystem = new clusterFileSystem();
    }else if(type === 'hive'){
      this.catalog = new Catalog();
    }
  }

  function Catalog() {
    this.catalogTable = new CatalogTable();
  }

  function CatalogTable() {
    this.uri = null;
    this.focused = false;
  }

  function FileSystem() {
    this.locations = null;
  }
  function clusterFileSystem() {
    this.locations = [ new Location('data',''), new Location('stats','/')];
  }

  function Location(type, path) {
    this.type = type;
    this.path= path;
    this.focused = false;
  }

  function Cluster(type, selected, name, partition) {
//    this.name = null;
    this.name = (name != undefined) ? name : "";
    this.type = type;
    this.selected = selected;
    if (type == 'source') {
      this.retention = new Frequency(90, 'days');
    } else if (type == 'target') {
      this.retention = new Frequency(12, 'months');
    } else {
      this.retention = new Frequency(null, 'hours');
    }

    this.retention.action = 'delete';
    this.validity = new Validity();
    this.storage = new clusterStorage(selected);
    if (partition != undefined) {
      this.partition = partition;
    }
  }

  function Partition(name) {
    this.name = name;
  }

  function Validity() {
    this.start = new DateAndTime();
    this.end = new DateAndTime();
    this.end.date = new Date("Dec 31, 2099 11:59:59");
    this.end.time = new Date("Dec 31, 2099 11:59:59");
    this.timezone = "";
  }

  function DateAndTime() {
    this.date = new Date();
    this.time = new Date();
    this.opened = false;
  }

  /*function currentDate() {
    var now = new Date();
    return now;
  }*/

  function currentTime() {
    return new Date(1900, 1, 1, 0, 0, 0);
  }

  function Process() {
    this.name = null;
    this.tags = [new Entry(null, null)];
    this.workflow = new Workflow();
    this.timezone = 'UTC';
    this.frequency = new Frequency(30, 'minutes');
    this.parallel = 1;
    this.order = 'FIFO';
    this.retry = new ProcessRetry();
    this.clusters = [new Cluster('source', true)];
    this.inputs = [];
    this.outputs = [];
    this.ACL = new ACL();
    this.properties = [new Property(null, null)];
    this.validity = new Validity();

    /*
    this.name = 'P';
    this.workflow.name = 'W';
    this.workflow.engine = 'oozie';
    this.workflow.version = '3.3.1';
    this.frequency.quantity = '2';
    this.retry.attempts = '4';
    this.retry.delay.quantity = '4';
    this.clusters[0].name = 'backupCluster';
    this.tags = [{key: 'tag1', value: 'value1'},{key: 'tag2', value: 'value2'}];
    */
  }

  function SparkAttributes() {
    this.name = '';
    this.master = 'yarn';
    this.mode = 'cluster';
    this.class = '';
    this.sparkOptions = '';
    this.jar = '';
    this.arg = '';
  }

  function Workflow() {
    this.name = '';
    this.engine = '';
    this.version = '';
    this.path = '/';
    this.spark = new SparkAttributes();
  }

  function Retry() {
    this.policy = 'exp-backoff';
    this.attempts = 3;
    this.delay = new Frequency(30, 'minutes');
  }

  function ProcessRetry() {
    this.policy = 'periodic';
    this.attempts = 3;
    this.delay = new Frequency(30, 'minutes');
  }

  function Input() {
    this.name = null;
    this.feed = "";
    this.start = null;
    this.end = null;
  }

  function Output() {
    this.name = null;
    this.feed = "";
    this.outputInstance = 'now(0,0)';
  }

  function ClusterEntity() {
    this.name = "";
    this.colo = null;
    this.description = null;
    this.tags = [new Entry(null, null)];
    this.ACL = new ACL();

    this.interfaces = [];
    this.properties = [];
    this.locations = []
  }

  function ClusterLocation(name, path) {
    this.name = name;
    this.path= path;
  }

  function ClusterInterface(type, endpoint, version) {
    this.type = type;
    this.endpoint = endpoint;
    this.version = version;
  }

  function SnapshotCluster(type) {
    this.cluster = '';
    this.directoryPath = '';
    if (type === 'source') {
      this.deleteFrequency = new Frequency(14, 'days');
      this.retentionNumber = 90;
    } else if (type === 'target') {
      this.deleteFrequency = new Frequency(14, 'days');
      this.retentionNumber = 90;
    }
  }

  function Snapshot() {
    this.name = '';
    this.type = 'snapshot';
    this.ACL = new ACL();
    this.tags = [new Entry(null,  null)];
    this.frequency = new Frequency(1, 'days');
    this.alerts = [];
    this.validity = new Validity();
    this.validity.timezone = 'UTC';
    this.runOn = 'target';
    this.retry = new Retry();
    this.source = new SnapshotCluster('source');
    this.target = new SnapshotCluster('target');
    this.allocation = {};
    this.tdeEncryptionEnabled = false;
  }

  function Credential() {
    this.type = "";
    this.userName = ""
    this.passwordText = "";
    this.passwordFile = "";
    this.passwordAlias = "";
    this.providerPath = "";
  }

  function DatasourceInterface() {
    this.type = "readonly";
    this.endpoint = "";
    this.credential = new Credential();
  }

  function DatasourceInterfaces() {
    this.credential = new Credential();
    this.interfaces = [new DatasourceInterface()];
  }

  function Driver() {
    this.clazz = null;
    this.jar = [{value:""}];
  }

  function Property(name, value) {
    this.name = name;
    this.value = value;
  }

  function datasourceProperties() {
    return [
      new Property('parameterFile', ''),
      new Property('verboseMode', ''),
      new Property('directMode', '')
    ];
  }

  function Datasource() {
    this.name = "";
    this.colo = null;
    this.description = null;
    this.tags = [new Entry(null, null)];
    this.type = "";
    this.customProperties = [];
    this.properties = new datasourceProperties();
    this.parameters = [];
    this.ACL = new ACL();
    this.interfaces = new DatasourceInterfaces();
    this.host = "";
    this.port = "";
    this.databaseName = "";
    this.driver = new Driver();
  }


})();
