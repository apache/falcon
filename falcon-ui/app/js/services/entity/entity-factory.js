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

  module.factory('EntityFactory', [function () {
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

      newCluster: function (type, selected) {
        return new Cluster(type, selected);
      },

      newEntry: function (key, value) {
        return new Entry(key, value);
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

      newEntity: function (type) {
        if (type === 'feed') {
          return this.newFeed();
        }
        if (type === 'process') {
          return this.newProcess();
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
    this.frequency = new Frequency(null, 'hours');
    this.lateArrival = new LateArrival();
    this.availabilityFlag = null;
    this.properties = feedProperties();
    this.customProperties = [new Entry(null, null)];
    this.storage = new Storage();
    this.clusters = [new Cluster('source', true)];
    this.timezone = null;
  }


  function ACL() {
    this.owner = null;
    this.group = null;
    this.permission = '*';
  }

  function Schema() {
    this.location = null;
    this.provider = null;
  }

  function feedProperties() {
    return [
      new Entry('queueName', 'default'),
      new Entry('jobPriority', ''),
      new Entry('timeout', new Frequency(1, 'hours')),
      new Entry('parallel', 3),
      new Entry('maxMaps', 8),
      new Entry('mapBandwidthKB', 1024)
    ];
  }

  function feedCustomProperties() {
    return [
      new Entry(null, null)
    ];
  }

  function LateArrival() {
    this.active = false;
    this.cutOff = new Frequency(null, 'hours');
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

  function Catalog() {
    this.active = false;
    this.catalogTable = new CatalogTable();
  }

  function CatalogTable() {
    this.uri = null;
    this.focused = false;
  }

  function FileSystem() {
    this.active = true;
    this.locations = [new Location('data','/'), new Location('stats','/'), new Location('meta','/')];
  }

  function Location(type, path) {
    this.type = type;
    this.path= path;
    this.focused = false;
  }

  function Cluster(type, selected) {
//    this.name = null;
	this.name = "";
    this.type = type;
    this.selected = selected;
    this.retention = new Frequency(null, 'hours');
    this.retention.action = 'delete';
    this.validity = new Validity();
    this.storage = new Storage();
  }

  function Validity() {
    this.start = new DateAndTime();
    this.end = new DateAndTime();
    this.timezone = "";
  }

  function DateAndTime() {
    this.date = "";
    this.time = currentTime();
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
    this.timezone = null;
    this.frequency = new Frequency(null, 'hours');
    this.parallel = 1;
    this.order = "";
    this.retry = new Retry();
    this.clusters = [new Cluster('source', true)];
    this.inputs = [];
    this.outputs = [];

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

  function Workflow() {
    this.name = null;
    this.engine = null;
    this.version = '';
    this.path = '/';
  }

  function Retry() {
    this.policy = '';
    this.attempts = null;
    this.delay = new Frequency(null, '');
  }

  function Input() {
    this.name = null;
    this.feed = "";
    this.start = null;
    this.end = null;
  }

  function Output() {
    this.name = null;
    this.feed = null;
    this.outputInstance = null;
  }

})();