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

  function findIndexByName(name) {
    var i;
    for (i = 0; i < datasetsList.length; i++) {
      if (datasetsList[i]["name"] === name) {
        return i;
      }
    }
    return false;
  }

  var clusterList = [
      {"id":"d29dd782-038f-42d0-9ede-10ce09ff08ec","name":"cluster1"},
      {"id":"485cb7ab-64ba-4ab9-928e-c7bc96b38f8f","name":"cluster2"},
      {"id":"cf8d00a8-f9c4-40c7-a3f0-86daedbea2d6","name":"cluster3"},
      {"id":"c06f5f43-c1da-4d31-b790-9d1d8b459537","name":"cluster4"},
      {"id":"b01aa770-22fe-47c2-bd46-f76ed26c1393","name":"cluster5"},
      {"id":"d8a32b27-1fc2-4ac9-b6db-1367c76f2519","name":"cluster7"},
      {"id":"a6f33616-f553-4b2d-bebb-5978f81bece7","name":"cluster8"},
      {"id":"2bfe1d86-bc62-4f3d-9c7e-d00924504a86","name":"cluster9"},
      {"id":"a1e37af1-91b5-4f99-95a3-be3abca1e65c","name":"cluster10"},
      {"id":"1f29031c-ddb5-44ab-aae1-8f1794f0529d","name":"cluster11"},
      {"id":"30b77cce-0135-4d8b-ade8-5b11a860ae13","name":"cluster12"},
      {"id":"9a126663-972a-4318-89e8-01d3d0bb4388","name":"cluster13"},
      {"id":"86476e26-771a-4a79-9139-73e69aba0670","name":"cluster14"},
      {"id":"86b37de5-849b-42b2-95a2-a4ff031e8668","name":"cluster15"}
    ],
    usersList = [
      {"id":"d29dd782-038f-42d0-9ede-10ce09ff08ec","name":"ambari-something"},
      {"id":"485cb7ab-64ba-4ab9-928e-c7bc96b38f8f","name":"ambari-admin"},
      {"id":"cf8d00a8-f9c4-40c7-a3f0-86daedbea2d6","name":"ambari-user"},
      {"id":"c06f5f43-c1da-4d31-b790-9d1d8b459537","name":"some-other-user"},
    ],
    datasetsList = [ { name: 'Test', tags: [ { key: 'ddd', value: 'ssd' } ], type: 'HDFS', clusters:
    { source_cluster: { name: 'cluster1', location: [Object] },
      target_cluster: { name: 'cluster1', location: [Object] } },
      start: 'Mon, 02 Feb 2015 00:00:00 GMT+00:00', frequency: { every: '2', unit: 'hours' },
      repeats: { every: '2', unit: 'days' }, allocation:
      { max_snapshots: '2', max_number_slots: '2', max_bandwidth: '2', measure: 'Kb' },
      run_as: 'default', permission: '*', on_error: { action: 'abort', options: 'back-off' },
      alerts: [ { email: 's@h.com', start: true, finish: false, fail: true, abort: false } ] }
    ];


  exports.findIndexByName = findIndexByName;
  exports.clusterList = clusterList;
  exports.usersList = usersList;
  exports.datasetsList = datasetsList;

}());