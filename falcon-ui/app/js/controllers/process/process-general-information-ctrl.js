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

  /***
   * @ngdoc controller
   * @name app.controllers.feed.FeedController
   * @requires clusters the list of clusters to display for selection of source
   * @requires EntityModel the entity model to copy the feed entity from
   * @requires Falcon the falcon entity service
   */
  var feedModule = angular.module('app.controllers.process');

  feedModule.controller('ProcessGeneralInformationCtrl', [ '$scope', function($scope) {
    var availableVerions = {
      oozie: ['3.1.3-incubating', '3.2.0-incubating', '3.3.0', '3.3.1', '3.3.2', '4.0.0', '4.0.1'],
      pig: ['pig-0.10.0', 'pig-0.10.1', 'pig-0.11.0', 'pig-0.11.1', 'pig-0.12.0', 'pig-0.12.1', 'pig-0.13.0', 'pig-0.8.0', 'pig-0.8.1', ' pig-0.9.0', ' pig-0.9.1', 'pig-0.9.2'],
      hive: ['hive-0.10.0', 'hive-0.11.0', 'hive-0.12.0', 'hive-0.13.0', 'hive-0.13.1', 'hive-0.6.0', 'hive-0.7.0', 'hive-0.8.0', 'hive-0.8.1', 'hive-0.9.0']
    };
    $scope.nameValid = false;
    
    $scope.init = function() {
      $scope.versions = [];
    };

    $scope.addTag = function() {
      $scope.process.tags.push({key: null, value: null});
    };

    $scope.removeTag = function(index) {
      if(index >= 0 && $scope.process.tags.length > 1) {
        $scope.process.tags.splice(index, 1);
      }
    };

    $scope.selectWorkflow = function() {
      
      if($scope.process.workflow) {        
        var engine = $scope.process.workflow.engine;
        $scope.versions = availableVerions[engine];
      }
    };

    $scope.init();
    $scope.selectWorkflow();

  }]);


})();
