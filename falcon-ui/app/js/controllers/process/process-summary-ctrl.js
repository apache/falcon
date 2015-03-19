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

  feedModule.controller('ProcessSummaryCtrl', [ '$scope', '$state', '$timeout', '$filter', 'Falcon',
                                                  function($scope, $state, $timeout, $filter, Falcon) {

    $scope.init = function() {
      if($scope.transform) {
        $scope.transform();
      }
    };

    $scope.hasTags = function() {
      var filteredTags = $filter('filter')($scope.process.tags, {key: '!!'});
      return filteredTags.length > 0;
    };

    $scope.optional = function(input, output) {
      return input ? (output || input) : 'Not specified';
    };

    $scope.saveEntity = function() {
      var type = $scope.entityType;
      if(!$scope.$parent.cloningMode) { 
        Falcon.logRequest();
        Falcon.postUpdateEntity($scope.xml, $scope.entityType, $scope[type].name)
          .success(function (response) {
             Falcon.logResponse('success', response, false); 
             $state.go('main'); 

          })
          .error(function (err) {   
            Falcon.logResponse('error', err, false);          
          });
      } 
      else {
        Falcon.logRequest();
        Falcon.postSubmitEntity($scope.xml, $scope.entityType)
          .success(function (response) {
             Falcon.logResponse('success', response, false); 
             $state.go('main'); 
 
          })
          .error(function (err) {   
            Falcon.logResponse('error', err, false);          
          });
      }

      $scope.editingMode = false;
      $scope.cloningMode = false;
    };

    $scope.init();

  }]);

})();
