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
   * @requires EntityModel the entity model to copy the feed entity from
   * @requires Falcon the falcon service to talk with the Falcon REST API
   */
  var clusterModule = angular.module('app.controllers.view', [ 'app.services' ]);

  clusterModule.controller('EntityDefinitionCtrl', [
    "$scope", "$interval", "Falcon", "EntityModel", "$state", "X2jsService",
    function ($scope, $interval, Falcon, EntityModel, $state, X2jsService) {

      $scope.entity = EntityModel;
      $scope.xmlPreview = { edit: false };

      var xmlStr = X2jsService.json2xml_str(angular.copy($scope.entity.model));
      $scope.prettyXml = X2jsService.prettifyXml(xmlStr);
      $scope.xml = xmlStr;

    }
  ]);

  clusterModule.filter('titleCase', function() {
    return function(input) {
      input = input || '';
      return input.replace(/\w\S*/g, function(txt){return txt.charAt(0).toUpperCase() + txt.substr(1).toLowerCase();});
    };
  });

})();
