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

  feedModule.controller('ProcessRootCtrl', [
    '$scope', '$state', '$interval', '$controller', 'EntityFactory',
    'EntitySerializer', 'X2jsService', 'ValidationService',
    function ($scope, $state, $interval, $controller, entityFactory, serializer, X2jsService, validationService) {

      $scope.entityType = 'process';

        //extending root controller
      $controller('EntityRootCtrl', {
        $scope: $scope
      });

      $scope.init = function() {
        $scope.baseInit();
        var type = $scope.entityType;
        $scope[type] = $scope.loadOrCreateEntity();
      };

      $scope.isActive = function (route) {
        return route === $state.$current.name;
      };

      $scope.loadOrCreateEntity = function() {
        var type = $scope.entityType;
        var model = $scope.models[type + 'Model'];
        $scope.models[type + 'Model'] = null;
        if(model) {
          return serializer.preDeserialize(model, type);
        }
        return entityFactory.newEntity(type);
      };

      $scope.init();

      $scope.transform = function() {
        var type = $scope.entityType;
        var xml = serializer.serialize($scope[type], $scope.entityType);
        $scope.prettyXml = X2jsService.prettifyXml(xml);
        $scope.xml = xml;
        return xml;
      };

      var xmlPreviewCallback = function() {
        var type = $scope.entityType;
        if($scope.editXmlDisabled) {
          try {
            $scope.transform();
          } catch (exception) {
            console.log('error when transforming xml');
            console.log(exception);
          }
        } else {
          try {
            $scope[type] = serializer.deserialize($scope.prettyXml, type);
          } catch (exception) {
            console.log('user entered xml incorrect format');
            console.log(exception);
          }
        }

      };

      var xmlPreviewWorker = $interval(xmlPreviewCallback, 1000);

      $scope.$on('$destroy', function() {
        $interval.cancel(xmlPreviewWorker);
      });

      //---------------------------------//
      $scope.goNext = function (formInvalid, stateName) {
        if (!validationService.nameAvailable || formInvalid) {
          validationService.displayValidations.show = true;
          validationService.displayValidations.nameShow = true;
          return;
        }
        validationService.displayValidations.show = false;
        validationService.displayValidations.nameShow = false;
        $state.go(stateName);
      };
      $scope.goBack = function (stateName) {
        validationService.displayValidations.show = false;
        validationService.displayValidations.nameShow = false;
        $state.go(stateName);
      };
    }
  ]);

}());
