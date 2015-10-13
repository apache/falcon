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
  var clusterModule = angular.module('app.controllers.instance', ['app.services']);

  clusterModule.controller('InstanceDetailsCtrl', [
    "$scope", "$interval", "Falcon", "EntityModel", "$state", "X2jsService", 'EntitySerializer',
    function ($scope, $interval, Falcon, EntityModel, $state, X2jsService) {

      $scope.instance = EntityModel.model;
      $scope.instance.type = EntityModel.type;
      $scope.instance.name = EntityModel.name;

      $scope.backToEntity = function () {
        var type = $scope.instance.type.toLowerCase();
        var name = $scope.instance.name;
        Falcon.logRequest();
        Falcon.getEntityDefinition(type, name)
          .success(function (data) {
            Falcon.logResponse('success', data, false, true);
            var entityModel = X2jsService.xml_str2json(data);
            EntityModel.type = type;
            EntityModel.name = name;
            EntityModel.model = entityModel;
            $state.go('entityDetails');
          })
          .error(function (err) {
            Falcon.logResponse('error', err, false, true);
          });
      };

      $scope.resumeInstance = function () {
        Falcon.logRequest();
        var start = $scope.instance.instance;
        var end = addOneMin(start);
        Falcon.postResumeInstance($scope.instance.type, $scope.instance.name, start, end)
          .success(function (message) {
            Falcon.logResponse('success', message, $scope.instance.type);
            $scope.instance.status = "RUNNING";
          })
          .error(function (err) {
            Falcon.logResponse('error', err, $scope.instance.type);

          });
      };

      $scope.reRunInstance = function () {
        Falcon.logRequest();
        var start = $scope.instance.instance;
        var end = addOneMin(start);
        Falcon.postReRunInstance($scope.instance.type, $scope.instance.name, start, end)
          .success(function (message) {
            Falcon.logResponse('success', message, $scope.instance.type);
            $scope.instance.status = "RUNNING";
          })
          .error(function (err) {
            Falcon.logResponse('error', err, $scope.instance.type);

          });
      };

      $scope.suspendInstance = function () {
        Falcon.logRequest();
        var start = $scope.instance.instance;
        var end = addOneMin(start);
        Falcon.postSuspendInstance($scope.instance.type, $scope.instance.name, start, end)
          .success(function (message) {
            Falcon.logResponse('success', message, $scope.instance.type);
            $scope.instance.status = "SUSPENDED";
          })
          .error(function (err) {
            Falcon.logResponse('error', err, $scope.instance.type);

          });
      };

      $scope.killInstance = function () {
        Falcon.logRequest();
        var start = $scope.instance.instance;
        var end = addOneMin(start);
        Falcon.postKillInstance($scope.instance.type, $scope.instance.name, start, end)
          .success(function (message) {
            Falcon.logResponse('success', message, $scope.instance.type);
            $scope.instance.status = "KILLED";
          })
          .error(function (err) {
            Falcon.logResponse('error', err, $scope.instance.type);

          });
      };

      var addOneMin = function (time) {
        var newtime = parseInt(time.substring(time.length - 3, time.length - 1));
        if (newtime === 59) {
          newtime = 0;
        } else {
          newtime++;
        }
        if (newtime < 10) {
          newtime = "0" + newtime;
        }
        return time.substring(0, time.length - 3) + newtime + "Z";
      }

    }
  ]);

})();
