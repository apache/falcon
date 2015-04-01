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

  var app = angular.module('app.controllers.rootCtrl', ['app.services']);

  app.controller('RootCtrl', [
    "$scope", "$timeout", "Falcon", "FileApi", "EntityModel", "$state", "X2jsService", "ValidationService",
    function ($scope, $timeout, Falcon, FileApi, EntityModel, $state, X2jsService, validationService) {

      $scope.server = Falcon;
      $scope.validations = validationService;
      $scope.models = {};

      $scope.handleFile = function (evt) {
        Falcon.logRequest();
        FileApi.loadFile(evt).then(function () {
          Falcon.postSubmitEntity(FileApi.fileRaw, EntityModel.type).success(function (response) {
            Falcon.logResponse('success', response, false);
            $scope.refreshList(EntityModel.type);
          }).error(function (err) {
            Falcon.logResponse('error', err, false);
          });
        });
      };

      $scope.lists = {};
      $scope.lists.feedList = [];
      $scope.lists.clusterList = [];
      $scope.lists.processList = [];

      $scope.refreshList = function (type) {
        type = type.toLowerCase();
        Falcon.responses.listLoaded[type] = false;
        if (Falcon.responses.multiRequest[type] > 0) { return; }

        Falcon.logRequest();

        Falcon.getEntities(type)
          .success(function (data) {
            
            Falcon.logResponse('success', data, false, true);
            Falcon.responses.listLoaded[type] = true;
            $scope.lists[type + 'List'] = [];

            if (data === null) {
              $scope.lists[type + 'List'] = [];
            }else{
              var typeOfData = Object.prototype.toString.call(data.entity);	
        	  if (typeOfData === "[object Array]") {
                $scope.lists[type + 'List'] = data.entity;
              } else if (typeOfData === "[object Object]") {
                $scope.lists[type + 'List'][0] = data.entity;
              } else {
                console.log("type of data not recognized");
              }
            }
          })
          .error(function (err) {
            Falcon.logResponse('error', err);
          });
      };

      $scope.refreshLists = function () {
        $scope.refreshList('cluster');
        $scope.refreshList('feed');
        $scope.refreshList('process');
      };
      $scope.closeAlert = function (index) {
        Falcon.removeMessage(index);
      };

    }]);

}());