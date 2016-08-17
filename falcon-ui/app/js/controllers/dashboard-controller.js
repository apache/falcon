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

  var dashboardCtrlModule = angular.module('app.controllers.dashboardCtrl', ['app.services']);

  dashboardCtrlModule.controller('DashboardCtrl', [ "$scope", "$q", "Falcon", "EntityFalcon", "EntityModel",
    "EntityScheduler", "FileApi", "$state", "X2jsService", "$timeout", "ServerAPI",
    function ($scope, $q, Falcon, EntityFalcon, EntityModel,
      EntityScheduler, FileApi, $state, X2jsService, $timeout, ServerAPI) {

      ServerAPI.getRuntimeConfig(EntityModel.getUserNameFromCookie());

      if(!($state.params && $state.params.fromAction === 'listEntities')){
        $scope.$parent.refreshList($scope.tags);
      }
      var searchPromise = $scope.$parent.goPage(1, 'list');
      searchPromise.then(function(){
        if($scope.$parent.searchList.length > 0){
          return;
        }
        EntityFalcon.searchEntities('', '', 'cluster', 0).then(function(){
          if(EntityFalcon.data !== null && EntityFalcon.data.entity && EntityFalcon.data.entity.length >0){
            $scope.$parent.hasClusters = true;
          }else{
            $scope.$parent.hasClusters = false;
          }
        });
      });
      $timeout(function() {
        angular.element('#nsPopover').trigger('click');
      }, 1000);

      $scope.focusSearch = function () {
        $scope.$parent.refreshList($scope.tags);
      };

      $scope.backToListing = function(type){
        if($scope.tags && $scope.tags.length > 0){
          $scope.$parent.refreshList($scope.tags);
        }else if(type === 'cluster'){
          $scope.$parent.goPage($scope.$parent.currentPage, 'cluster');
        }else{
          $scope.$parent.goPage($scope.$parent.currentPage, 'list');
        }
      }

      $scope.deleteEntity = function (type, name) {
        EntityScheduler.deleteEntity(type, name).then(function(status){
            if(status === "DELETED"){
              $scope.backToListing(type);
            }
        });
      };

      //-----------------------------------------//
      $scope.entityDefinition = function (name, type) {

    	  type = type.toLowerCase(); //new sandbox returns uppercase type

    	  Falcon.logRequest();
          Falcon.getEntityDefinition(type, name)
            .success(function (data) {
              Falcon.logResponse('success', data, false, true);
              var entityModel = X2jsService.xml_str2json(data);
              EntityModel.type = type;
              EntityModel.name = name;
              EntityModel.model = entityModel;
              $state.go('entityDefinition');
            })
            .error(function (err) {
              Falcon.logResponse('error', err, false, true);
            });
      };
      //----------------------------------------//
      $scope.resumeEntity = function (type, name) {
        EntityScheduler.resumeEntity(type, name).then(function(status){
          if(status === "RUNNING"){
            $scope.backToListing(type);
          }
        });
      };

      $scope.scheduleEntity = function (type, name) {
        EntityScheduler.scheduleEntity(type, name).then(function(status){
          if(status === "RUNNING"){
            $scope.backToListing(type);
          }
        });
      };

      $scope.suspendEntity = function (type, name) {
        EntityScheduler.suspendEntity(type, name).then(function(status){
          if(status === "SUSPENDED"){
            $scope.backToListing(type);
          }
        });
      };

      $scope.loadTags = function(query) {
        var tags = new Array();
        if(!$scope.$parent.nameFounded){
          tags.push({ text: 'Name:' + query });
        }
        if(!$scope.$parent.typeFounded){
          var queryAux = query.toUpperCase();
          if(queryAux === "F" || queryAux === "FE" || queryAux === "FEE" || queryAux === "FEED"){
            tags.push({ text: 'Type:feed'});
          }
          if(queryAux === "P" || queryAux === "PR" || queryAux === "PRO" || queryAux === "PROC" || queryAux === "PROCE"
              || queryAux === "PROCES" || queryAux === "PROCESS"){
            tags.push({ text: 'Type:process'});
          }
          if(queryAux === "M" || queryAux === "MI" || queryAux === "MIR" || queryAux === "MIRR" || queryAux === "MIRRO"
              || queryAux === "MIRROR"){
            tags.push({ text: 'Type:mirror'});
          }
        }
        if(query !== "*"){
          tags.push({ text: 'Tag:' + query });
        }
        return tags;
      };

      $scope.relationsEntity = function (type, name) {
        console.log("relations " + type + " - " + name);
      };

      $scope.displayResults = function () {
        $scope.$parent.refreshList($scope.tags);
      };

      $scope.entityDetails = function (name, type) {

        type = type.toLowerCase(); //new sandbox returns uppercase type

        Falcon.logRequest();
        var entityDetailsPromise = Falcon.getEntityDefinition(type, name);
        var entityStatusPromise = Falcon.getEntityStatus(type, name);
        $q.all([entityDetailsPromise,entityStatusPromise]).then(function(responses){
              Falcon.logResponse('success', responses[0].data, false, true);
              Falcon.logResponse('success', responses[1].data, false, true);
              var entityModel = X2jsService.xml_str2json(responses[0].data);
              EntityModel.type = type;
              EntityModel.name = name;
              var status = responses[1].data.message;
              EntityModel.status = status.substr(status.indexOf("/") + 1, status.length - 1).trim();
              EntityModel.model = entityModel;
              $state.go('entityDetails');
        },function(err){
              Falcon.logResponse('error', err, type);
        });
      };

      $scope.clearTags = function(){
        $scope.tags = [];
        $scope.$parent.refreshList($scope.tags);
      };

    }]);

})();
