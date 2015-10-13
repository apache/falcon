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

  clusterModule.controller('EntityDetailsCtrl', [
    "$scope", "$timeout", "$interval", "Falcon", "EntityModel", "$state", "X2jsService", 'EntitySerializer', 'InstanceFalcon',
    function ($scope, $timeout, $interval, Falcon, EntityModel, $state, X2jsService, serializer, InstanceFalcon) {

      $scope.entity = EntityModel;

      var resultsPerPage = 10;
      var visiblePages = 3;
      $scope.entityName = $scope.entity.name;
      $scope.entityType = $scope.entity.type;

      $scope.pages = [];
      $scope.nextPages = false;
      $scope.mirrorTag = "_falcon_mirroring_type";

      $scope.isMirror = function(tags){
        var flag = false;
        if(tags !== undefined && tags.indexOf($scope.mirrorTag) !== -1){
          flag = true;
        }
        return flag;
      };

      if($scope.entity.type === "feed"){
        $scope.feed = serializer.preDeserialize($scope.entity.model, "feed");
        $scope.feed.name = $scope.entity.name;
        $scope.feed.type = $scope.entity.type;
        $scope.entity.start = $scope.entity.model.feed.clusters.cluster[0].validity._start;
        $scope.entity.end = $scope.entity.model.feed.clusters.cluster[0].validity._end;
      }else{
        var tags = $scope.entity.model.process.tags;
        if($scope.isMirror(tags)){
          $scope.entityTypeLabel = "Mirror";
        }
        $scope.process = serializer.preDeserialize($scope.entity.model, "process");
        $scope.process.name = $scope.entity.name;
        $scope.process.type = $scope.entity.type;
        $scope.entity.start = $scope.entity.model.process.clusters.cluster[0].validity._start;
        $scope.entity.end = $scope.entity.model.process.clusters.cluster[0].validity._end;
      }

      $scope.capitalize = function(input) {
        return input.charAt(0).toUpperCase() + input.slice(1);
      };

      $scope.dateFormatter = function (date) {
        var dates = date.split('T')[0],
            time = date.split('T')[1].split('Z')[0].split('.')[0];
        return dates + ' ' + time;
      };

      $scope.refreshInstanceList = function (type, name, start, end, status, orderBy, sortOrder) {
        $scope.instancesList = [];
        changePagesSet(0, 0, 0, start, end, status, orderBy, sortOrder);
      };

      var consultPage = function(offset, page, defaultPage, start, end, status, orderBy, sortOrder){
        Falcon.responses.listLoaded = false;
        InstanceFalcon.searchInstances($scope.entityType, $scope.entityName, offset, start, end, status, orderBy, sortOrder).then(function() {
          if (InstanceFalcon.data) {
            $scope.pages[page] = {};
            $scope.pages[page].index = page;
            $scope.pages[page].data = InstanceFalcon.data.instances;
            $scope.pages[page].show = true;
            $scope.pages[page].enabled = true;
            $scope.pages[page].label = "" + ((offset/resultsPerPage)+1);
            if($scope.pages[page].data.length > resultsPerPage){
              offset = offset + resultsPerPage;
              $scope.nextPages = true;
              if(page < visiblePages-1){
                consultPage(offset, page+1, defaultPage, start, end, status, orderBy, sortOrder);
              }else{
                $scope.goPage(defaultPage);
              }
            }else{
              $scope.nextPages = false;
              $scope.goPage(defaultPage);
            }
          }
        });
      };

      var changePagesSet = function(offset, page, defaultPage, start, end, status, orderBy, sortOrder){
        $scope.pages = [];
        consultPage(offset, page, defaultPage, start, end, status, orderBy, sortOrder);
      };

      $scope.goPage = function (page) {
        $scope.pages.forEach(function(pag) {
          pag.enabled = true;
        });
        $scope.pages[page].enabled = false;
        $scope.instancesList = $scope.pages[page].data;
        if($scope.instancesList.length > resultsPerPage){
          $scope.instancesList.pop();
        }
        $scope.prevPages = parseInt($scope.pages[page].label) >  visiblePages ? true : false;
        Falcon.responses.listLoaded = true;
      };

      $scope.changePagesSet = function(offset, page, defaultPage, start, end, status, orderBy, sortOrder){
        changePagesSet(offset, page, defaultPage, start, end, status, orderBy, sortOrder);
      };

      $scope.instanceDetails = function (instance) {
        EntityModel.model = instance;
        EntityModel.type = $scope.entity.type;
        EntityModel.name = $scope.entity.name;
        $state.go("instanceDetails");
      };

      $scope.displayIcon = function (type, model) {
        if(type === "FEED"){
          $scope.entityTypeLabel = "Feed";
          return "entypo download";
        }else if(type === "PROCESS"){
          var tags = model.process.tags;
          if($scope.isMirror(tags)){
            $scope.entityTypeLabel = "Mirror";
            return "glyphicon glyphicon-duplicate";
          }else{
            $scope.entityTypeLabel = "Process";
            return "entypo cycle";
          }
        }else{
          $scope.entityTypeLabel = "Process";
          return "entypo cycle";
        }
      };

    }
  ]);

  clusterModule.filter('titleCase', function() {
    return function(input) {
      input = input || '';
      return input.replace(/\w\S*/g, function(txt){return txt.charAt(0).toUpperCase() + txt.substr(1).toLowerCase();});
    };
  });

})();
