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

	var entitiesListModule = angular.module('app.directives.entities-search-list', ['app.services' ]);

  entitiesListModule.controller('EntitiesSearchListCtrl', ['$scope', 'Falcon', 'X2jsService', '$window', 'EncodeService',
                                      function($scope, Falcon, X2jsService, $window, encodeService) {

    $scope.downloadEntity = function(type, name) {
      Falcon.logRequest();
      Falcon.getEntityDefinition(type, name) .success(function (data) {
        Falcon.logResponse('success', data, false, true);
        $window.location.href = 'data:application/octet-stream,' + encodeService.encode(data);
      }).error(function (err) {
        Falcon.logResponse('error', err, false);
      });
    };

  }]);

  entitiesListModule.filter('tagFilter', function () {
    return function (items) {
      var filtered = [], i;
      for (i = 0; i < items.length; i++) {
        var item = items[i];
        if(!item.list || !item.list.tag) { item.list = {tag:[""]}; }
        filtered.push(item);
      }
      return filtered;
    };
  });

  entitiesListModule.directive('entitiesSearchList', ["$timeout", 'Falcon', function($timeout, Falcon) {
    return {
      scope: {
        input: "=",
        schedule: "=",
        suspend: "=",
        clone: "=",
        remove: "=",
        edit: "=",
        type: "@",
        tags: "=",
        focusSearch: "=",
        entityDetails:"=",
        entityDefinition:"=",
        resume:"=",
        refresh: "=",
        pages: "=",
        goPage: "="
      },
      controller: 'EntitiesSearchListCtrl',
      restrict: "EA",
      templateUrl: 'html/directives/entitiesSearchListDv.html',
      link: function (scope) {
        scope.server = Falcon;
        scope.$watch('input', function() {
          scope.selectedRows = [];
          scope.checkButtonsToShow();

        }, true);

        scope.selectedRows = [];
        scope.mirrorTag = "_falcon_mirroring_type";

        scope.checkedRow = function (name) {
          var isInArray = false;
          scope.selectedRows.forEach(function(item) {
            if (name === item.name) {
              isInArray = true;
            }
          });
          return isInArray;
        };

        var nameOrder = true;
        scope.toggleSortOrder = function () {
          Falcon.orderBy.enable = true;
          if (nameOrder) {
            Falcon.orderBy.name = 'asc';
          } else {
            Falcon.orderBy.name = 'desc';
          }
          nameOrder = !nameOrder;
          scope.$parent.refreshList(scope.$parent.tags);

        };

        scope.simpleFilter = {};

        scope.selectedDisabledButtons = {
          schedule:true,
          suspend:true,
          resume:true
        };

        scope.checkButtonsToShow = function() {
          var statusCount = {
            "SUBMITTED":0,
            "RUNNING":0,
            "SUSPENDED":0,
            "UNKNOWN":0
          };

          $timeout(function() {

            if(scope.selectedRows.length === scope.input.length){
              scope.selectedAll = true;
            }else{
              scope.selectedAll = false;
            }

            scope.selectedRows.forEach(function(entity) {
              statusCount[entity.status] = statusCount[entity.status]+1;
            });

            if(statusCount.SUBMITTED > 0) {
              if(statusCount.RUNNING > 0 || statusCount.SUSPENDED > 0 || statusCount.UNKNOWN > 0) {
                scope.selectedDisabledButtons = { schedule:true, suspend:true, resume:true };
              }
              else {
                scope.selectedDisabledButtons = { schedule:false, suspend:true, resume:true };
              }
            }
            if(statusCount.RUNNING > 0) {
              if(statusCount.SUBMITTED > 0 || statusCount.SUSPENDED > 0 || statusCount.UNKNOWN > 0) {
                scope.selectedDisabledButtons = { schedule:true, suspend:true, resume:true };
              }
              else {
                scope.selectedDisabledButtons = { schedule:true, suspend:false, resume:true };
              }
            }
            if (statusCount.SUSPENDED > 0) {
              if(statusCount.SUBMITTED > 0 || statusCount.RUNNING > 0 || statusCount.UNKNOWN > 0) {
                scope.selectedDisabledButtons = { schedule:true, suspend:true, resume:true };
              }
              else {
                scope.selectedDisabledButtons = { schedule:true, suspend:true, resume:false };
              }
            }
            if (statusCount.UNKNOWN > 0) {
              scope.selectedDisabledButtons = { schedule:true, suspend:true, resume:true };
            }

            if(scope.selectedRows.length === 0) {
              scope.selectedDisabledButtons = {
                schedule:true,
                suspend:true,
                resume:true
              };
            }
          }, 50);
        };

        var isSelected = function(item){
          var selected = false;
          scope.selectedRows.forEach(function(entity) {
            if(angular.equals(item, entity)){
              selected = true;
            }
          });
          return selected;
        };

        scope.checkAll = function () {
          if(scope.selectedRows.length === scope.input.length){
            angular.forEach(scope.input, function (item) {
              scope.selectedRows.pop();
            });
          }else{
            angular.forEach(scope.input, function (item) {
              var checkbox = {name:item.name, type:item.type, status:item.status};
              if(!isSelected(checkbox)){
                scope.selectedRows.push(checkbox);
              }
            });
          }
        };

        scope.addTag = function(text){
          var added = false;
          angular.forEach(scope.tags, function (scopeTag) {
            if(scopeTag.text === text){
              added = true;
            }
          });
          if(!added){
            var tag = {text:"Tag:"+text};
            scope.tags.push(tag);
            scope.focusSearch();
          }
        };

        scope.scopeEdit = function () {
          scope.edit(scope.selectedRows[0].type, scope.selectedRows[0].name);
        };
        scope.scopeClone = function () {
          scope.clone(scope.selectedRows[0].type, scope.selectedRows[0].name);
        };
        scope.goEntityDefinition = function(name, type) {
          scope.entityDefinition(name, type);
        };
        scope.goEntityDetails = function(name, type) {
          scope.entityDetails(name, type);
        };

        scope.scopeRemove = function () {
          var i;
          for(i = 0; i < scope.selectedRows.length; i++) {
            var multiRequestType = scope.selectedRows[i].type.toLowerCase();
            Falcon.responses.multiRequest[multiRequestType] += 1;
            scope.remove(scope.selectedRows[i].type, scope.selectedRows[i].name);
          }
        };

        scope.scopeSchedule = function () {
          var i;
          for(i = 0; i < scope.selectedRows.length; i++) {
            var multiRequestType = scope.selectedRows[i].type.toLowerCase();
            Falcon.responses.multiRequest[multiRequestType] += 1;
            scope.schedule(scope.selectedRows[i].type, scope.selectedRows[i].name);
          }
        };

        scope.scopeSuspend = function () {
          var i;
          for(i = 0; i < scope.selectedRows.length; i++) {
            var multiRequestType = scope.selectedRows[i].type.toLowerCase();
            Falcon.responses.multiRequest[multiRequestType] += 1;
            scope.suspend(scope.selectedRows[i].type, scope.selectedRows[i].name);
          }
        };
        scope.scopeResume = function () {
          var i;
          for(i = 0; i < scope.selectedRows.length; i++) {
            var multiRequestType = scope.selectedRows[i].type.toLowerCase();
            Falcon.responses.multiRequest[multiRequestType] += 1;
            scope.resume(scope.selectedRows[i].type, scope.selectedRows[i].name);
          }
        };

        scope.download = function() {
          var i;
          for(i = 0; i < scope.selectedRows.length; i++) {
            scope.downloadEntity(scope.selectedRows[i].type, scope.selectedRows[i].name);
          }
        };

        scope.scopeGoPage = function (page) {
          scope.goPage(page);
        };

        scope.isMirror = function(tags){
          var flag = false;
          if(tags !== undefined){
            tags.forEach(function(tag) {
              if(tag.indexOf(scope.mirrorTag) !== -1){
                flag = true;
              }
            });
          }
          return flag;
        };

        scope.displayIcon = function (type, tags) {
          if(type === "FEED"){
            return "entypo download";
          }else if(type === "PROCESS" && scope.isMirror(tags)){
            return "glyphicon glyphicon-duplicate";
          }else{
            return "entypo cycle";
          }
        };

        scope.displayType = function (tag) {
          var tagKeyVal = tag.split("=");
          if(tagKeyVal[0] === "_falcon_mirroring_type"){
            return tagKeyVal[1];
          }else{
            return "";
          }
        };

      }
    };
  }]);

})();