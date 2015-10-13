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

	var entitiesListModule = angular.module('app.directives.instances-list', ['app.services' ]);

  entitiesListModule.controller('InstancesListCtrl', ['$scope', 'Falcon', 'X2jsService', '$window', 'EncodeService',
                                      function($scope, Falcon, X2jsService, $window, encodeService) {

    //$scope.downloadEntity = function(logURL) {
    //  Falcon.logRequest();
    //  Falcon.getInstanceLog(logURL) .success(function (data) {
    //    Falcon.logResponse('success', data, false, true);
    //    $window.location.href = 'data:application/octet-stream,' + encodeService.encode(data);
    //  }).error(function (err) {
    //    Falcon.logResponse('error', err, false);
    //  });
    //};

    $scope.downloadEntity = function(logURL) {
      $window.location.href = logURL;
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

  entitiesListModule.directive('instancesList', ["$timeout", 'Falcon', '$filter', function($timeout, Falcon, $filter) {
    return {
      scope: {
        input: "=",
        type: "=",
        name: "=",
        start: "=",
        end: "=",
        instanceDetails:"=",
        refresh: "=",
        pages: "=",
        nextPages: "=",
        prevPages: "=",
        goPage: "=",
        changePagesSet: "="
      },
      controller: 'InstancesListCtrl',
      restrict: "EA",
      templateUrl: 'html/directives/instancesListDv.html',
      link: function (scope) {
        scope.server = Falcon;
        scope.$watch(function () { return scope.input; }, function() {
          scope.selectedRows = [];
          scope.checkButtonsToShow();
        }, true);

        var resultsPerPage = 10;
        var visiblePages = 3;
        scope.selectedRows = [];
        scope.$parent.refreshInstanceList(scope.type, scope.name, scope.start, scope.end);

        scope.startSortOrder = "desc";
        scope.endSortOrder = "desc";
        scope.statusSortOrder = "desc";

        scope.checkedRow = function (name) {
          var isInArray = false;
          scope.selectedRows.forEach(function(item) {
            if (name === item.instance) {
              isInArray = true;
            }
          });
          return isInArray;
        };

        scope.simpleFilter = {};

        scope.selectedDisabledButtons = {
          schedule:true,
          suspend:true,
          resume:true,
          stop:true
        };

        scope.checkButtonsToShow = function() {
          var statusCount = {
            "SUBMITTED":0,
            "RUNNING":0,
            "SUSPENDED":0,
            "UNKNOWN":0,
            "KILLED":0,
            "WAITING":0,
            "FAILED":0,
            "SUCCEEDED":0
          };

          $timeout(function() {

            if(scope.selectedRows.length === scope.input.length){
              scope.selectedAll = true;
            }else{
              scope.selectedAll = false;
            }

            scope.selectedRows.forEach(function(instance) {
              statusCount[instance.status] = statusCount[instance.status]+1;
            });

            if(statusCount.SUBMITTED > 0) {
              if(statusCount.RUNNING > 0 || statusCount.SUSPENDED > 0 || statusCount.UNKNOWN > 0 || statusCount.KILLED > 0 || statusCount.WAITING > 0 || statusCount.FAILED > 0) {
                scope.selectedDisabledButtons = { schedule:true, suspend:true, resume:true, stop:true, rerun:true };
              }
              else {
                scope.selectedDisabledButtons = { schedule:false, suspend:true, resume:true, stop:true, rerun:true };
              }
            }
            if(statusCount.RUNNING > 0) {
              if(statusCount.SUBMITTED > 0 || statusCount.SUSPENDED > 0 || statusCount.UNKNOWN > 0 || statusCount.KILLED > 0 || statusCount.WAITING > 0 || statusCount.FAILED > 0) {
                scope.selectedDisabledButtons = { schedule:true, suspend:true, resume:true, stop:false, rerun:true };
              }
              else {
                scope.selectedDisabledButtons = { schedule:true, suspend:false, resume:true, stop:false, rerun:true  };
              }
            }
            if (statusCount.SUSPENDED > 0) {
              if(statusCount.SUBMITTED > 0 || statusCount.RUNNING > 0 || statusCount.UNKNOWN > 0 || statusCount.KILLED > 0 || statusCount.WAITING > 0 || statusCount.FAILED > 0) {
                scope.selectedDisabledButtons = { schedule:true, suspend:true, resume:true, stop:false, rerun:true };
              }
              else {
                scope.selectedDisabledButtons = { schedule:true, suspend:true, resume:false, stop:false, rerun:true };
              }
            }
            if (statusCount.KILLED > 0) {
              if(statusCount.SUBMITTED > 0 || statusCount.SUSPENDED > 0 || statusCount.RUNNING > 0 || statusCount.UNKNOWN > 0 || statusCount.WAITING > 0 || statusCount.FAILED > 0) {
                scope.selectedDisabledButtons = { schedule:true, suspend:true, resume:true, stop:true, rerun:true };
              }
              else {
                scope.selectedDisabledButtons = { schedule:true, suspend:true, resume:true, stop:true, rerun:false };
              }
            }
            if(statusCount.WAITING > 0) {
              if(statusCount.SUBMITTED > 0 || statusCount.RUNNING > 0 || statusCount.SUSPENDED > 0 || statusCount.UNKNOWN > 0 || statusCount.KILLED > 0 || statusCount.FAILED > 0) {
                scope.selectedDisabledButtons = { schedule:true, suspend:true, resume:true, stop:true, rerun:true };
              }
              else {
                scope.selectedDisabledButtons = { schedule:true, suspend:true, resume:true, stop:true, rerun:true  };
              }
            }
            if (statusCount.FAILED > 0) {
              if(statusCount.SUBMITTED > 0 || statusCount.SUSPENDED > 0 || statusCount.RUNNING > 0 || statusCount.UNKNOWN > 0 || statusCount.KILLED > 0 || statusCount.WAITING > 0) {
                scope.selectedDisabledButtons = { schedule:true, suspend:true, resume:true, stop:true, rerun:true };
              }
              else {
                scope.selectedDisabledButtons = { schedule:true, suspend:true, resume:true, stop:true, rerun:false };
              }
            }
            if(statusCount.SUCCEEDED > 0) {
              if(statusCount.SUBMITTED > 0 || statusCount.RUNNING > 0 || statusCount.SUSPENDED > 0 || statusCount.UNKNOWN > 0 || statusCount.KILLED > 0 || statusCount.WAITING > 0 || statusCount.FAILED > 0) {
                scope.selectedDisabledButtons = { schedule:true, suspend:true, resume:true, stop:true, rerun:true };
              }
              else {
                scope.selectedDisabledButtons = { schedule:true, suspend:true, resume:true, stop:true, rerun:false };
              }
            }
            if (statusCount.UNKNOWN > 0) {
              scope.selectedDisabledButtons = { schedule:true, suspend:true, resume:true, stop:true, rerun:true };
            }

            if(scope.selectedRows.length === 0) {
              scope.selectedDisabledButtons = {
                schedule:true,
                resume:true,
                suspend:true,
                stop:true,
                rerun:true
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
        }

        scope.checkAll = function () {
          if(scope.selectedRows.length >= scope.input.length){
            angular.forEach(scope.input, function (item) {
              scope.selectedRows.pop();
            });
          }else{
            angular.forEach(scope.input, function (item) {
              var checkbox = {'instance':item.instance, 'startTime':item.startTime, 'endTime':item.endTime, 'status':item.status, 'type':scope.type, 'logFile':item.logFile};
              if(!isSelected(checkbox)){
                scope.selectedRows.push(checkbox);
              }
            });
          }
        };

        scope.goInstanceDetails = function(instance) {
          scope.instanceDetails(instance);
        };

        var resumeInstance = function (type, name, start, end, refresh) {
          Falcon.logRequest();
          Falcon.postResumeInstance(type, name, start, end)
            .success(function (message) {
              Falcon.logResponse('success', message, type);
              if(refresh){
                scope.$parent.refreshInstanceList(scope.type, scope.name, scope.start, scope.end);
              }
            })
            .error(function (err) {
              Falcon.logResponse('error', err, type);

            });
        };

        var suspendInstance = function (type, name, start, end, refresh) {
          Falcon.logRequest();
          Falcon.postSuspendInstance(type, name, start, end)
            .success(function (message) {
              Falcon.logResponse('success', message, type);
              if(refresh){
                scope.$parent.refreshInstanceList(scope.type, scope.name, scope.start, scope.end);
              }
            })
            .error(function (err) {
              Falcon.logResponse('error', err, type);

            });
        };

        var reRunInstance = function (type, name, start, end, refresh) {
          Falcon.logRequest();
          Falcon.postReRunInstance(type, name, start, end)
            .success(function (message) {
              Falcon.logResponse('success', message, type);
              if(refresh){
                scope.$parent.refreshInstanceList(scope.type, scope.name, scope.start, scope.end);
              }
            })
            .error(function (err) {
              Falcon.logResponse('error', err, type);

            });
        };

        var killInstance = function (type, name, start, end, refresh) {
          Falcon.logRequest();
          Falcon.postKillInstance(type, name, start, end)
            .success(function (message) {
              Falcon.logResponse('success', message, type);
              if(refresh){
                scope.$parent.refreshInstanceList(scope.type, scope.name, scope.start, scope.end);
              }
            })
            .error(function (err) {
              Falcon.logResponse('error', err, type);

            });
        };

        scope.scopeResume = function () {
          for(var i = 0; i < scope.selectedRows.length; i++) {
            var multiRequestType = scope.selectedRows[i].type.toLowerCase();
            Falcon.responses.multiRequest[multiRequestType] += 1;
            var start = scope.selectedRows[i].instance;
            var end = addOneMin(start);
            var refresh = i === scope.selectedRows.length-1 ? true : false;
            resumeInstance(scope.type, scope.name, start, end, refresh);
          }
        };

        scope.scopeSuspend = function () {
          for(var i = 0; i < scope.selectedRows.length; i++) {
            var multiRequestType = scope.selectedRows[i].type.toLowerCase();
            Falcon.responses.multiRequest[multiRequestType] += 1;
            var start = scope.selectedRows[i].instance;
            var end = addOneMin(start);
            var refresh = i === scope.selectedRows.length-1 ? true : false;
            suspendInstance(scope.type, scope.name, start, end, refresh);
          }
        };

        scope.scopeRerun = function () {
          for(var i = 0; i < scope.selectedRows.length; i++) {
            var multiRequestType = scope.selectedRows[i].type.toLowerCase();
            Falcon.responses.multiRequest[multiRequestType] += 1;
            var start = scope.selectedRows[i].instance;
            var end = addOneMin(start);
            var refresh = i === scope.selectedRows.length-1 ? true : false;
            reRunInstance(scope.type, scope.name, start, end, refresh);
          }
        };

        scope.scopeKill = function () {
          for(var i = 0; i < scope.selectedRows.length; i++) {
            var multiRequestType = scope.selectedRows[i].type.toLowerCase();
            Falcon.responses.multiRequest[multiRequestType] += 1;
            var start = scope.selectedRows[i].instance;
            var end = addOneMin(start);
            var refresh = i === scope.selectedRows.length-1 ? true : false;
            killInstance(scope.type, scope.name, start, end, refresh);
          }
        };

        scope.download = function() {
          var i;
          for(i = 0; i < scope.selectedRows.length; i++) {
            scope.downloadEntity(scope.selectedRows[i].logFile);
          }
        };

        scope.scopeGoPage = function (page) {
          scope.goPage(page);
        };

        scope.scopeNextOffset = function (page) {
          var offset = (parseInt(scope.pages[0].label)+(visiblePages-1))*resultsPerPage;
          scope.changePagesSet(offset, page, 0, scope.start, scope.end);
        };

        scope.scopePrevOffset = function (page) {
          var offset = (parseInt(scope.pages[0].label)-(visiblePages+1))*resultsPerPage;
          scope.changePagesSet(offset, page, visiblePages-1, scope.start, scope.end);
        };

        scope.validateDate = function(event, type){
          var which = event.which || event.keyCode;
          var charStr = String.fromCharCode(which);
          event.preventDefault();
          if (
            which == 8 || which == 46 || which == 37 || which == 39 ||
            (which >= 48 && which <= 57)
          ) {
            if(type == "start"){
              if(scope.startFilter){
                if(scope.startFilter.length == 1){
                  //mm
                  var prevChar = scope.startFilter.substring(scope.startFilter.length-1);
                  prevChar = parseInt(prevChar);
                  if(prevChar < 1){
                    if(prevChar == 0 && charStr == 0){

                    }else if(charStr <= 9){
                      scope.startFilter += charStr + "/";
                    }
                  }else{
                    if(charStr <= 2){
                      scope.startFilter += charStr + "/";
                    }
                  }
                }else if(scope.startFilter.length == 2){
                  //mm/
                  if(charStr <= 3){
                    scope.startFilter += "/" + charStr;
                  }
                }else if(scope.startFilter.length == 3){
                  //mm/d
                  if(charStr <= 3){
                    scope.startFilter += charStr;
                  }
                }else if(scope.startFilter.length == 4){
                  //mm/dd
                  var prevChar = scope.startFilter.substring(scope.startFilter.length-1);
                  prevChar = parseInt(prevChar);
                  if(prevChar < 3){
                    if(prevChar == 0 && charStr == 0){

                    }else if(charStr <= 9){
                      scope.startFilter += charStr + "/";
                    }
                  }else{
                    if(charStr <= 1){
                      scope.startFilter += charStr + "/";
                    }
                  }
                }else if(scope.startFilter.length == 5){
                  //mm/dd/
                  if(charStr <= 2){
                    scope.startFilter += "/" + charStr;
                  }
                }else if(scope.startFilter.length == 6){
                  //mm/dd/y
                  if(charStr <= 2){
                    scope.startFilter += charStr;
                  }
                }else if(scope.startFilter.length == 7){
                  //mm/dd/yy
                  if(charStr <= 9){
                    scope.startFilter += charStr;
                  }
                }else if(scope.startFilter.length == 8){
                  //mm/dd/yyy
                  if(charStr <= 9){
                    scope.startFilter += charStr;
                  }
                }else if(scope.startFilter.length == 9){
                  //mm/dd/yyyy
                  if(charStr <= 9){
                    scope.startFilter += charStr + " ";
                  }
                }else if(scope.startFilter.length == 10){
                  //mm/dd/yyyy
                  if(charStr <= 2){
                    scope.startFilter += " " + charStr;
                  }
                }else if(scope.startFilter.length == 11){
                  //mm/dd/yyyy h
                  if(charStr <= 2){
                    scope.startFilter += charStr;
                  }
                }else if(scope.startFilter.length == 12){
                  //mm/dd/yyyy hh
                  var prevChar = scope.startFilter.substring(scope.startFilter.length-1);
                  prevChar = parseInt(prevChar);
                  if(prevChar < 2){
                    if(charStr <= 9){
                      scope.startFilter += charStr + ":";
                    }
                  }else{
                    if(charStr <= 4){
                      scope.startFilter += charStr + ":";
                    }
                  }
                }else if(scope.startFilter.length == 13){
                  //mm/dd/yyyy hh:
                  if(charStr <= 5){
                    scope.startFilter += ":" + charStr;
                  }
                }else if(scope.startFilter.length == 14){
                  //mm/dd/yyyy hh:m
                  if(charStr <= 5){
                    scope.startFilter += charStr;
                  }
                }else if(scope.startFilter.length == 15){
                  //mm/dd/yyyy hh:mm
                  if(charStr <= 9){
                    scope.startFilter += charStr;
                    scope.startFilterError = false;
                  }
                }
              }else{
                //m
                if(charStr <= 1){
                  scope.startFilter = charStr;
                }
              }
            }else{
              if(scope.endFilter){
                if(scope.endFilter.length == 1){
                  //mm
                  var prevChar = scope.endFilter.substring(scope.endFilter.length-1);
                  prevChar = parseInt(prevChar);
                  if(prevChar < 1){
                    if(prevChar == 0 && charStr == 0){

                    }else if(charStr <= 9){
                      scope.endFilter += charStr + "/";
                    }
                  }else{
                    if(charStr <= 2){
                      scope.endFilter += charStr + "/";
                    }
                  }
                }else if(scope.endFilter.length == 2){
                  //mm/
                  if(charStr <= 3){
                    scope.endFilter += "/" + charStr;
                  }
                }else if(scope.endFilter.length == 3){
                  //mm/d
                  if(charStr <= 3){
                    scope.endFilter += charStr;
                  }
                }else if(scope.endFilter.length == 4){
                  //mm/dd
                  var prevChar = scope.endFilter.substring(scope.endFilter.length-1);
                  prevChar = parseInt(prevChar);
                  if(prevChar < 3){
                    if(prevChar == 0 && charStr == 0){

                    }else if(charStr <= 9){
                      scope.endFilter += charStr + "/";
                    }
                  }else{
                    if(charStr <= 1){
                      scope.endFilter += charStr + "/";
                    }
                  }
                }else if(scope.endFilter.length == 5){
                  //mm/dd/
                  if(charStr <= 2){
                    scope.endFilter += "/" + charStr;
                  }
                }else if(scope.endFilter.length == 6){
                  //mm/dd/y
                  if(charStr <= 2){
                    scope.endFilter += charStr;
                  }
                }else if(scope.endFilter.length == 7){
                  //mm/dd/yy
                  if(charStr <= 9){
                    scope.endFilter += charStr;
                  }
                }else if(scope.endFilter.length == 8){
                  //mm/dd/yyy
                  if(charStr <= 9){
                    scope.endFilter += charStr;
                  }
                }else if(scope.endFilter.length == 9){
                  //mm/dd/yyyy
                  if(charStr <= 9){
                    scope.endFilter += charStr + " ";
                  }
                }else if(scope.endFilter.length == 10){
                  //mm/dd/yyyy
                  if(charStr <= 2){
                    scope.endFilter += " " + charStr;
                  }
                }else if(scope.endFilter.length == 11){
                  //mm/dd/yyyy h
                  if(charStr <= 2){
                    scope.endFilter += charStr;
                  }
                }else if(scope.endFilter.length == 12){
                  //mm/dd/yyyy hh
                  var prevChar = scope.endFilter.substring(scope.endFilter.length-1);
                  prevChar = parseInt(prevChar);
                  if(prevChar < 2){
                    if(charStr <= 9){
                      scope.endFilter += charStr + ":";
                    }
                  }else{
                    if(charStr <= 4){
                      scope.endFilter += charStr + ":";
                    }
                  }
                }else if(scope.endFilter.length == 13){
                  //mm/dd/yyyy hh:
                  if(charStr <= 5){
                    scope.endFilter += ":" + charStr;
                  }
                }else if(scope.endFilter.length == 14){
                  //mm/dd/yyyy hh:m
                  if(charStr <= 5){
                    scope.endFilter += charStr;
                  }
                }else if(scope.endFilter.length == 15){
                  //mm/dd/yyyy hh:mm
                  if(charStr <= 9){
                    scope.endFilter += charStr;
                    scope.endFilterError = false;
                  }
                }
              }else{
                //m
                if(charStr <= 1){
                  scope.endFilter = charStr;
                }
              }
            }
          }
        };

        var changeDateFormat = function(date){
          var completeDate = date.split(" ");
          var dates = completeDate[0].split("/");
          date = dates[2] + "-" + dates[0] + "-" + dates[1] + "T" + completeDate[1] + "Z";
          return date;
        };

        var validateDateFormat = function(date){
          var char = date.substring(0, 1);
          if(isNaN(char)){
            return false;
          }
          char = date.substring(1, 2);
          if(isNaN(char)){
            return false;
          }
          char = date.substring(2, 3);
          if(char != "/"){
            return false;
          }
          char = date.substring(3, 4);
          if(isNaN(char)){
            return false;
          }
          char = date.substring(4, 5);
          if(isNaN(char)){
            return false;
          }
          char = date.substring(5, 6);
          if(char != "/"){
            return false;
          }
          char = date.substring(6, 7);
          if(isNaN(char)){
            return false;
          }
          char = date.substring(7, 8);
          if(isNaN(char)){
            return false;
          }
          char = date.substring(8, 9);
          if(isNaN(char)){
            return false;
          }
          char = date.substring(9, 10);
          if(isNaN(char)){
            return false;
          }
          char = date.substring(10, 11);
          if(char != " "){
            return false;
          }
          char = date.substring(11, 12);
          if(isNaN(char)){
            return false;
          }
          char = date.substring(12, 13);
          if(isNaN(char)){
            return false;
          }
          char = date.substring(13, 14);
          if(char != ":"){
            return false;
          }
          char = date.substring(14, 15);
          if(isNaN(char)){
            return false;
          }
          char = date.substring(15, 16);
          if(isNaN(char)){
            return false;
          }
          return true;
        };

        scope.filterInstances = function(orderBy){
          var start;
          var end;
          var executeFilter = false;
          scope.startFilterError = false;
          scope.endFilterError = false;
          scope.startAfterEndError = false;
          scope.startAfterNominalError = false;
          scope.startBeforeNominalError = false;
          scope.endAfterNominalError = false;
          scope.endBeforeNominalError = false;
          var nominalStartDate = new Date(scope.start);
          var nominalEndDate = new Date(scope.end);
          if(scope.startFilter && scope.endFilter){
            if(scope.startFilter.length == 16 && scope.endFilter.length == 16){
              if(!validateDateFormat(scope.startFilter)){
                executeFilter = false;
                scope.startFilterError = true;
              }else if(!validateDateFormat(scope.endFilter)){
                executeFilter = false;
                scope.endFilterError = true;
              }else{
                start = changeDateFormat(scope.startFilter);
                var filterStartDate = new Date(start);
                end = changeDateFormat(scope.endFilter);
                var filterEndDate = new Date(end);
                if(filterStartDate > filterEndDate){
                  executeFilter = false;
                  scope.startAfterEndError = true;
                }else{
                  if(filterStartDate < nominalStartDate){
                    executeFilter = false;
                    scope.startAfterNominalError = true;
                  }else if(filterStartDate > nominalEndDate){
                    executeFilter = false;
                    scope.startBeforeNominalError = true;
                  }else if(filterEndDate < nominalStartDate){
                    executeFilter = false;
                    scope.endAfterNominalError = true;
                  }else if(filterEndDate > nominalEndDate){
                    executeFilter = false;
                    scope.endBeforeNominalError = true;
                  }else{
                    executeFilter = true;
                  }
                }
              }
            }else{
              if(scope.startFilter.length != 16){
                scope.startFilterError = true;
              }
              if(scope.endFilter.length != 16){
                scope.endFilterError = true;
              }
            }
          }else if(scope.startFilter){
            scope.endFilterError = false;
            if(scope.startFilter.length == 16){
              if(!validateDateFormat(scope.startFilter)){
                executeFilter = false;
                scope.startFilterError = true;
              }else{
                start = changeDateFormat(scope.startFilter);
                var filterStartDate = new Date(start);
                if(filterStartDate < nominalStartDate){
                  executeFilter = false;
                  scope.startAfterNominalError = true;
                }else if(filterStartDate > nominalEndDate){
                  executeFilter = false;
                  scope.startBeforeNominalError = true;
                }else{
                  executeFilter = true;
                }
              }
            }else{
              scope.startFilterError = true;
            }
          }else if(scope.endFilter){
            scope.startFilterError = false;
            if(scope.endFilter.length == 16){
              if(!validateDateFormat(scope.endFilter)){
                executeFilter = false;
                scope.endFilterError = true;
              }else{
                end = changeDateFormat(scope.endFilter);
                var filterEndDate = new Date(end);
                if(filterEndDate < nominalStartDate){
                  executeFilter = false;
                  scope.endAfterNominalError = true;
                }else if(filterEndDate > nominalEndDate){
                  executeFilter = false;
                  scope.endBeforeNominalError = true;
                }else{
                  executeFilter = true;
                }
              }
            }else{
              scope.endFilterError = true;
            }
          }else{
            executeFilter = true;
          }

          if(executeFilter){
            var sortOrder = "";
            if(orderBy){
              if(orderBy === "startTime"){
                if(scope.startSortOrder === "desc"){
                  scope.startSortOrder = "asc";
                }else{
                  scope.startSortOrder = "desc";
                }
                sortOrder = scope.startSortOrder;
              }else if(orderBy === "endTime"){
                if(scope.endSortOrder === "desc"){
                  scope.endSortOrder = "asc";
                }else{
                  scope.endSortOrder = "desc";
                }
                sortOrder = scope.endSortOrder;
              }else if(orderBy === "status"){
                if(scope.statusSortOrder === "desc"){
                  scope.statusSortOrder = "asc";
                }else{
                  scope.statusSortOrder = "desc";
                }
                sortOrder = scope.statusSortOrder;
              }
            }else{
              orderBy = "startTime";
              sortOrder = "desc";
            }

            if(!start){
              start = scope.start;
            }
            if(!end){
              end = scope.end;
            }

            scope.$parent.refreshInstanceList(scope.type, scope.name, start, end, scope.statusFilter, orderBy, sortOrder);
          }
        }

        var addOneMin = function(time){
          var newtime = parseInt(time.substring(time.length-3, time.length-1));
          if(newtime === 59){
            newtime = 0;
          }else{
            newtime++;
          }
          if(newtime < 10){
            newtime = "0"+newtime;
          }
          return time.substring(0, time.length-3) + newtime + "Z";
        }

      }
    };
  }]);

})();