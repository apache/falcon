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

  entitiesListModule.controller('InstancesListCtrl', ['$scope', 'Falcon', 'X2jsService', '$window', 'EncodeService', "DateHelper",
                                      function($scope, Falcon, X2jsService, $window, encodeService, DateHelper) {

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

  entitiesListModule.directive('instancesList', ["$timeout", 'Falcon', '$filter', 'DateHelper', function($timeout, Falcon, $filter, DateHelper) {
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
      link: function (scope, element) {
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

        var dateFormat = DateHelper.getLocaleDateFormat().toLowerCase();
        scope.dateFormat = DateHelper.getLocaleDateFormat() + ' HH:mm';
        var dateSeperator;
        if(dateFormat.indexOf('.') >=0){
          dateSeperator = '.';
        }else if (dateFormat.indexOf('-') >=0) {
          dateSeperator='-'
        }else {
          dateSeperator = '/';
        }

        var splitDate = dateFormat.split(dateSeperator);
        var mask ='';
        splitDate.forEach(function(value, index){
          if(value.indexOf('d')>=0){
            mask = mask + dateSeperator +'00'
          }else if (value.indexOf('m')>=0) {
            mask =  mask + dateSeperator +'00'
          }else if (value.indexOf('y')>=0) {
            if(value.length > 2){
              mask =  mask + dateSeperator +'0000'
            }else{
              mask =  mask + dateSeperator +'00'
            }
          }
        });
        mask = mask + ' 00:00';
        console.log(mask);
        element.find('.dateInput').mask(mask.substr(1));

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

        var CountDown = function(countParam){
		this.count=countParam;
		this.down=function(){
			this.count--;
		}
		this.isDone=function(){
			return this.count<1;
		}
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

        var resumeInstance = function (type, name, start, end, countDown) {
          Falcon.logRequest();
          Falcon.postResumeInstance(type, name, start, end)
            .success(function (message) {
              countDown.down();
              Falcon.logResponse('success', message, type);
              if(countDown.isDone()){
                scope.$parent.refreshInstanceList(scope.type, scope.name, scope.start, scope.end);
              }
            })
            .error(function (err) {
              countDown.down();
              Falcon.logResponse('error', err, type);

            });
        };

        var suspendInstance = function (type, name, start, end, countDown) {
          Falcon.logRequest();
          Falcon.postSuspendInstance(type, name, start, end)
            .success(function (message) {
              countDown.down();
              Falcon.logResponse('success', message, type);
              if(countDown.isDone()){
                scope.$parent.refreshInstanceList(scope.type, scope.name, scope.start, scope.end);
              }
            })
            .error(function (err) {
              countDown.down();
              Falcon.logResponse('error', err, type);

            });
        };

        var reRunInstance = function (type, name, start, end, countDown) {
          Falcon.logRequest();
          Falcon.postReRunInstance(type, name, start, end)
            .success(function (message) {
              countDown.down();
              if(countDown.isDone()){
                $timeout(function () {
                  Falcon.logResponse('success', message, type);
                  scope.$parent.refreshInstanceList(scope.type, scope.name, scope.start, scope.end);
                }, 10000);
              } else {
                Falcon.logResponse('success', message, type);
              }
            })
            .error(function (err) {
              countDown.down();
              Falcon.logResponse('error', err, type);

            });
        };

        var killInstance = function (type, name, start, end, countDown) {
          Falcon.logRequest();
          Falcon.postKillInstance(type, name, start, end)
            .success(function (message) {
              countDown.down();
              Falcon.logResponse('success', message, type);
              if(countDown.isDone()){
                scope.$parent.refreshInstanceList(scope.type, scope.name, scope.start, scope.end);
              }
            })
            .error(function (err) {
              countDown.down();
              Falcon.logResponse('error', err, type);

            });
        };

        scope.scopeResume = function () {
          var countDown=new CountDown(scope.selectedRows.length);
          for(var i = 0; i < scope.selectedRows.length; i++) {
            var multiRequestType = scope.selectedRows[i].type.toLowerCase();
            Falcon.responses.multiRequest[multiRequestType] += 1;
            var start = scope.selectedRows[i].instance;
            var end = addOneMin(start);
            resumeInstance(scope.type, scope.name, start, end, countDown);
          }
        };

        scope.scopeSuspend = function () {
          var countDown=new CountDown(scope.selectedRows.length);
          for(var i = 0; i < scope.selectedRows.length; i++) {
            var multiRequestType = scope.selectedRows[i].type.toLowerCase();
            Falcon.responses.multiRequest[multiRequestType] += 1;
            var start = scope.selectedRows[i].instance;
            var end = addOneMin(start);
            suspendInstance(scope.type, scope.name, start, end, countDown);
          }
        };

        scope.scopeRerun = function () {
          var countDown=new CountDown(scope.selectedRows.length);
          for(var i = 0; i < scope.selectedRows.length; i++) {
            var multiRequestType = scope.selectedRows[i].type.toLowerCase();
            Falcon.responses.multiRequest[multiRequestType] += 1;
            var start = scope.selectedRows[i].instance;
            var end = addOneMin(start);
            reRunInstance(scope.type, scope.name, start, end, countDown);
          }
        };

        scope.scopeKill = function () {
          var countDown=new CountDown(scope.selectedRows.length);
          for(var i = 0; i < scope.selectedRows.length; i++) {
            var multiRequestType = scope.selectedRows[i].type.toLowerCase();
            Falcon.responses.multiRequest[multiRequestType] += 1;
            var start = scope.selectedRows[i].instance;
            var end = addOneMin(start);
            killInstance(scope.type, scope.name, start, end, countDown);
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

        scope.startDateValid = true;
        scope.endDateValid = true;

        scope.validateDate = function(event, type){
          var which = event.which || event.keyCode;
          var charStr = String.fromCharCode(which);
          var valueEntered = event.target.value + charStr;
          var dateSeperator;
          if(dateFormat.indexOf('.') >=0){
            dateSeperator = '.';
          }else if (dateFormat.indexOf('-') >=0) {
            dateSeperator='-'
          }else {
            dateSeperator = '/';
          }

          var splitDateFormat = dateFormat.split(dateSeperator);
          var dateArr = [];

          var completeDate = valueEntered.split(" ");
          var dates = completeDate[0].split(dateSeperator);
          var timeArr = [];
          if(completeDate[1]){
            completeDate[1].split(":")
          }
          splitDateFormat.forEach(function(value, index){
            if(value.indexOf('d')>=0){
              dateArr[0] = dates[index];
            }else if (value.indexOf('m')>=0) {
              dateArr[1] = dates[index];
            }else if (value.indexOf('y')>=0) {
              dateArr[2] = dates[index];
            }
          });
          var dateValid = checkDateTimeValidity(dateArr, timeArr);
          if(type === 'start'){
            scope.startDateValid = dateValid;
          }else if(type === 'end'){
            scope.endDateValid = dateValid;
          }
        };

        var checkDateTimeValidity = function(dateArr, timeArr){
          var dateValid = false;
         if(dateArr[2]){
           if(parseInt(dateArr[2]) > 0 && parseInt(dateArr[2]) <= 9999){
             dateValid = true;
           }else {
             return false;
           }
         }
         if(dateArr[1]){
           if(parseInt(dateArr[1]) > 0 && parseInt(dateArr[1]) <= 12){
             dateValid = true;
           }else {
             return false;
           }
         }
         if(dateArr[0]) {
           if(parseInt(dateArr[0]) > 0 && parseInt(dateArr[0]) <= daysInMonth(dateArr[1], dateArr[2])){
             dateValid = true;
           }else {
             return false;
           }
         }
         if(timeArr[0]){
           if(parseInt(timeArr[0]) >= 0 && parseInt(timeArr[0]) < 24){
             dateValid = true;
           }else {
             return false;
           }
         }
         if(timeArr[1]){
           if(parseInt(timeArr[1]) >= 0 && parseInt(timeArr[1] < 60)){
             dateValid = true;
           }else {
             return false;
           }
         }
         return dateValid;
        }

        var changeDateFormat = function(date){

          var dateSeperator;
          if(dateFormat.indexOf('.') >=0){
            dateSeperator = '.';
          }else if (dateFormat.indexOf('-') >=0) {
            dateSeperator='-'
          }else {
            dateSeperator = '/';
          }

          var splitDateFormat = dateFormat.split(dateSeperator);
          var dateArr = [];

          var completeDate = date.split(" ");
          var dates = completeDate[0].split(dateSeperator);

          splitDateFormat.forEach(function(value, index){
            if(value.indexOf('d')>=0){
              dateArr[0] = dates[index];
            }else if (value.indexOf('m')>=0) {
              dateArr[1] = dates[index];
            }else if (value.indexOf('y')>=0) {
              dateArr[2] = dates[index];
            }
          });

          date = dateArr[2] + "-" + dateArr[1] + "-" + dateArr[0] + "T" + completeDate[1] + "Z";
          return date;
        };

        var validateDateFormat = function(date){
          var date = new Date(changeDateFormat(date));
          return !isNaN(date.getTime());
        };

        function daysInMonth(month, year) {
          switch (month) {
            case 2 :
              return (year % 4 == 0 && year % 100) || year % 400 == 0 ? 29 : 28;
            case 9 : case 4 : case 6 : case 11 :
              return 30;
            default :
              return 31
    }
}

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
                scope.startDateValid = false;
              }else if(!validateDateFormat(scope.endFilter)){
                executeFilter = false;
                scope.endFilterError = true;
                scope.endDateValid = false;
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