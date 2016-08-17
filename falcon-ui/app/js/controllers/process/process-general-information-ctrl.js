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

  feedModule.controller('ProcessGeneralInformationCtrl', [
    '$scope', 'clustersList', 'feedsList', 'EntityFactory', 'Falcon', 'X2jsService','DateHelper',
    function($scope, clustersList, feedsList, entityFactory, Falcon, X2jsService, DateHelper) {

    $scope.nameValid = false;

    $scope.$watch('process.workflow.spark.jar',function(){
      if($scope.process.workflow.spark.jar && $scope.process.workflow.spark.jar.endsWith('.py')){
        $scope.isPython = true;
      }else{
        $scope.isPython = false;
      }
    },true);

    $scope.init = function() {
      unwrapClusters(clustersList);
      unwrapFeeds(feedsList);
      $scope.dateFormat = DateHelper.getLocaleDateFormat();
    };

    $scope.openDatePicker = function($event, container) {
      $event.preventDefault();
      $event.stopPropagation();
      container.opened = true;
    };

    $scope.validateStartEndDate = function () {
      delete $scope.invalidEndDate;
      if (this.input.start && this.input.end) {
        var startDate = new Date(this.input.start),
          endDate = new Date(this.input.end);
        if (endDate.toString !== 'Invalid Date' && startDate.toString !== 'Invalid Date') {
          if (startDate > endDate) {
            $scope.invalidEndDate = "ng-dirty ng-invalid";
          }
        }
      }
    };

    // TAGS
    $scope.addTag = function() {
      $scope.process.tags.push({key: null, value: null});
    };

    $scope.removeTag = function(index) {
      if(index >= 0 && $scope.process.tags.length > 1) {
        $scope.process.tags.splice(index, 1);
      }
    };

    // inputs
    $scope.addInput = function () {
      $scope.process.inputs.push(entityFactory.newInput());
    };

    $scope.removeInput = function (index) {
      if (index >= 0) {
        $scope.process.inputs.splice(index, 1);
      }
    };

    // OUTPUTS
    $scope.addOutput = function () {
      $scope.process.outputs.push(entityFactory.newOutput());
    };

    $scope.removeOutput = function (index) {
      if (index >= 0) {
        $scope.process.outputs.splice(index, 1);
      }
    };

    $scope.init();

    $scope.getSourceDefinition = function (clusterName) {
      Falcon.getEntityDefinition("cluster", clusterName)
        .success(function (data) {
          $scope.sourceClusterModel = X2jsService.xml_str2json(data);
          var sparkInterface = $scope.sourceClusterModel.cluster.interfaces.interface.filter(
            function(clusterInterface) { return clusterInterface._type === 'spark'; }
          )[0];
          if (sparkInterface) {
            var sparkEndpoint = sparkInterface._endpoint.trim();
            if (sparkEndpoint.indexOf('yarn') != '-1') {
              $scope.process.workflow.spark.master = 'yarn';
              $scope.process.workflow.spark.mode = sparkEndpoint.substring(5);
            } else if (sparkEndpoint === 'local') {
              $scope.process.workflow.spark.master = 'local';
            }
          }
        })
        .error(function (err) {
          Falcon.logResponse('error', err, false, true);
        });
    };

    //-----------PROPERTIES----------------//
    $scope.addProperty = function () {
      var lastOne = $scope.process.properties.length - 1;
      if($scope.process.properties[lastOne].name && $scope.process.properties[lastOne].value) {
        $scope.process.properties.push(entityFactory.newProperty("", ""));
      }
    };

    $scope.removeProperty = function(index) {
      if(index !== null && $scope.process.properties[index]) {
        $scope.process.properties.splice(index, 1);
      }
    };

    $scope.policyChange = function(){
      if($scope.process.retry.policy === 'final'){
       $scope.process.retry.delay.quantity = '0';
       $scope.process.retry.delay.unit = 'minutes';
       $scope.process.retry.attempts = '0';
      }else{
        $scope.process.retry.delay.quantity = '30';
        $scope.process.retry.delay.unit = 'minutes';
        $scope.process.retry.attempts = '3';
      }
    }

    function unwrapClusters(clusters) {
      $scope.clusterList = [];
      var typeOfData = Object.prototype.toString.call(clusters.entity);
      if(typeOfData === "[object Array]") {
        $scope.clusterList = clusters.entity;
      } else if(typeOfData === "[object Object]") {
        $scope.clusterList = [clusters.entity];
      } else {
        //console.log("type of data not recognized");
      }
    }

    function unwrapFeeds(feeds) {
      $scope.feedsList = [];
      var typeOfData = Object.prototype.toString.call(feeds.entity);
      if (typeOfData === "[object Array]") {
        $scope.feedsList = feeds.entity;
      } else if (typeOfData === "[object Object]") {
        $scope.feedsList = [feeds.entity];
      } else {
        //console.log("type of data not recognized");
      }
    }

  }]);


})();
