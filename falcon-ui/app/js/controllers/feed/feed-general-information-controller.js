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
  var feedModule = angular.module('app.controllers.feed');


  feedModule.controller('FeedGeneralInformationController', [ "$scope","clustersList",'EntityFactory',
    function ($scope, clustersList, entityFactory) {

    $scope.propPlaceholders = {
      queueName: 'default',
      jobPriority: '',
      parallel: 3,
      maxMaps: 8,
      mapBandwidthKB: 1024
    };

    unwrapClusters(clustersList);
    $scope.nameValid = false;

    $scope.addTag = function () {
      $scope.feed.tags.push({key: null, value: null});
    };

    $scope.removeTag = function (index) {
      if (index >= 0 && $scope.feed.tags.length > 1) {
        $scope.feed.tags.splice(index, 1);
      }
    };

    function unwrapClusters(clusters) {
      if(clusters !== undefined && clusters !== null && clusters !== "null"){
        $scope.clustersList = [];
        var typeOfData = Object.prototype.toString.call(clusters.entity);
        if(typeOfData === "[object Array]") {
          $scope.clustersList = clusters.entity;
        } else if(typeOfData === "[object Object]") {
          $scope.clustersList = [clusters.entity];
        } else {
          //console.log("type of data not recognized");
        }
      }
    }

    function findClusterExists(clusterList, newClusterName, newClusterType) {
      var clusterExists = false;
      clusterList.forEach(function (cluster) {
        if (cluster.name === newClusterName && cluster.type === newClusterType) {
          clusterExists = true;
          return;
        }
      });
      return clusterExists;
    }

    $scope.onTypeChange = function(value){
      $scope.feed.clusters = [];
      if (value === 'hive' || value ==='hdfs') {
        $scope.feed.sourceClusterLocationType = value;
        $scope.feed.targetClusterLocationType = value;
        $scope.feed.clusters.push(
          entityFactory.newCluster('source', $scope.feed.sourceClusterLocationType, "", null));
        $scope.feed.clusters.push(
            entityFactory.newCluster('target', $scope.feed.targetClusterLocationType, "", null));
      } else if (value === 'import') {
        $scope.feed.targetClusterLocationType = "hdfs";
        $scope.feed.clusters.push(
            entityFactory.newCluster('source', $scope.feed.targetClusterLocationType, "", null))
      } else if (value === 'export') {
          $scope.feed.sourceClusterLocationType = "hdfs";
        $scope.feed.clusters.push(
          entityFactory.newCluster('source', $scope.feed.sourceClusterLocationType, "", null));
      }
    };

    $scope.addClusterStorage = function(clusterDetails){
      var cluster = entityFactory.newCluster(clusterDetails.type, clusterDetails.dataTransferType, "", null);
      if($scope.feed.clusters.length > 1
        && findClusterExists($scope.feed.clusters,cluster.name,cluster.type)){
        if(cluster.type === 'source'){
          $scope.sourceClusterExists = true;
        }else{
          $scope.targetClusterExists = true;
        }
        return;
      }else{
          $scope.sourceClusterExists = false;
          $scope.targetClusterExists = false;
      }
      if(cluster.storage.fileSystem) {
        cluster.storage.fileSystem.locations = [];
        cluster.storage.fileSystem.locations.push({'type':'data','path':''});
        cluster.storage.fileSystem.locations.push({'type':'stats','path':'/'});
      }
      $scope.feed.clusters.unshift(cluster);
    };

    $scope.toggleClusterStorage = function(type){
      if(type === 'source'){
        $scope.showSourceClusterStorage = !$scope.showSourceClusterStorage;
      }else if(type === 'target'){
        $scope.showTargetClusterStorage = !$scope.showTargetClusterStorage;
      }
    };

    $scope.$watch("feed.import.source.includesCSV",function(){
      if($scope.feed.dataTransferType === 'import' && $scope.feed.import.source.includesCSV){
        $scope.feed.import.source.fields = { 'includes' : $scope.feed.import.source.includesCSV.split(",") };
      }
    }, true);
    $scope.$watch("feed.import.source.excludesCSV",function(){
      if($scope.feed.dataTransferType === 'import' && $scope.feed.import.source.excludesCSV){
        $scope.feed.import.source.fields = { 'excludes' : $scope.feed.import.source.excludesCSV.split(",") };
      }
    }, true);
    $scope.$watch("feed.export.target.includesCSV",function(){
      if($scope.feed.dataTransferType === 'export' && $scope.feed.export.target.includesCSV){
        $scope.feed.export.target.fields = { 'includes' : $scope.feed.export.target.includesCSV.split(",") };
      }
    }, true);
    $scope.$watch("feed.export.target.excludesCSV",function(){
      if($scope.feed.dataTransferType === 'export' && $scope.feed.export.target.excludesCSV){
        $scope.feed.export.target.fields = { 'excludes' : $scope.feed.export.target.excludesCSV.split(",") };
      }
    }, true);

    $scope.$watch("feed.sourceClusterLocationType", function(){
      var sourceClusters = $scope.feed.clusters.filter(function(obj) {
        return obj.type == 'source';
      });
      if(sourceClusters.length < 1){
        var cluster = entityFactory.newCluster('source', $scope.feed.sourceClusterLocationType, "", null);
        $scope.feed.clusters.push(cluster)
      }
    });

    $scope.$watch("feed.targetClusterLocationType", function(){
      var targetClusters = $scope.feed.clusters.filter(function(obj) {
        return obj.type == 'target';
      });
      if(targetClusters.length < 1 && $scope.feed.dataTransferType !== 'import'){
        var cluster = entityFactory.newCluster('target', $scope.feed.targetClusterLocationType, "", null);
        $scope.feed.clusters.push(cluster)
      }
    });
  }]);
}());
