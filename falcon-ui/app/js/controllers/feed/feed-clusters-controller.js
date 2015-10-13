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

  feedModule.controller('FeedClustersController', [ "$scope", "clustersList", "EntityFactory", "$timeout",
                                            function($scope, clustersList, entityFactory, $timeout) {

      function focusOnElement () {
        $timeout(function () {
          angular.element('#clusterNameSelect').trigger('focus');
        }, 500);
      }
      focusOnElement();

      unwrapClusters(clustersList);

      $scope.updateRetention = function() {
        if($scope.selectedCluster.retention.action === 'archive' && $scope.selectedCluster.type === 'source') {
          $scope.allClusters.length = 0;
          $scope.allClusters.concat($scope.feed.clusters);

          $scope.feed.clusters.length = 0;
          $scope.feed.clusters.push($scope.sourceCluster);
          $scope.feed.clusters.push($scope.archiveCluster);

          $scope.sourceCluster.selected = false;
          $scope.archiveCluster.selected = true;
          $scope.archiveCluster.active = true;
          $scope.selectedCluster = $scope.archiveCluster;
        }

        if($scope.selectedCluster.retention.action !== 'archive'&& $scope.selectedCluster.type === 'source' && $scope.archiveCluster.active) {
          $scope.archiveCluster.selected = false;
          $scope.feed.clusters.length = 0;
          $scope.allClusters.length = 0;
          $scope.feed.clusters.push($scope.sourceCluster);
          $scope.sourceCluster.selected = true;
          $scope.archiveCluster.active = false;
        }
      };

      $scope.addCluster = function() {
        $scope.selectedCluster.selected = false;
        var cluster = $scope.newCluster(true);
        $scope.feed.clusters.push(cluster);
        $scope.selectedCluster = cluster;
      };

      $scope.newCluster = function(selected) {
        return entityFactory.newCluster('target', selected);
      };

      $scope.handleCluster = function(cluster, index) {
        if(cluster.selected) {
          $scope.removeCluster(index);
        } else {
          $scope.selectCluster(cluster);
        }
      };

      $scope.selectCluster = function (cluster) {
        $scope.selectedCluster.selected = false;
        cluster.selected = true;
        $scope.selectedCluster = cluster;
      };

      $scope.removeCluster = function(index) {
        if(index >= 0 && $scope.feed.clusters.length > 1 &&
          $scope.feed.clusters[index].type !== 'source' &&
          !$scope.archiveCluster.active) {
          $scope.feed.clusters.splice(index, 1);
          $scope.selectCluster($scope.sourceCluster);
        }
      };

      function unwrapClusters(clusters) {
	if(clusters !== undefined && clusters !== null && clusters !== "null"){
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
      }


      $scope.clusterLocationsPlaceHolders = (function () {
        var obj = {};
        $scope.feed.storage.fileSystem.locations.forEach(function (item) {
          obj[item.type] = item.path;
        });
        return obj;
      }());



      $scope.selectedCluster = $scope.selectedCluster || $scope.feed.clusters[0];
      $scope.sourceCluster = $scope.sourceCluster || $scope.feed.clusters[0];
      $scope.archiveCluster = $scope.newCluster(false);
      $scope.archiveCluster.active = false;
      $scope.allClusters = [];


    }]);

})();
