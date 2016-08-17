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

      unwrapClusters(clustersList);
      $scope.sourceClusterName;
      $scope.sourceClusterNotSelected = false;
      $scope.sourceClusterExists = false;
      $scope.targetClusterName;
      $scope.targetClusterNotSelected = false;
      $scope.targetClusterExists = false;

      function preparePartitionString() {
        $scope.feedPartitions = "";
        $scope.feed.partitions.forEach(function(partition, index){
          $scope.feedPartitions = $scope.feedPartitions.concat(partition.name);
          if (index < $scope.feed.partitions.length - 1) {
            $scope.feedPartitions = $scope.feedPartitions.concat(",");
          }
        });
      }

      $scope.$watch("feed.partitions",function(){
        preparePartitionString();
        $scope.$broadcast("createClusterPartitions");
      }, true);

      $scope.createFeedClusterPartitions = function() {
        if ($scope.feedPartitions != undefined) {
          var currentPartitions = ($scope.feedPartitions.trim() === "") ? [] : $scope.feedPartitions.split(",");
          $scope.feed.partitions = [];
          currentPartitions.forEach(function(element) {
            $scope.feed.partitions.push(entityFactory.newPartition(element));
          });
        }
      };

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

      function openClusterInAccordion(object, flagValue) {
        object.isAccordionOpened = flagValue;
      }

      $scope.feed.clusters.forEach(function(cluster) {
        openClusterInAccordion(cluster, false);
      });

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

      $scope.clearSourceClusterFlags = function() {
        $scope.sourceClusterNotSelected = false;
        $scope.sourceClusterExists = false;
      }

      $scope.clearTargetClusterFlags = function() {
        $scope.targetClusterNotSelected = false;
        $scope.targetClusterExists = false;
      }

      $scope.addSourceCluster = function() {
        $scope.clearSourceClusterFlags();
        if ($scope.sourceClusterName == undefined) {
          $scope.sourceClusterNotSelected = true;
        } else if (!findClusterExists($scope.feed.clusters, $scope.sourceClusterName, 'source')) {
          $scope.addCluster('source', $scope.sourceClusterName);
        } else {
          $scope.sourceClusterExists = true;
        }
      };

      $scope.addTargetCluster = function() {
        if ($scope.targetClusterName == undefined) {
          $scope.targetClusterNotSelected = true;
        } else if (!findClusterExists($scope.feed.clusters, $scope.targetClusterName, 'target')) {
          $scope.addCluster('target', $scope.targetClusterName);
        } else {
          $scope.targetClusterExists = true;
        }
      };

      $scope.addCluster = function(clusterType, clusterName) {
        var cluster = $scope.newCluster(clusterType, true, clusterName);
        if($scope.feed.storage.catalog.active){
          cluster.storage.catalog.catalogTable.uri = $scope.feed.storage.catalog.catalogTable.uri
        }
        cluster.storage.fileSystem.locations.forEach(function (location) {
          if (location.type === 'data') {
            var dataLocation = $scope.feed.storage.fileSystem.locations.filter(function(obj) {
              return obj.type == 'data';
            })[0];
            location.path = (dataLocation != undefined) ? dataLocation.path : '';
          }
        });
        openClusterInAccordion(cluster, true);
        $scope.feed.clusters.push(cluster);
      };

      $scope.newCluster = function(clusterType, selected, clusterName, partition) {
        return entityFactory.newCluster(clusterType, selected, clusterName, partition);
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

      function findClusterIndex(atIndex, ofType) {
        var index = -1;
        for (var i=0,len=$scope.feed.clusters.length;i<len;i++){
          var cluster=$scope.feed.clusters[i];
          if (cluster.type === ofType) {
            index++;
            if (index === atIndex) {
              return i;
            }
          }
        }
        return -1;
      };

      $scope.removeCluster = function(clusterIndex, clusterType) {
        if (clusterIndex >= 0 && $scope.feed.clusters.length > 0
          && !$scope.archiveCluster.active) {
          var clusterTypeIndex = findClusterIndex(clusterIndex, clusterType);
          if (clusterTypeIndex > -1) {
            $scope.feed.clusters.splice(clusterTypeIndex, 1);
          }
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
