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

  var module = angular.module('app.directives.feed-cluster-partitions', []);

  module.directive('feedFormClusterPartitions', function () {
    return {
      replace: false,
      restrict: 'EA',
      templateUrl: 'html/feed/feedFormClusterPartitionsTpl.html',
      link: function ($scope, $element) {
        function addFeedPartitionsIfNotExists(partitions, existingPartitions) {
          var runningPartition;
          for (var i=0;i<partitions.length;i++) {
            runningPartition = partitions[i];
            if (runningPartition.trim() != "") {
              var matchedPartitions = existingPartitions.filter(function(element) {
                return element.partitionName === runningPartition;
              });
              if (matchedPartitions.length < 1) {
                existingPartitions.splice(i, 0, {"partitionName" : runningPartition,
                  "partitionText" : "",
                  "mapping" : "any"});
              }
            }
          }
        }

        function removeClusterPartitionsIfNotExists(existingPartitions, partitions) {
          var runningPartition;
          var updatedExistingParititons = [];
          for (var i=0;i<existingPartitions.length;i++) {
            runningPartition = existingPartitions[i];
            var matchedPartitions = partitions.filter(function(element) {
              return element === runningPartition.partitionName;
            });
            if (matchedPartitions.length > 0) {
              updatedExistingParititons.push(runningPartition);
            }
          }
          return updatedExistingParititons;
        }

        function createDefaultClusterPartitions() {
          var currentPartitions = ($scope.feedPartitions.trim() === "") ? [] : $scope.feedPartitions.split(",");
          addFeedPartitionsIfNotExists(currentPartitions, $scope.partitionList);
          $scope.partitionList = removeClusterPartitionsIfNotExists($scope.partitionList, currentPartitions);
        }

        function createSpecificClusterPartitions() {
          var partitionExpressions = $scope.cluster.partition.split("/");
          var currentPartitions = ($scope.feedPartitions.trim() === "") ? [] : $scope.feedPartitions.split(",");
          partitionExpressions.forEach(function(partitionExpression, index) {
            $scope.partitionList.push({"partitionName" : currentPartitions[index],
              "partitionText" : (partitionExpression == "*") ? "" : partitionExpression,
              "mapping" : (partitionExpression == "*") ? "any" : "mappedPartition"
            });
          });
        }

        $scope.preparePartitionExpression = function(partitionList) {
          var partitionExpression = "";
          for(var i=0;i<partitionList.length;i++) {
            var partition = partitionList[i];
            if (partition.mapping === 'any') {
              partitionExpression = partitionExpression.concat("*");
            } else if (partition.mapping === 'mappedPartition') {
              partitionExpression = partitionExpression.concat(partition.partitionText);
            }
            if (i < partitionList.length-1) {
              partitionExpression = partitionExpression.concat("/");
            }
          }
          $scope.cluster.partition = partitionExpression;
        }

        $scope.partitionList = [];
        if ($scope.cluster.partition != undefined) {
          $scope.selectPartition = true;
          createSpecificClusterPartitions()
        } else {
          $scope.selectPartition = false;
          createDefaultClusterPartitions();
        }

        $scope.$watch('selectPartition', function() {
          if ($scope.selectPartition) {
            $scope.preparePartitionExpression($scope.partitionList);
          } else {
            delete $scope.cluster.partition;
          }
        });

        $scope.$on('feed:createClusterPartitions', function() {
          createDefaultClusterPartitions();
          $scope.preparePartitionExpression($scope.partitionList);
        });
      }
    };
  });

})();
