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

  var datasetModule = angular.module('app.controllers.dataset', [ 'app.services' ]);

  datasetModule.controller('DatasetCtrl', [
    "$scope", "$interval", "Falcon", "EntityModel", "$state", "X2jsService", "DateHelper",
    "ValidationService", "SpinnersFlag", "$timeout", "$rootScope", "clustersList", "$cookieStore",
    function ($scope, $interval, Falcon, EntityModel, $state, X2jsService, DateHelper,
              validationService, SpinnersFlag, $timeout, $rootScope, clustersList, $cookieStore) {


      $scope.skipUndo = false;
      $scope.$on('$destroy', function () {

        if (!$scope.skipUndo && !angular.equals($scope.UIModel, EntityModel.defaultValues.MirrorUIModel)) {
          $scope.$parent.cancel('dataset', $rootScope.previousState);
        }
      });

      $scope.isActive = function (route) {
        return route === $state.current.name;
      };

      $scope.isCompleted = function (route) {
        return $state.get(route).data && $state.get(route).data.completed;
      };

      $scope.clone = $scope.$parent.cloningMode;

      if (!clustersList) {
        $scope.clustersList = [];
      } else if (clustersList.type) { // is an object
        $scope.clustersList = [clustersList];
      } else {
        $scope.clustersList = clustersList;
      }

      $scope.switchModel = function (type) {
        $scope.model = EntityModel.datasetModel[type].process;
        $scope.UIModel.formType = type;
        $scope.completeModel = EntityModel.datasetModel[type];
        switchTag(type);
        checkClusters();
      };
      $scope.model = EntityModel.datasetModel.HDFS.process;
      $scope.UIModel = EntityModel.datasetModel.UIModel;
      $scope.completeModel = EntityModel.datasetModel.HDFS;

      $scope.UIModel.acl.owner = $cookieStore.get('userToken').user;

      //-------------------------//
      function checkClusters() {
        if ($scope.UIModel.source.cluster && $scope.UIModel.formType === 'HIVE') {
          $scope.getSourceDefinition();
        }
        if ($scope.UIModel.target.cluster && $scope.UIModel.formType === 'HIVE') {
          $scope.getTargetDefinition();
        }
      }
      $scope.checkFromSource = function () {
        if ($scope.UIModel.source.location !== "HDFS") {
          $scope.UIModel.target.location = "HDFS";
          $scope.UIModel.runOn = 'target';
        }
      };
      $scope.checkFromTarget = function () {
        if ($scope.UIModel.target.location !== "HDFS") {
          $scope.UIModel.source.location = "HDFS";
          $scope.UIModel.runOn = 'source';
        }
      };
      //----------------TAGS---------------------//
      $scope.addTag = function () {
        if ($scope.UIModel.tags.newTag.value === "_falcon_mirroring_type") {
          return;
        }
        $scope.UIModel.tags.tagsArray.push($scope.UIModel.tags.newTag);
        $scope.UIModel.tags.newTag = {value: "", key: ""};
        $scope.convertTags();
      };
      $scope.removeTag = function (index) {
        $scope.UIModel.tags.tagsArray.splice(index, 1);
        $scope.convertTags();
      };
      function switchTag (type) {
        $scope.UIModel.tags.tagsArray.forEach(function (item) {
          if (item.key === "_falcon_mirroring_type") {
            item.value = type;
          }
        });
      }
      $scope.convertTags = function () {
        var result = [];
        $scope.UIModel.tags.tagsArray.forEach(function (element) {
          if (element.key && element.value) {
            result.push(element.key + "=" + element.value);
          }
        });
        result = result.join(",");
        $scope.UIModel.tags.tagsString = result;
      };
      $scope.splitTags = function () {
        $scope.UIModel.tags.tagsArray = [];
        $scope.UIModel.tags.tagsString.split(",").forEach(function (fieldToSplit) {
          var splittedString = fieldToSplit.split("=");
          $scope.UIModel.tags.tagsArray.push({key: splittedString[0], value: splittedString[1]});
        });
      };
      //----------- Alerts -----------//
      $scope.addAlert = function () {
        $scope.UIModel.alerts.alertsArray.push($scope.UIModel.alerts.alert.email);
        $scope.UIModel.alerts.alert = {email: ""};
      };
      $scope.removeAlert = function (index) {
        $scope.UIModel.alerts.alertsArray.splice(index, 1);
      };
      //----------------- DATE INPUTS -------------------//
      $scope.dateFormat = 'MM/dd/yyyy';

      $scope.openStartDatePicker = function ($event) {
        $event.preventDefault();
        $event.stopPropagation();
        $scope.startOpened = true;
      };
      $scope.openEndDatePicker = function ($event) {
        $event.preventDefault();
        $event.stopPropagation();
        $scope.endOpened = true;
      };

      $scope.constructDate = function () {

        if ($scope.UIModel.validity.start && $scope.UIModel.validity.end && $scope.UIModel.validity.startTime && $scope.UIModel.validity.endTime) {
          $scope.UIModel.validity.startISO = DateHelper.createISO($scope.UIModel.validity.start, $scope.UIModel.validity.startTime, $scope.UIModel.validity.tz);
          $scope.UIModel.validity.endISO = DateHelper.createISO($scope.UIModel.validity.end, $scope.UIModel.validity.endTime, $scope.UIModel.validity.tz);
        }

      };
      $scope.$watch(function () {
        return $scope.UIModel.validity.tz;
      }, function () {
        return $scope.constructDate();
      });

      //-------------------------------------//

      $scope.goNext = function (formInvalid, stateName) {
        $state.current.data = $state.current.data || {};
        $state.current.data.completed = !formInvalid;

        SpinnersFlag.show = true;
        if (!validationService.nameAvailable || formInvalid) {
          validationService.displayValidations.show = true;
          validationService.displayValidations.nameShow = true;
          SpinnersFlag.show = false;
          angular.element('body, html').animate({scrollTop: 0}, 500);
          return;
        }
        validationService.displayValidations.show = false;
        validationService.displayValidations.nameShow = false;
        $scope.convertTags();
        createXML();
        $state.go(stateName);
        angular.element('body, html').animate({scrollTop: 0}, 500);
      };

      $scope.goBack = function (stateName) {
        SpinnersFlag.backShow = true;
        validationService.displayValidations.show = false;
        validationService.displayValidations.nameShow = false;
        $state.go(stateName);
        angular.element('body, html').animate({scrollTop: 0}, 500);
      };

      $scope.sourceClusterModel = {};
      $scope.targetClusterModel = {};

      $scope.getSourceDefinition = function () { // only fills general step info, rest of operations performed in createXml
        Falcon.getEntityDefinition("cluster", $scope.UIModel.source.cluster)
          .success(function (data) {
            $scope.sourceClusterModel = X2jsService.xml_str2json(data);
            if (!EntityModel.datasetModel.UIModel.hiveOptions.source.stagingPath && EntityModel.datasetModel.UIModel.formType === 'HIVE') {
              EntityModel.datasetModel.UIModel.hiveOptions.source.stagingPath = findLocation($scope.sourceClusterModel.cluster.locations.location, 'staging');
            }
            if (!EntityModel.datasetModel.UIModel.hiveOptions.source.hiveServerToEndpoint && EntityModel.datasetModel.UIModel.formType === 'HIVE') {
              EntityModel.datasetModel.UIModel.hiveOptions.source.hiveServerToEndpoint = replaceHive(findInterface($scope.sourceClusterModel.cluster.interfaces.interface, 'registry'));
            }

          })
          .error(function (err) {
            $scope.UIModel.source.cluster = "";
            Falcon.logResponse('error', err, false, true);
          });
      };
      $scope.getTargetDefinition = function () {
        Falcon.getEntityDefinition("cluster", $scope.UIModel.target.cluster)
          .success(function (data) {
            $scope.targetClusterModel = X2jsService.xml_str2json(data);
            if (!EntityModel.datasetModel.UIModel.hiveOptions.target.stagingPath && EntityModel.datasetModel.UIModel.formType === 'HIVE') {
              EntityModel.datasetModel.UIModel.hiveOptions.target.stagingPath = findLocation($scope.targetClusterModel.cluster.locations.location, 'staging');
            }
            if (!EntityModel.datasetModel.UIModel.hiveOptions.target.hiveServerToEndpoint && EntityModel.datasetModel.UIModel.formType === 'HIVE') {
              EntityModel.datasetModel.UIModel.hiveOptions.target.hiveServerToEndpoint = replaceHive(findInterface($scope.targetClusterModel.cluster.interfaces.interface, 'registry'));
            }
          })
          .error(function (err) {
            $scope.UIModel.target.cluster = "";
            Falcon.logResponse('error', err, false, true);
          });
      };

      function findLocation (array, locationString) {
        var loc = "";
        array.forEach(function (item) {
          if (item._name === locationString) {
            loc = item._path;
          }
        });
        return loc;
      }
      function findInterface(array, interfaceString) {
        var inter = "";
        array.forEach(function (item) {
          if (item._type === interfaceString) {
            inter = item._endpoint;
          }
        });
        return inter;
      }

      function replaceHive(string) {
        if (string) {
          var splitted = string.split(':');
          var uri = 'hive2' + ':' + splitted[1] + ':10000';
          return uri;
        }
      }

      function createXML() {
        $scope.model._name = $scope.UIModel.name;
        $scope.model.tags = $scope.UIModel.tags.tagsString;
        $scope.model.retry._policy = $scope.UIModel.retry.policy;
        $scope.model.retry._delay = $scope.UIModel.retry.delay.unit + '(' + $scope.UIModel.retry.delay.number + ')';
        $scope.model.retry._attempts = $scope.UIModel.retry.attempts;
        $scope.model.ACL._owner = $scope.UIModel.acl.owner;
        $scope.model.ACL._group = $scope.UIModel.acl.group;
        $scope.model.ACL._permission = $scope.UIModel.acl.permissions;
        $scope.model.frequency = $scope.UIModel.frequency.unit + '(' + $scope.UIModel.frequency.number + ')';
        $scope.model.clusters.cluster[0].validity._start = $scope.UIModel.validity.startISO;
        $scope.model.clusters.cluster[0].validity._end = $scope.UIModel.validity.endISO;
        $scope.model.timezone = $scope.UIModel.validity.tz;
        if ($scope.UIModel.formType === 'HDFS') {

          if ($scope.UIModel.runOn === "source") {
            $scope.model.clusters.cluster[0]._name = $scope.UIModel.source.cluster;
          } else {
            $scope.model.clusters.cluster[0]._name = $scope.UIModel.target.cluster;
          }

          $scope.model.workflow._name = $scope.UIModel.name + '-WF';

          $scope.model.properties.property.forEach(function (item) {
            if (item._name === 'distcpMaxMaps') {
              item._value = $scope.UIModel.allocation.hdfs.maxMaps;
            }
            if (item._name === 'distcpMapBandwidth') {
              item._value = $scope.UIModel.allocation.hdfs.maxBandwidth;
            }
            if (item._name === 'drSourceDir') {
              item._value = $scope.UIModel.source.path;
            }
            if (item._name === 'drTargetDir') {
              item._value = $scope.UIModel.target.path;
            }
            if (item._name === 'drSourceClusterFS') {
              if ($scope.UIModel.source.location === 'HDFS') {
                item._value = findInterface($scope.sourceClusterModel.cluster.interfaces.interface, 'write');
              } else {
                item._value = $scope.UIModel.source.url;
              }
            }
            if (item._name === 'drTargetClusterFS') {
              if ($scope.UIModel.target.location === 'HDFS') {
                item._value = findInterface($scope.targetClusterModel.cluster.interfaces.interface, 'write');
              } else {
                item._value = $scope.UIModel.target.url;
              }
            }
            if (item._name === 'drNotificationReceivers') {
              item._value = (function () {
                if ($scope.UIModel.alerts.alertsArray.length === 0) {
                  return "NA";
                } else {
                  return $scope.UIModel.alerts.alertsArray.join();
                }
              }());
            }
            if (item._name === 'sourceCluster') {
              if ($scope.UIModel.source.location === 'HDFS') { item._value = $scope.UIModel.source.cluster; }
              else { item._value = ""; }
            }
            if (item._name === 'targetCluster') {
              if ($scope.UIModel.target.location === 'HDFS') { item._value = $scope.UIModel.target.cluster; }
              else { item._value = ""; }
            }
          });

        } else if ($scope.UIModel.formType === 'HIVE') {

          $scope.model.clusters.cluster[0]._name = $scope.UIModel.source.cluster;
          $scope.model.properties.property.forEach(function (item) {
            if (item._name === 'distcpMaxMaps') {
              item._value = $scope.UIModel.allocation.hive.maxMapsDistcp;
            }
            if (item._name === 'distcpMapBandwidth') {
              item._value = $scope.UIModel.allocation.hive.maxBandwidth;
            }
            if (item._name === 'sourceCluster') {
              item._value = $scope.UIModel.source.cluster;
            }
            if (item._name === 'targetCluster') {
              item._value = $scope.UIModel.target.cluster;
            }
            if (item._name === 'sourceHiveServer2Uri') {
              item._value = $scope.UIModel.hiveOptions.source.hiveServerToEndpoint;
            }
            if (item._name === 'targetHiveServer2Uri') {
              item._value = $scope.UIModel.hiveOptions.target.hiveServerToEndpoint;
            }
            if (item._name === 'sourceStagingPath') {
              item._value = $scope.UIModel.hiveOptions.source.stagingPath;
            }
            if (item._name === 'targetStagingPath') {
              item._value = $scope.UIModel.hiveOptions.target.stagingPath;
            }
            if (item._name === 'sourceNN') {
              item._value = findInterface($scope.sourceClusterModel.cluster.interfaces.interface, 'write');
            }
            if (item._name === 'targetNN') {
              item._value = findInterface($scope.targetClusterModel.cluster.interfaces.interface, 'write');
            }
            if (item._name === 'sourceMetastoreUri') {
              item._value = findInterface($scope.sourceClusterModel.cluster.interfaces.interface, 'registry');
            }
            if (item._name === 'targetMetastoreUri') {
              item._value = findInterface($scope.targetClusterModel.cluster.interfaces.interface, 'registry');
            }
            if (item._name === 'sourceTable') {
              if ($scope.UIModel.source.hiveDatabaseType === "databases") {
                item._value = "*";
              } else {
                item._value = $scope.UIModel.source.hiveTables;
              }
            }
            if (item._name === 'sourceDatabase') {
              if ($scope.UIModel.source.hiveDatabaseType === "databases") {
                item._value = $scope.UIModel.source.hiveDatabases;
              } else {
                item._value = $scope.UIModel.source.hiveDatabase;
              }
            }
            if (item._name === 'maxEvents') {
              item._value = $scope.UIModel.allocation.hive.maxMapsEvents;
            }
            if (item._name === 'replicationMaxMaps') {
              item._value = $scope.UIModel.allocation.hive.maxMapsMirror;
            }
            if (item._name === 'clusterForJobRun') {
              if ($scope.UIModel.runOn === "source") {
                item._value = $scope.UIModel.source.cluster;
              } else {
                item._value = $scope.UIModel.target.cluster;
              }
            }
            if (item._name === 'clusterForJobRunWriteEP') {
              if ($scope.UIModel.runOn === "source") {
                item._value = findInterface($scope.sourceClusterModel.cluster.interfaces.interface, 'write');
              } else {
                item._value = findInterface($scope.targetClusterModel.cluster.interfaces.interface, 'write');
              }
            }
            if (item._name === 'drJobName') {
              item._value = $scope.UIModel.name;
            }
            if (item._name === 'drNotificationReceivers') {
              item._value = (function () {
                if ($scope.UIModel.alerts.alertsArray.length === 0) {
                  return "NA";
                } else {
                  return $scope.UIModel.alerts.alertsArray.join();
                }
              }());
            }

          });

        } else {
          console.log('error in form type');
        }

        $scope.xmlString = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' + X2jsService.json2xml_str($scope.completeModel);

      }

      $scope.save = function () {
        SpinnersFlag.show = true;

        if(!$scope.$parent.cloningMode) {
          Falcon.postUpdateEntity($scope.xmlString, 'process', $scope.model._name)
            .success(function (response) {
              $scope.skipUndo = true;
              Falcon.logResponse('success', response, false);
              $state.go('main');

            })
            .error(function (err) {
              SpinnersFlag.show = false;
              Falcon.logResponse('error', err, false);
              angular.element('body, html').animate({scrollTop: 0}, 300);
            });
        } else {
          Falcon.postSubmitEntity($scope.xmlString, 'process')
            .success(function (response) {
              $scope.skipUndo = true;
              Falcon.logResponse('success', response, false);
              $state.go('main');
            })
            .error(function (err) {
              Falcon.logResponse('error', err, false);
              SpinnersFlag.show = false;
              angular.element('body, html').animate({scrollTop: 0}, 300);
            });
        }

      };

      function identifyLocationType (val) {
        if (validationService.patterns.s3.test(val)) {
          return "s3";
        } else if (validationService.patterns.azure.test(val)) {
          return "azure";
        } else {
          return "HDFS";
        }
      }

      function importModel(model) {

        var mirrorType;

        if (model.process.tags.search('_falcon_mirroring_type=HDFS') !== -1) {
          mirrorType = 'HDFS';
        } else {
          mirrorType = 'HIVE';
        }
        $scope.switchModel(mirrorType);
        EntityModel.datasetModel.UIModel.formType = mirrorType;
        EntityModel.datasetModel.UIModel.name = (function () { if (!$scope.clone) { return model.process._name; } else { return ""; } }());
        EntityModel.datasetModel.UIModel.retry.policy = model.process.retry._policy;
        EntityModel.datasetModel.UIModel.retry.attempts = model.process.retry._attempts;
        EntityModel.datasetModel.UIModel.retry.delay.number = (function () {
          return parseInt(model.process.retry._delay.split('(')[1]);
        }());
        EntityModel.datasetModel.UIModel.retry.delay.unit = (function () {
          return model.process.retry._delay.split('(')[0];
        }());
        EntityModel.datasetModel.UIModel.frequency.number = (function () {
          return parseInt(model.process.frequency.split('(')[1]);
        }());
        EntityModel.datasetModel.UIModel.frequency.unit = (function () {
          return model.process.frequency.split('(')[0];
        }());
        EntityModel.datasetModel.UIModel.acl.owner = model.process.ACL._owner;
        EntityModel.datasetModel.UIModel.acl.group = model.process.ACL._group;
        EntityModel.datasetModel.UIModel.acl.permissions = model.process.ACL._permission;

        EntityModel.datasetModel.UIModel.validity.startISO = model.process.clusters.cluster[0].validity._start;
        EntityModel.datasetModel.UIModel.validity.endISO = model.process.clusters.cluster[0].validity._end;
        EntityModel.datasetModel.UIModel.validity.tz = model.process.timezone;
        EntityModel.datasetModel.UIModel.validity.start = DateHelper.importDate (model.process.clusters.cluster[0].validity._start, model.process.timezone);
        EntityModel.datasetModel.UIModel.validity.startTime = DateHelper.importDate (model.process.clusters.cluster[0].validity._start, model.process.timezone);
        EntityModel.datasetModel.UIModel.validity.end = DateHelper.importDate (model.process.clusters.cluster[0].validity._end, model.process.timezone);
        EntityModel.datasetModel.UIModel.validity.endTime = DateHelper.importDate (model.process.clusters.cluster[0].validity._end, model.process.timezone);

        EntityModel.datasetModel.UIModel.tags.tagsString = model.process.tags;
        EntityModel.datasetModel.UIModel.tags.tagsArray = (function () {
          var array = [];
          model.process.tags.split(',').forEach(function (fieldToSplit) {
            var splittedString = fieldToSplit.split("=");
            array.push({key: splittedString[0], value: splittedString[1]});
          });
          return array;
        }());

        if (mirrorType === 'HDFS') {
          model.process.properties.property.forEach(function (item) {
            if (item._name === 'distcpMaxMaps') {
              EntityModel.datasetModel.UIModel.allocation.hdfs.maxMaps = item._value;
            }
            if (item._name === 'distcpMapBandwidth') {
              EntityModel.datasetModel.UIModel.allocation.hdfs.maxBandwidth = item._value;
            }
            if (item._name === 'drSourceDir') {
              EntityModel.datasetModel.UIModel.source.path = item._value;
            }
            if (item._name === 'drTargetDir') {
              EntityModel.datasetModel.UIModel.target.path = item._value;
            }
            if (item._name === 'drNotificationReceivers') {
              EntityModel.datasetModel.UIModel.alerts.alertsArray = (function () {
                if (item._value !== "NA") {
                  return item._value.split(',');
                } else {
                  return [];
                }
              }());
            }
            if (item._name === 'targetCluster') {
              EntityModel.datasetModel.UIModel.target.cluster = item._value;
            }
            if (item._name === 'sourceCluster') {
              EntityModel.datasetModel.UIModel.source.cluster = item._value;
            }
            if (item._name === 'drSourceClusterFS') {
              EntityModel.datasetModel.UIModel.source.url = item._value;
            }
            if (item._name === 'drTargetClusterFS') {
              EntityModel.datasetModel.UIModel.target.url = item._value;
            }
          });

          if (EntityModel.datasetModel.UIModel.source.cluster === model.process.clusters.cluster[0]._name) {
            EntityModel.datasetModel.UIModel.runOn = "source";
          }
          if (EntityModel.datasetModel.UIModel.target.cluster === model.process.clusters.cluster[0]._name) {
            EntityModel.datasetModel.UIModel.runOn = "target";
          }

          EntityModel.datasetModel.UIModel.source.location = identifyLocationType(EntityModel.datasetModel.UIModel.source.url);
          EntityModel.datasetModel.UIModel.target.location = identifyLocationType(EntityModel.datasetModel.UIModel.target.url);

        } else if (mirrorType === 'HIVE') {

          model.process.properties.property.forEach(function (item) {
            if (item._name === 'distcpMaxMaps') {
              EntityModel.datasetModel.UIModel.allocation.hive.maxMapsDistcp = item._value;
            }
            if (item._name === 'distcpMapBandwidth') {
              EntityModel.datasetModel.UIModel.allocation.hive.maxBandwidth = item._value;
            }
            if (item._name === 'sourceCluster') {
              EntityModel.datasetModel.UIModel.source.cluster = item._value;
            }
            if (item._name === 'targetCluster') {
              EntityModel.datasetModel.UIModel.target.cluster = item._value;
            }
            if (item._name === 'sourceStagingPath') {
              EntityModel.datasetModel.UIModel.hiveOptions.source.stagingPath = item._value;
            }
            if (item._name === 'targetStagingPath') {
              EntityModel.datasetModel.UIModel.hiveOptions.target.stagingPath = item._value;
              if (item._value === "*") {
                EntityModel.datasetModel.UIModel.source.hiveDatabaseType = "databases";
              } else {
                EntityModel.datasetModel.UIModel.source.hiveDatabaseType = "tables";
              }
            }
            if (item._name === 'sourceHiveServer2Uri') {
              EntityModel.datasetModel.UIModel.hiveOptions.source.hiveServerToEndpoint = item._value;
            }
            if (item._name === 'targetHiveServer2Uri') {
              EntityModel.datasetModel.UIModel.hiveOptions.target.hiveServerToEndpoint = item._value;
            }
            if (item._name === 'sourceTable') {
              EntityModel.datasetModel.UIModel.source.hiveTables = item._value;
              if (EntityModel.datasetModel.UIModel.source.hiveDatabaseType === "databases") {
                EntityModel.datasetModel.UIModel.source.hiveTables = "*";
              }
              else {
                EntityModel.datasetModel.UIModel.source.hiveTables = item._value;
              }
            }
            if (item._name === 'sourceDatabase') {
              if (EntityModel.datasetModel.UIModel.source.hiveDatabaseType === "databases") {
                EntityModel.datasetModel.UIModel.source.hiveDatabases = item._value;
              } else {
                EntityModel.datasetModel.UIModel.source.hiveDatabase = item._value;
              }
            }
            if (item._name === 'maxEvents') {
              EntityModel.datasetModel.UIModel.allocation.hive.maxMapsEvents = item._value;
            }
            if (item._name === 'replicationMaxMaps') {
              EntityModel.datasetModel.UIModel.allocation.hive.maxMapsMirror = item._value;
            }
            if (item._name === 'clusterForJobRun') {
              if (EntityModel.datasetModel.UIModel.source.cluster === item._value) {
                EntityModel.datasetModel.UIModel.runOn = "source";
              } else {
                EntityModel.datasetModel.UIModel.runOn = "target";
              }
            }
            if (item._name === 'drNotificationReceivers') {
              EntityModel.datasetModel.UIModel.alerts.alertsArray = (function () {
                if (item._value !== "NA") {
                  return item._value.split(',');
                } else {
                  return [];
                }
              }());
            }

          });
        }

        if(EntityModel.datasetModel.UIModel.source.cluster) { $scope.getSourceDefinition(); }
        if(EntityModel.datasetModel.UIModel.target.cluster) { $scope.getTargetDefinition(); }

      }
      if (EntityModel.datasetModel.toImportModel) {
        importModel(EntityModel.datasetModel.toImportModel);
      }
    }]);
}());
