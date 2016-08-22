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
    "$scope", "$interval", "Falcon", "EntityModel", "$state", "X2jsService", "DateHelper", "RouteHelper",
    "ValidationService", "SpinnersFlag", "$timeout", "$rootScope", "clustersList", "$cookieStore", "DatasetModel",
    "ExtensionSerializer",
    function ($scope, $interval, Falcon, EntityModel, $state, X2jsService, DateHelper, RouteHelper,
              validationService, SpinnersFlag, $timeout, $rootScope, clustersList, $cookieStore, datasetModel,
              extensionSerializer) {

      var stateMatrix = {
        general : {previous : '', next : 'summary'},
        summary : {previous : 'general', next : ''}
      };

      $scope.skipUndo = false;
      $scope.secureMode = $rootScope.secureMode;
      $scope.clusterErrorMessage = '';
      $scope.$on('$destroy', function () {

        if (!$scope.skipUndo && !angular.equals($scope.UIModel, EntityModel.defaultValues.MirrorUIModel)) {
          if($scope.clone){
            EntityModel.datasetModel.UIModel.clone = true;
          }
          if($scope.editingMode){
            EntityModel.datasetModel.UIModel.edit = true;
          }
          $scope.$parent.cancel('dataset', $rootScope.previousState);
        }
      });

      $scope.isFrequencyValid = true;
      $scope.checkMininumFrequency = function(quantity, unit, field) {
        $scope.isFrequencyValid = quantity ? true : false;
        if (quantity && unit === 'minutes') {
          $scope.isFrequencyValid = validationService.checkMininum(quantity);
        } else if (unit !== 'minutes' && quantity && parseInt(quantity) === 0) {
          $scope.isFrequencyValid = false;
        }
        if (field) {
          field.$setValidity('required', $scope.isFrequencyValid);
        }
      };

      $scope.isActive = function (route) {
        return route === $state.current.name;
      };

      $scope.isCompleted = function (route) {
        return $state.get(route).data && $state.get(route).data.completed;
      };

      if (!clustersList) {
        $scope.clustersList = [];
      } else if (clustersList.type) { // is an object
        $scope.clustersList = [clustersList];
      } else {
        $scope.clustersList = clustersList;
      }

      $scope.switchModel = function (type) {
        $scope.model = EntityModel.datasetModel[type].process;
        $scope.UIModel.type = type;
        $scope.completeModel = EntityModel.datasetModel[type];
        checkClusters();
      };
      $scope.model = EntityModel.datasetModel.HDFS.process;
      $scope.UIModel = EntityModel.datasetModel.UIModel;
      $scope.completeModel = EntityModel.datasetModel.HDFS;

      if($scope.UIModel.clone === true || (datasetModel && datasetModel.clone === true)){
        $scope.clone = true;
        $scope.editingMode = false;
      }else if($scope.UIModel.edit === true || (datasetModel && datasetModel.edit === true)){
        $scope.editingMode = true;
        $scope.clone = false;
      }else{
        $scope.editingMode = false;
        $scope.clone = false;
      }

      $scope.UIModel.ACL.owner = $cookieStore.get('userToken').user;

      //-------------------------//
      function checkClusters() {
        if ($scope.UIModel.source.cluster && $scope.UIModel.type === 'HIVE') {
          $scope.getSourceDefinition();
        }
        if ($scope.UIModel.target.cluster && $scope.UIModel.type === 'HIVE') {
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
        $scope.UIModel.tags.push({key: null, value: null});
      };
      $scope.removeTag = function (index) {
        if (index >= 0 && $scope.UIModel.tags.length > 1) {
          $scope.UIModel.tags.splice(index, 1);
        }
      };
      //----------- Alerts -----------//
      $scope.addAlert = function () {
        $scope.UIModel.alerts.push($scope.UIModel.alert.email);
        $scope.UIModel.alert = {email: ""};
      };
      $scope.removeAlert = function (index) {
        $scope.UIModel.alerts.splice(index, 1);
      };

      //----------------- DATE INPUTS -------------------//
      $scope.dateFormat = DateHelper.getLocaleDateFormat();

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

        if ($scope.UIModel.validity.start.date && $scope.UIModel.validity.end.date && $scope.UIModel.validity.start.time && $scope.UIModel.validity.end.time) {
          $scope.UIModel.validity.startISO = DateHelper.createISO($scope.UIModel.validity.start.date, $scope.UIModel.validity.start.time, $scope.UIModel.validity.timezone);
          $scope.UIModel.validity.endISO = DateHelper.createISO($scope.UIModel.validity.end.date, $scope.UIModel.validity.end.time, $scope.UIModel.validity.timezone);
        }

      };
      $scope.$watch(function () {
        return $scope.UIModel.validity.timezone;
      }, function () {
        return $scope.constructDate();
      });

      //-------------------------------------//

      $scope.goNext = function (formInvalid) {
        $state.current.data = $state.current.data || {};
        $state.current.data.completed = !formInvalid;

        SpinnersFlag.show = true;
        if (!validationService.nameAvailable || formInvalid) {
          validationService.displayValidations.show = true;
          validationService.displayValidations.nameShow = true;
          SpinnersFlag.show = false;
          return;
        }

        if ($scope.clusterErrorMessage !== '') {
          SpinnersFlag.show = false;
          return;
        }

        validationService.displayValidations.show = false;
        validationService.displayValidations.nameShow = false;
        $state.go(RouteHelper.getNextState($state.current.name, stateMatrix));
        angular.element('body, html').animate({scrollTop: 0}, 500);
      };

      $scope.goBack = function () {
        SpinnersFlag.backShow = true;
        validationService.displayValidations.show = false;
        validationService.displayValidations.nameShow = false;
        $state.go(RouteHelper.getPreviousState($state.current.name, stateMatrix));
        angular.element('body, html').animate({scrollTop: 0}, 500);
      };

      $scope.sourceClusterModel = {};
      $scope.targetClusterModel = {};

      $scope.getSourceDefinition = function () { // only fills general step info, rest of operations performed in createXml
        Falcon.getEntityDefinition("cluster", $scope.UIModel.source.cluster)
          .success(function (data) {
            $scope.sourceClusterModel = X2jsService.xml_str2json(data);
            if (EntityModel.datasetModel.UIModel.type === 'HIVE') {
              EntityModel.datasetModel.UIModel.hiveOptions.source.stagingPath
                = findLocation($scope.sourceClusterModel.cluster.locations.location, 'staging');
              EntityModel.datasetModel.UIModel.hiveOptions.source.hiveServerToEndpoint
                = replaceHive(findInterface($scope.sourceClusterModel.cluster.interfaces.interface, 'registry'));
            }
          })
          .error(function (err) {
            $scope.UIModel.source.cluster = "";
            Falcon.logResponse('error', err, false, true);
          });

          if ($scope.UIModel.source.cluster === $scope.UIModel.target.cluster) {
            $scope.clusterErrorMessage = 'Target cannot be the same as the Source';
          } else {
            $scope.clusterErrorMessage = '';
            return;
          }
      };
      $scope.getTargetDefinition = function () {
        Falcon.getEntityDefinition("cluster", $scope.UIModel.target.cluster)
          .success(function (data) {
            $scope.targetClusterModel = X2jsService.xml_str2json(data);
            if (EntityModel.datasetModel.UIModel.type === 'HIVE') {
              EntityModel.datasetModel.UIModel.hiveOptions.target.stagingPath
                = findLocation($scope.targetClusterModel.cluster.locations.location, 'staging');
            }
            if (EntityModel.datasetModel.UIModel.type === 'HIVE') {
              EntityModel.datasetModel.UIModel.hiveOptions.target.hiveServerToEndpoint
                = replaceHive(findInterface($scope.targetClusterModel.cluster.interfaces.interface, 'registry'));
            }
          })
          .error(function (err) {
            $scope.UIModel.target.cluster = "";
            Falcon.logResponse('error', err, false, true);
          });

          if ($scope.UIModel.source.cluster === $scope.UIModel.target.cluster) {
            $scope.clusterErrorMessage = 'Target cannot be the same as the Source';
          } else {
            $scope.clusterErrorMessage = '';
            return;
          }
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

      $scope.save = function () {
        SpinnersFlag.show = true;
        var extensionData = extensionSerializer.convertObjectToString(
          extensionSerializer.serializeExtensionProperties($scope.UIModel, $scope.UIModel.type + '-MIRROR'));

        if($scope.editingMode) {
          Falcon.postUpdateExtension(extensionData, $scope.UIModel.type + '-MIRRORING')
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
            Falcon.postSubmitExtension(extensionData, $scope.UIModel.type + '-MIRRORING')
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

      var extensionModel;
      if (datasetModel) {
        extensionModel = datasetModel;
      } else if (EntityModel.datasetModel.toImportModel) {
        extensionModel = EntityModel.datasetModel.toImportModel;
      }
      if (extensionModel && extensionModel.process.tags) {
        var datasetType;
        if (extensionModel.process.tags.indexOf('_falcon_extension_name=HDFS-MIRRORING') !== -1) {
          datasetType = 'hdfs-mirror';
          $scope.UIModel = extensionSerializer.serializeExtensionModel(extensionModel, datasetType, $scope.secureMode);
        } else if (extensionModel.process.tags.indexOf('_falcon_extension_name=HIVE-MIRRORING') !== -1) {
          datasetType = 'hive-mirror';
          $scope.UIModel = extensionSerializer.serializeExtensionModel(extensionModel, datasetType, $scope.secureMode);
        }
      }
      if ($scope.clone) {
        $scope.UIModel.name = "";
      }
      if($state.current.name !== "forms.dataset.general"){
        $state.go("forms.dataset.general");
      }
    }]);
}());
