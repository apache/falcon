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

  var snapshotModule = angular.module('app.controllers.snapshot', [ 'app.services' ]);

  snapshotModule.controller('SnapshotController', [
    "$scope", "$interval", "$controller", "Falcon", "EntityModel", "$state", "X2jsService", "DateHelper",
    "RouteHelper", "ValidationService", "SpinnersFlag", "$timeout", "$rootScope", "clustersList",
    "$cookieStore", "SnapshotModel", "EntityFactory", "ExtensionSerializer",
    function ($scope, $interval, $controller, Falcon, EntityModel, $state, X2jsService, DateHelper,
       RouteHelper, validationService, SpinnersFlag, $timeout, $rootScope, clustersList,
       $cookieStore, snapshotModel, entityFactory, extensionSerializer) {

      var stateMatrix = {
        general : {previous : '', next : 'summary'},
        summary : {previous : 'general', next : ''}
      };

      $scope.entityType = 'snapshot';
      $scope.skipUndo = false;
      $scope.clusterErrorMessage = '';
      unwrapClusters(clustersList);

      //extending root controller
      $controller('EntityRootCtrl', {
        $scope: $scope
      });

      $scope.$on('$destroy', function() {
        var defaultProcess = entityFactory.newEntity('snapshot'),
          nameIsEqual = ($scope.snapshot.name == null || $scope.snapshot.name === ""),
          ACLIsEqual = angular.equals($scope.snapshot.ACL, defaultProcess.ACL);

        if (!$scope.skipUndo && (!nameIsEqual || !ACLIsEqual)) {
          $scope.$parent.models.snapshotModel = $scope.snapshot;
          if ($scope.cloningMode) {
            $scope.$parent.models.snapshotModel.clone = true;
          }
          if ($scope.editingMode) {
            $scope.$parent.models.snapshotModel.edit = true;
          }
          $scope.$parent.cancel('snapshot', $rootScope.previousState);
        }
      });

       $scope.isActive = function (route) {
        return route === $state.current.name;
      };

      $scope.isCompleted = function (route) {
        return $state.get(route).data && $state.get(route).data.completed;
      };

      $scope.loadOrCreateEntity = function() {
        var type = $scope.entityType;

        if(!snapshotModel && $scope.$parent.models.snapshotModel) {
          var snapshotObj = $scope.$parent.models.snapshotModel;
          $scope.$parent.models.snapshotModel = null;
          return snapshotObj;
        }
        $scope.$parent.models.snapshotModel = null;
        return snapshotModel ? extensionSerializer.serializeExtensionModel(snapshotModel, 'snapshot')
          : entityFactory.newEntity(type);
      };

      $scope.init = function() {
        $scope.baseInit();
        var type = $scope.entityType;
        $scope[type] = $scope.loadOrCreateEntity();
        if(snapshotModel && snapshotModel.clone === true) {
          $scope.cloningMode = true;
          $scope.editingMode = false;
          $scope[type].name = "";
        } else if(snapshotModel && snapshotModel.edit === true) {
          $scope.editingMode = true;
          $scope.cloningMode = false;
        } else{
          $scope.editingMode = false;
          $scope.cloningMode = false;
        }
        console.log($scope.editingMode);
      }

      $scope.init();

      //----------------TAGS---------------------//
      $scope.addTag = function () {
        $scope.snapshot.tags.push({key: null, value: null});
      };

      $scope.removeTag = function (index) {
        if (index >= 0 && $scope.snapshot.tags.length > 1) {
          $scope.snapshot.tags.splice(index, 1);
        }
      };

      //----------- Alerts -----------//
      $scope.addAlert = function () {
        $scope.snapshot.alerts.push($scope.snapshot.alert.email);
        $scope.snapshot.alert = {email: ""};
      };
      $scope.removeAlert = function (index) {
        $scope.snapshot.alerts.splice(index, 1);
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
        if ($scope.snapshot.validity.start && $scope.snapshot.validity.end
          && $scope.snapshot.validity.startTime && $scope.snapshot.validity.endTime) {
          $scope.snapshot.validity.startISO = DateHelper.createISO(
            $scope.snapshot.validity.start, $scope.snapshot.validity.startTime, $scope.snapshot.validity.timezone);
          $scope.snapshot.validity.endISO = DateHelper.createISO(
            $scope.snapshot.validity.end, $scope.snapshot.validity.endTime, $scope.snapshot.validity.timezone);
        }
      };

      $scope.$watch(function () {
        return $scope.snapshot.validity.timezone;
      }, function () {
        return $scope.constructDate();
      });

      $scope.validateCluster = function() {
        if ($scope.snapshot.source.cluster === $scope.snapshot.target.cluster) {
          $scope.clusterErrorMessage = 'Target cannot be the same as the Source';
        } else {
          $scope.clusterErrorMessage = '';
        }
      };

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

      $scope.save = function (formInvalid) {
        SpinnersFlag.saveShow = true;

        $state.current.data = $state.current.data || {};
        $state.current.data.completed = !formInvalid;
        if (!validationService.nameAvailable || formInvalid) {
          validationService.displayValidations.show = true;
          validationService.displayValidations.nameShow = true;
          SpinnersFlag.saveShow = false;
          return;
        }
        if ($scope.clusterErrorMessage !== '') {
          SpinnersFlag.saveShow = false;
          return;
        }
        validationService.displayValidations.show = false;
        validationService.displayValidations.nameShow = false;

        var snapshotData = extensionSerializer.convertObjectToString(
          extensionSerializer.serializeExtensionProperties($scope.snapshot, 'snapshot'));

        if($scope.editingMode) {
          Falcon.postUpdateExtension(snapshotData, 'HDFS-SNAPSHOT-MIRRORING')
            .success(function (response) {
              $scope.skipUndo = true;
              Falcon.logResponse('success', response, false);
              SpinnersFlag.saveShow = false;
              $state.go('main');

            })
            .error(function (err) {
              Falcon.logResponse('error', err, false);
              SpinnersFlag.saveShow = false;
              angular.element('body, html').animate({scrollTop: 0}, 300);
            });
        } else {
          Falcon.postSubmitExtension(snapshotData, 'HDFS-SNAPSHOT-MIRRORING')
            .success(function (response) {
              $scope.skipUndo = true;
              Falcon.logResponse('success', response, false);
              SpinnersFlag.saveShow = false;
              $state.go('main');
            })
            .error(function (err) {
              Falcon.logResponse('error', err, false);
              SpinnersFlag.saveShow = false;
              angular.element('body, html').animate({scrollTop: 0}, 300);
            });
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
            console.log("type of data not recognized");
          }
	}
      }

      if($state.current.name !== "forms.snapshot.general"){
        $state.go("forms.snapshot.general");
      }

    }]);
}());
