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

  var navHeaderModule = angular.module('app.controllers.navHeader', [
    'app.services.entity.model',
    'app.services.validation',
    'ngCookies'
  ]);

  navHeaderModule.controller('HeaderController', [
    '$rootScope', '$scope', '$state', '$cookieStore', '$timeout', 'EntityModel', 'ValidationService', 'Falcon',
    function ($rootScope, $scope, $state, $cookieStore, $timeout, EntityModel, validationService, Falcon) {

      $scope.fake = { focus: false }; //used in upload button to fake the focus borders
      $scope.notifs = false;
      $scope.responses = Falcon.responses;

      $scope.isInForm = function (type) {
        if($rootScope.currentState) {
          var currState = $rootScope.currentState.split('.'),
            formType = currState[1];
          return type === formType;
        }
      };

      $scope.resetCluster = function () {
        validationService.displayValidations = {show: false, nameShow: false};
        angular.copy(EntityModel.defaultValues.cluster, EntityModel.clusterModel);
        $state.go("forms.cluster.general");
      };

      $scope.resetProcess = function () {
        validationService.displayValidations = {show: false, nameShow: false};
        $scope.cloningMode = true;
        $scope.models.processModel = null;
        $state.go("forms.process.general");
      };

      $scope.resetFeed = function () {
        validationService.displayValidations = {show: false, nameShow: false};
        $scope.cloningMode = true;
        $scope.models.feedModel = null;
        $state.go("forms.feed.general");
      };

      $scope.resetDataset = function () {
        validationService.displayValidations = {show: false, nameShow: false};
        EntityModel.datasetModel.toImportModel = undefined;
        angular.copy(EntityModel.defaultValues.MirrorUIModel, EntityModel.datasetModel.UIModel);
        $scope.cloningMode = true;
        $scope.models.feedModel = null;
        $state.go("forms.dataset.general");
      };

      $scope.userLogged = function () {
        if($rootScope.isSecureMode()){
          return true;
        }else if($rootScope.userLogged()){
		if(angular.isDefined($cookieStore.get('userToken')) && $cookieStore.get('userToken') !== null){
			$scope.userToken = $cookieStore.get('userToken').user;
			return true;
		}else{
            return false;
		}
	  }else{
		  return false;
	  }
      };

      $scope.isSecureMode = function () {
        return $rootScope.isSecureMode();
      };

      $scope.logOut = function() {
	$cookieStore.put('userToken', null);
	$state.transitionTo('login');
      };

      $scope.restore = function(state) {
        $state.go(state);
      };

      $scope.notify = function() {
        Falcon.notify(true);
      };

      $scope.hideNotifs = function() {
        Falcon.hideNotifs();
      };

    }]);

    navHeaderModule.filter('reverse', function() {
      return function(items) {
        return items.slice().reverse();
      };
    });

})();