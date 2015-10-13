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
   * @requires EntityModel the entity model to copy the feed entity from
   * @requires Falcon the falcon service to talk with the Falcon REST API
   */
  var loginModule = angular.module('app.controllers.login', ['ngMessages', 'app.services']);

  loginModule.controller('LoginFormCtrl', ['$scope', '$state', '$cookieStore', '$http', 'ServerAPI',
    function ($scope, $state, $cookieStore, $http, ServerAPI) {
    $scope.showLoginVal = {show: false, userPassInvalid: false};

    //$scope.loggUser = function(form) {
    //if(form.$valid){
    //	$http.get('config/loginData.json').success(function(data) {
    // 		var user = data.user;
    //	  	var password = data.password;
    // 		if($scope.login.user === user && $scope.login.password === password){
    //  			var userToken = {};
    //  			userToken.timeOutLimit = $scope.login.timeOut;
    //  			userToken.user = $scope.login.user;
    //  			userToken.timeOut = new Date().getTime();
    //  			$cookieStore.put('userToken', userToken);
    //  			$state.go('main');
    //  		}else{
    //			$scope.showLoginVal.show = true;
    //			$scope.showLoginVal.userPassInvalid = true;
    //  		}
    // 	});
    //}else{
    //	$scope.showLoginVal.show = true;
    //}
    //};

    $scope.loggUser = function (form) {
      if (form.$valid) {
        ServerAPI.clearUser().then(function() {
          var userToken = {};
          userToken.timeOutLimit = $scope.login.timeOut;
          userToken.user = $scope.login.user;
          userToken.timeOut = new Date().getTime();
          $cookieStore.put('userToken', userToken);
          $state.go('main');
        });
      } else {
        $scope.showLoginVal.show = true;
      }
    };

  }]);

}());
