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

  var app = angular.module('app.services.server', ['app.services']);

  app.factory('ServerAPI', [
    "Falcon", "$q",
    function (Falcon, $q) {

      var ServerAPI = {};

      ServerAPI.getServerConfig = function(){
        var deffered = $q.defer();
        Falcon.logRequest();
        Falcon.getServerConfig().success(function (data) {
          Falcon.logResponse('success', data, false, true);
          ServerAPI.data = data;
          deffered.resolve();
        }).error(function (err) {
          Falcon.logResponse('error', err);
          deffered.resolve();
        });
        return deffered.promise;
      };

      ServerAPI.setOptions = function(username){
        var deffered = $q.defer();
        Falcon.logRequest();
        Falcon.setOptions(username).success(function (data) {
          Falcon.logResponse('success', data, false, true);
          deffered.resolve();
        }).error(function (err) {
          Falcon.logResponse('error', err);
          deffered.resolve();
        });
        return deffered.promise;
      };

      ServerAPI.clearUser = function(){
        var deffered = $q.defer();
        Falcon.logRequest();
        Falcon.clearUser().success(function (data) {
          Falcon.logResponse('success', data, false, true);
          ServerAPI.setted = data;
          deffered.resolve();
        }).error(function (err) {
          Falcon.logResponse('error', err);
          deffered.resolve();
        });
        return deffered.promise;
      };

      ServerAPI.getCurrentUser = function(){
        var deffered = $q.defer();
        Falcon.logRequest();
        Falcon.getCurrentUser().success(function (data) {
          Falcon.logResponse('success', data, false, true);
          ServerAPI.user = data;
          deffered.resolve();
        }).error(function (err) {
          Falcon.logResponse('error', err);
          deffered.resolve();
        });
        return deffered.promise;
      };

      return ServerAPI;

    }]);

}());