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

  var entityScheduler = angular.module("app.services.entity.scheduler",['app.services.falcon']);
  entityScheduler.factory('EntityScheduler',["$window", "$q", "Falcon", "EncodeService", "X2jsService", function($window, $q, Falcon, encodeService, X2jsService){
    var entityScheduler = {};
    var entityStatus = {
      RUNNING : "RUNNING",
      SUBMITTED : "SUBMITTED",
      SUSPENDED : "SUSPENDED",
      DELETED : "DELETED",
      FAILED : "FAILED"
    };

    entityScheduler.resumeEntity = function (type, name) {
      var deferred = $q.defer();
      type = type.toLowerCase();
      Falcon.logRequest();
      Falcon.postResumeEntity(type, name).success(function (data) {
        Falcon.logResponse('success', data, type);
        deferred.resolve(entityStatus.RUNNING);
      })
      .error(function (err) {
        Falcon.logResponse('error', err, type);
        deferred.resolve(entityStatus.SUSPENDED);
      });
      return deferred.promise;
    };

    entityScheduler.scheduleEntity = function (type, name) {
      var deferred = $q.defer();
      type = type.toLowerCase();
      Falcon.logRequest();
      Falcon.postScheduleEntity(type, name).success(function (data) {
        Falcon.logResponse('success', data, type);
        deferred.resolve(entityStatus.RUNNING);
      })
      .error(function (err) {
        Falcon.logResponse('error', err, type);
        deferred.resolve(entityStatus.SUBMITTED);
      });
      return deferred.promise;
    };

    entityScheduler.suspendEntity = function (type, name) {
      var deferred = $q.defer();
      Falcon.logRequest();
      type = type.toLowerCase();
      Falcon.postSuspendEntity(type, name)
        .success(function (message) {
          Falcon.logResponse('success', message, type);
          deferred.resolve(entityStatus.SUSPENDED);
        })
        .error(function (err) {
          Falcon.logResponse('error', err, type);
          deferred.resolve(entityStatus.RUNNING);
        });
        return deferred.promise;
    };

    entityScheduler.deleteEntity = function (type, name) {
      type = type.toLowerCase(); //new sandbox returns uppercase type
      var deferred = $q.defer();
      Falcon.logRequest();
      Falcon.deleteEntity(type, name)
        .success(function (data) {
          Falcon.logResponse('success', data, type);
          deferred.resolve(entityStatus.DELETED);
        })
        .error(function (err) {
          Falcon.logResponse('error', err, type);
          deferred.resolve(entityStatus.FAILED);
        });
        return deferred.promise;
    };

    entityScheduler.getEntityDefinition  = function(type, name){
      var deferred = $q.defer();
      type = type.toLowerCase(); //new sandbox returns uppercase type
      Falcon.logRequest();
      Falcon.getEntityDefinition(type, name)
        .success(function (data) {
            Falcon.logResponse('success', data, false, true);
            deferred.resolve(X2jsService.xml_str2json(data));
        })
        .error(function (err) {
            Falcon.logResponse('error', err, false, true);
            deferred.reject(entityStatus.FAILED);
        });
      return deferred.promise;
    };

    entityScheduler.downloadEntity = function (type, name) {
      type = type.toLowerCase();
      Falcon.logRequest();
      Falcon.getEntityDefinition(type, name) .success(function (data) {
        Falcon.logResponse('success', data, false, true);
        $window.location.href = 'data:application/octet-stream,' + encodeService.encode(data);
      }).error(function (err) {
        Falcon.logResponse('error', err, false);
      });
    };
    return entityScheduler;
  }]);
})();
