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
  var module = angular.module('app.services.entity.details', []);

  module.factory('EntityDetails', ['Falcon','$q','X2jsService', 'EntityModel', function(Falcon, $q, X2jsService, EntityModel){

    var entityDetails = {};

    entityDetails.getEntityDetails = function(name, type){
      Falcon.logRequest();
      var entityDetailsPromise = Falcon.getEntityDefinition(type, name);
      var entityStatusPromise = Falcon.getEntityStatus(type, name);
      return $q.all([entityDetailsPromise,entityStatusPromise]).then(function(responses){
            Falcon.logResponse('success', responses[0].data, false, true);
            Falcon.logResponse('success', responses[1].data, false, true);
            var entityModel = X2jsService.xml_str2json(responses[0].data);
            EntityModel.type = type;
            EntityModel.name = name;
            var status = responses[1].data.message;
            EntityModel.status = status.substr(status.indexOf("/") + 1, status.length - 1).trim();
            EntityModel.model = entityModel;
            return EntityModel;
      },function(err){
            Falcon.logResponse('error', err, type);
      });
    };

    entityDetails.getEntityDefinition  = function(type, name){
      var deferred = $q.defer();
      type = type.toLowerCase(); //new sandbox returns uppercase type
      Falcon.logRequest();
      Falcon.getEntityDefinition(type, name)
        .success(function (data) {
            Falcon.logResponse('success', data, false, true);
            EntityModel.type = type;
            EntityModel.name = name;
            deferred.resolve(X2jsService.xml_str2json(data));
        })
        .error(function (err) {
            Falcon.logResponse('error', err, false, true);
            deferred.reject(err);
        });
      return deferred.promise;
    };
    return entityDetails;
  }]);
})();
