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

  var dashboardCtrlModule = angular.module('app.controllers.dashboardCtrl', ['app.services']);

  dashboardCtrlModule.controller('DashboardCtrl', [ "$scope", "Falcon", "EntityModel", "FileApi", "$state", "X2jsService",
    function ($scope, Falcon, EntityModel, FileApi, $state, X2jsService) {
      
      $scope.$parent.refreshLists();

      $scope.deleteEntity = function (type, name) {
        type = type.toLowerCase(); //new sandbox returns uppercase type
        Falcon.logRequest();
        Falcon.deleteEntity(type, name)
          .success(function (data) {          
            Falcon.logResponse('success', data, type);           
            $scope.$parent.refreshList(type);              
          })
          .error(function (err) {
            
            Falcon.logResponse('error', err, type);
          });
      };
      $scope.cloneEntity = function (type, name) {
        type = type.toLowerCase(); //new sandbox returns uppercase type
        
        Falcon.logRequest();
        Falcon.getEntityDefinition(type, name)
          .success(function (data) {
            Falcon.logResponse('success', data, false, true);
            var modelName = type + "Model",
                entityModel = X2jsService.xml_str2json(data);
                
            EntityModel[modelName] = entityModel;
            EntityModel[modelName][type]._name = "";
            $scope.models[modelName] = angular.copy(entityModel);
            $scope.cloningMode = true; // dont know utility of this
            $scope.$parent.cloningMode = true;
            $state.go('forms.' + type + ".general");
          })
          .error(function (err) {
            Falcon.logResponse('error', err, false, true);
          });
      };
      $scope.editEntity = function (type, name) {        
        type = type.toLowerCase(); //new sandbox returns uppercase type
        
        Falcon.logRequest();
        Falcon.getEntityDefinition(type, name)
          .success(function (data) {
            Falcon.logResponse('success', data, false, true);
            var entityModel = X2jsService.xml_str2json(data);
            var modelName = type + "Model";
            EntityModel[modelName] = entityModel;
            $scope.models[modelName] = angular.copy(entityModel);
            $scope.editingMode = true;// dont know utility of this
            $scope.$parent.cloningMode = false;
            $state.go('forms.' + type + ".general");
          })
          .error(function (err) {
            Falcon.logResponse('error', err, false, true);
          });
      };
      //-----------------------------------------//
      $scope.entityDetails = function (name, type) {
    	  type = type.toLowerCase(); //new sandbox returns uppercase type
    	  
    	  Falcon.logRequest();
          Falcon.getEntityDefinition(type, name)
            .success(function (data) {
              Falcon.logResponse('success', data, false, true);
              var entityModel = X2jsService.xml_str2json(data);
              var modelName = type + "Model";
              EntityModel[modelName] = entityModel;
              $scope.models[modelName] = angular.copy(entityModel);
              $scope.editingMode = true;// dont know utility of this
              $scope.$parent.cloningMode = false;
              //$state.go('forms.' + type + ".general");
              $state.go('entityDetails');
            })
            .error(function (err) {
              Falcon.logResponse('error', err, false, true);
            });
      };
      //----------------------------------------//
      $scope.resumeEntity = function (type, name) {
        Falcon.logRequest();
        Falcon.postResumeEntity(type, name).success(function (data) {
          Falcon.logResponse('success', data, type);
          $scope.$parent.refreshList(type);      
        })
        .error(function (err) {
          Falcon.logResponse('error', err, type);
        });
      };
      $scope.scheduleEntity = function (type, name) {
        Falcon.logRequest();
        Falcon.postScheduleEntity(type, name).success(function (data) {
          Falcon.logResponse('success', data, type);
          $scope.$parent.refreshList(type);      
        })
        .error(function (err) {
          Falcon.logResponse('error', err, type);
        });
      };

      $scope.suspendEntity = function (type, name) {
        Falcon.logRequest();
        Falcon.postSuspendEntity(type, name)
          .success(function (message) {
            Falcon.logResponse('success', message, type);           
            $scope.$parent.refreshList(type);      
          })
          .error(function (err) {
            Falcon.logResponse('error', err, type);
            
          });
      };
      $scope.relationsEntity = function (type, name) {
        console.log("relations " + type + " - " + name);
      };
      
      
    }]);

})();