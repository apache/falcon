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
   * @requires clusters the list of clusters to display for selection of source
   * @requires EntityModel the entity model to copy the feed entity from
   * @requires Falcon the falcon entity service
   */
  var processModule = angular.module('app.controllers.process');

  processModule.controller('ProcessRootCtrl', [
    '$scope', '$state', '$interval', '$controller', 'EntityFactory', 'RouteHelper',
    'EntitySerializer', 'X2jsService', 'ValidationService', 'SpinnersFlag', '$rootScope', 'ProcessModel', 'Falcon',
    function ($scope, $state, $interval, $controller, entityFactory, RouteHelper,
              serializer, X2jsService, validationService, SpinnersFlag, $rootScope, processModel, Falcon) {

      $scope.entityType = 'process';

      var stateMatrix = {
              general : {previous : '', next : 'summary'},
              summary : {previous : 'general', next : ''}
      };

        //extending root controller
      $controller('EntityRootCtrl', {
        $scope: $scope
      });

      $scope.init = function() {
        $scope.baseInit();
        var type = $scope.entityType;
        $scope[type] = $scope.loadOrCreateEntity();
        if(processModel && processModel.clone === true){
          $scope.cloningMode = true;
          $scope.editingMode = false;
          $scope[type].name = "";
        }else if(processModel && processModel.edit === true){
          $scope.editingMode = true;
          $scope.cloningMode = false;
        }else{
          $scope.editingMode = false;
          $scope.cloningMode = false;
        }
      };

      $scope.isActive = function (route) {
        return route === $state.current.name;
      };

      $scope.isCompleted = function (route) {
        return $state.get(route).data && $state.get(route).data.completed;
      };

      $scope.loadOrCreateEntity = function() {
        var type = $scope.entityType;
        if(!processModel && $scope.$parent.models.processModel){
          processModel = $scope.$parent.models.processModel;
        }
        $scope.$parent.models.processModel = null;
        return processModel ? serializer.preDeserialize(processModel, type) : entityFactory.newEntity(type);
      };

      $scope.init();

      $scope.transform = function() {
        var type = $scope.entityType;
        var xml = serializer.serialize($scope[type], $scope.entityType);
        $scope.prettyXml = X2jsService.prettifyXml(xml);
        $scope.xml = xml;
        return xml;
      };

      var xmlPreviewCallback = function() {
        var type = $scope.entityType;
        if($scope.editXmlDisabled) {
          try {
            $scope.transform();
          } catch (exception) {
            console.log('error when transforming xml');
            console.log(exception);
          }
        } else {
          try {
            $scope[type] = serializer.deserialize($scope.prettyXml, type);
            $scope.invalidXml = false;
          } catch (exception) {
            $scope.invalidXml = true;
            console.log('user entered xml incorrect format');
            console.log(exception);
          }
        }

      };

      $scope.$watch('process', function(){
        if($scope.editXmlDisabled) {
          xmlPreviewCallback();
        }
      }, true);
     $scope.$watch('prettyXml', function(){
       if(!$scope.editXmlDisabled) {
         xmlPreviewCallback();
       }
      }, true);

      $scope.skipUndo = false;
      $scope.$on('$destroy', function() {

        var defaultProcess = entityFactory.newEntity('process'),

          nameIsEqual = ($scope.process.name == null || $scope.process.name === ""), // falsey as it needs also to catch undefined
          ACLIsEqual = angular.equals($scope.process.ACL, defaultProcess.ACL),
          workflowIsEqual = angular.equals($scope.process.workflow, defaultProcess.workflow);

        if (!$scope.skipUndo && (!nameIsEqual || !ACLIsEqual || !workflowIsEqual)) {
          $scope.$parent.models.processModel = angular.copy(X2jsService.xml_str2json($scope.xml));
          if($scope.cloningMode){
            $scope.$parent.models.processModel.clone = true;
          }
          if($scope.editingMode){
            $scope.$parent.models.processModel.edit = true;
          }
          $scope.$parent.cancel('process', $rootScope.previousState);
        }
      });

      //---------------------------------//
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
        validationService.displayValidations.show = false;
        validationService.displayValidations.nameShow = false;
        $state.go(RouteHelper.getNextState($state.current.name, stateMatrix));
      };

      $scope.goBack = function () {
        SpinnersFlag.backShow = true;
        validationService.displayValidations.show = false;
        validationService.displayValidations.nameShow = false;
        $state.go(RouteHelper.getPreviousState($state.current.name, stateMatrix));
      };

      $scope.goTest = function (formInvalid) {
        console.log(formInvalid);
      };

      $scope.toggleclick = function () {
        $('.formBoxContainer').toggleClass('col-xs-14 ');
        $('.xmlPreviewContainer ').toggleClass('col-xs-10 hide');
        $('.preview').toggleClass('pullOver pullOverXml');
        ($('.preview').hasClass('pullOver')) ? $('.preview').find('button').html('Preview XML') : $('.preview').find('button').html('Hide XML');
        ($($("textarea")[0]).attr("ng-model") == "prettyXml" ) ? $($("textarea")[0]).css("min-height", $(".formBoxContainer").height() - 40 ) : '';
      };

      $scope.saveEntity = function() {
        var type = $scope.entityType;
        SpinnersFlag.saveShow = true;

        if($scope.editingMode) {
          Falcon.logRequest();
          Falcon.postUpdateEntity('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'  + $scope.xml, $scope.entityType, $scope[type].name)
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
        else {
          Falcon.logRequest();
          Falcon.postSubmitEntity('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' + $scope.xml, $scope.entityType)
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

      if($state.current.name !== "forms.process.general"){
        $state.go("forms.process.general");
      }
    }
  ]);

}());
