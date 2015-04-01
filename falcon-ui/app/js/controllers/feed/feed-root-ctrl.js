/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * 'License'); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS,
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
  var feedModule = angular.module('app.controllers.feed');

  feedModule.controller('FeedController',
    [ '$scope', '$state', '$timeout',
      'Falcon', 'X2jsService',
      'JsonTransformerFactory', 'EntityFactory',
      'EntitySerializer', '$interval',
      '$controller', "ValidationService",
      function($scope, $state, $timeout, Falcon,
               X2jsService, transformerFactory, entityFactory,
               serializer, $interval, $controller, validationService) {

        $scope.entityType = 'feed';

        //extending root controller
        $controller('EntityRootCtrl', {
          $scope: $scope
        });

        $scope.loadOrCreateEntity = function() {
          var type = $scope.entityType;
          var model = $scope.models[type + 'Model'];
          $scope.models[type + 'Model'] = null;
          return model ? serializer.preDeserialize(model, type) : entityFactory.newEntity(type);
        };

        $scope.init = function() {
          $scope.baseInit();
          var type = $scope.entityType;
          $scope[type] = $scope.loadOrCreateEntity();
          $scope.dateFormat ='dd-MMMM-yyyy';
        };

        $scope.openDatePicker = function($event, container) {
          $event.preventDefault();
          $event.stopPropagation();
          container.opened = true;
        };

        $scope.init();

        $scope.transform = function() {
          var type = $scope.entityType;
          var xml = serializer.serialize($scope[type], $scope.entityType);
          $scope.prettyXml = X2jsService.prettifyXml(xml);
          $scope.xml = xml;
          return xml;
        };

        $scope.saveEntity = function() {
          var type = $scope.entityType;
          if(!$scope.$parent.cloningMode) {
            Falcon.logRequest();
            Falcon.postUpdateEntity($scope.xml, $scope.entityType, $scope[type].name)
              .success(function (response) {
                Falcon.logResponse('success', response, false); 
                $state.go('main');
              })
              .error(function(err) {
                Falcon.logResponse('error', err, false);
              });
          } else {
            Falcon.logRequest();
            Falcon.postSubmitEntity($scope.xml, $scope.entityType)
              .success(function (response) {
                Falcon.logResponse('success', response, false); 
                $state.go('main');
              })
              .error(function(err) {
                Falcon.logResponse('error', err, false);
              });
          }

          $scope.editingMode = false;
          $scope.cloningMode = false;
        };

        $scope.isActive = function (route) {
          return route === $state.$current.name;
        };

        $scope.parseDate = function(input) {
          return input ? input.split('T')[0] : input;
        };

        $scope.parseTime = function(input) {
          if (input) {
            var partialTime = input.split('T')[1].split(':');
            partialTime = partialTime[0] + ':' + partialTime[1];
            return partialTime;
          }
          return 'Not defined';
        };

        $scope.appendVariable = function(timeVariable, holder, fieldName) {
          holder[fieldName] = holder[fieldName] ? (holder[fieldName] + '-' + timeVariable) : timeVariable;
          holder.focused = false;
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
            } catch (exception) {
              console.log('user entered xml incorrect format');
              console.log(exception);
            }
          }

        };

        var xmlPreviewWorker = $interval(xmlPreviewCallback, 1000);

        $scope.$on('$destroy', function () {
          $interval.cancel(xmlPreviewWorker);
        });

        //$scope.nameValid = $scope.$parent.nameValid;
        /*
        * needed for validation
        * */
        $scope.goNext = function (formInvalid, stateName) {
          if (!validationService.nameAvailable || formInvalid) {
            validationService.displayValidations.show = true;
            validationService.displayValidations.nameShow = true;
            return;
          }
          validationService.displayValidations.show = false;
          validationService.displayValidations.nameShow = false;
          $state.go(stateName);
        };
        $scope.goBack = function (stateName) {
          validationService.displayValidations.show = false;
          validationService.displayValidations.nameShow = false;
          $state.go(stateName);
        };

      }]);


  

})();
