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
   * @name app.controllers.datasource.DatasourceController
   * @requires EntityModel the entity model to copy the datasource entity from
   * @requires Falcon the falcon entity service
   */
  var datasourceModule = angular.module('app.controllers.datasource');

  datasourceModule.controller('DatasourceController',
    [ '$scope', '$state', '$timeout', "RouteHelper",
      'Falcon', 'X2jsService',
      'JsonTransformerFactory', 'EntityFactory',
      'EntitySerializer', '$interval',
      '$controller', "ValidationService",
      "SpinnersFlag", "$rootScope", "DatasourceModel",
      function($scope, $state, $timeout, RouteHelper, Falcon,
               X2jsService, transformerFactory, entityFactory,
               serializer, $interval, $controller,
               validationService, SpinnersFlag, $rootScope, datasourceModel) {

         $scope.entityType = 'datasource';
         var stateMatrix = {
                 general : {previous : '', next : 'summary'},
                 summary : {previous : 'general', next : ''}
         };
         //extending root controller
         $controller('EntityRootCtrl', {
           $scope: $scope
         });

         $scope.skipUndo = false;
         $scope.secureMode = $rootScope.secureMode;

         $scope.$on('$destroy', function () {

          //  if (!$scope.skipUndo && !angular.equals($scope.UIModel, EntityModel.defaultValues.datasourceModel)) {
          //    if($scope.clone){
          //      EntityModel.datasourceModel.UIModel.clone = true;
          //    }
          //    if($scope.editingMode){
          //      EntityModel.datasourceModel.UIModel.edit = true;
          //    }
          //    $scope.$parent.cancel('datasource', $rootScope.previousState);
          //  }
         });

         $scope.loadOrCreateEntity = function() {
           var type = $scope.entityType;
           if(!datasourceModel && $scope.$parent.models.datasourceModel){
             datasourceModel = $scope.$parent.models.datasourceModel;
           }
           $scope.$parent.models.datasourceModel = null;
           return datasourceModel ? serializer.preDeserialize(datasourceModel, type) : entityFactory.newEntity(type);
         };

         $scope.init = function() {
           $scope.baseInit();
           var type = $scope.entityType;
           $scope[type] = $scope.loadOrCreateEntity();
           if(datasourceModel && datasourceModel.clone === true){
             $scope.cloningMode = true;
             $scope.editingMode = false;
             $scope[type].name = "";
           }else if(datasourceModel && datasourceModel.edit === true){
             $scope.editingMode = true;
             $scope.cloningMode = false;
           }else{
             $scope.editingMode = false;
             $scope.cloningMode = false;
           }
         }

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

         $scope.$watch('datasource', xmlPreviewCallback, true);
         $scope.$watch('prettyXml', xmlPreviewCallback, true);
         $scope.$watch('datasource.interfaces', function() {
           if ($scope.datasource.interfaces.interfaces[0]
             && $scope.datasource.interfaces.interfaces[0].credential) {
            $scope.datasource.interfaces.credential = $scope.datasource.interfaces.interfaces[0].credential;
           }
         }, true);

         $scope.toggleclick = function () {
              $('.formBoxContainer').toggleClass('col-xs-14 ');
              $('.xmlPreviewContainer ').toggleClass('col-xs-10 hide');
              $('.preview').toggleClass('pullOver pullOverXml');
              ($('.preview').hasClass('pullOver')) ? $('.preview').find('button').html('Preview XML') : $('.preview').find('button').html('Hide XML');
              ($($("textarea")[0]).attr("ng-model") == "prettyXml" ) ? $($("textarea")[0]).css("min-height", $(".formBoxContainer").height() - 40 ) : '';
          };

          $scope.isActive = function (route) {
              return route === $state.current.name;
          };

          $scope.isCompleted = function (route) {
              return $state.get(route).data && $state.get(route).data.completed;
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

          function cleanXml (xml) {
            var obj = X2jsService.xml_str2json(xml);
            //feed properties
            // if (obj.datasource.properties.property.length === 1 && obj.feed.properties.property[0] === "") {
            //   delete obj.datasource.properties;
            // }

            return X2jsService.json2xml_str(obj);
          }

          $scope.validate = function() {
            var type = $scope.entityType;
            var cleanedXml = cleanXml($scope.xml);
            SpinnersFlag.validateShow = true;
            Falcon.logRequest();

            Falcon.postValidateEntity('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' + cleanedXml, $scope.entityType, $scope[type].name)
              .success(function (response) {
                Falcon.logResponse('success', response, false);
                SpinnersFlag.validateShow = false;
                angular.element('body, html').animate({scrollTop: 0}, 300);
              })
              .error(function(err) {
                Falcon.logResponse('error', err, false);
                SpinnersFlag.validateShow = false;
                angular.element('body, html').animate({scrollTop: 0}, 300);
              });
          }

          $scope.saveEntity = function() {
            var type = $scope.entityType;
            var cleanedXml = cleanXml($scope.xml);
            SpinnersFlag.show = true;

            if($scope.editingMode) {
              Falcon.logRequest();

              Falcon.postUpdateEntity('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' + cleanedXml, $scope.entityType, $scope[type].name)
                .success(function (response) {
                  $scope.skipUndo = true;
                  Falcon.logResponse('success', response, false);
                  $state.go('main');
                })
                .error(function(err) {
                  Falcon.logResponse('error', err, false);
                  SpinnersFlag.show = false;
                  angular.element('body, html').animate({scrollTop: 0}, 300);
                });
            } else {
              Falcon.logRequest();
              Falcon.postSubmitEntity('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' + cleanedXml, $scope.entityType)
                .success(function (response) {
                  $scope.skipUndo = true;
                  Falcon.logResponse('success', response, false);
                  $state.go('main');
                })
                .error(function(err) {
                  Falcon.logResponse('error', err, false);
                  SpinnersFlag.show = false;
                  angular.element('body, html').animate({scrollTop: 0}, 300);
                });
            }

            $scope.editingMode = false;
            $scope.cloningMode = false;
          };

          if($state.current.name !== "forms.datasource.general"){
            $state.go("forms.datasource.general");
          }

      }
    ]);


})();
