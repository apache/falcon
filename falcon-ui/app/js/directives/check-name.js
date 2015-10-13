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

  var checkNameModule = angular.module('app.directives.check-name', ['app.services.falcon', 'app.services.validation']);

  checkNameModule.directive('checkName', [ "ValidationService", "$timeout", "Falcon", "EntityFalcon", function (validationService, $timeout, Falcon, EntityFalcon) {
    return {
      replace: false,
      scope: {
        checkName: "="
      },
      restrict: 'A',
      link: function (scope, element) {

        var options = scope.checkName,
          //entities = scope.$parent.lists[options.type + 'List'],
          type = options.type,
          name = element[0].value;

        if (!options.check) {
          return;
        }

        scope.$watch(function () {
          return element[0].value;
        }, function () {
          if (!scope.$parent.editXmlDisabled) {
            if (element[0].value.length === 0) {
              element.addClass('empty');
            }
            getNameAvailability(function() {
              getMessage();
              if (element.hasClass('ng-valid') && validationService.nameAvailable) {
                angular.element('.nameValidationMessage').addClass('hidden');
              } else {
                element.parent().addClass("showValidationStyle");
                angular.element('.nameValidationMessage').removeClass('hidden');
                element.removeClass('empty');
              }
            });
          }


        });

        function getLabels() {
          element.parent()
            .append("<div class='nameInputDisplay hidden'>" +
                "</div><label class='custom-danger nameValidationMessage'></label>");
        }

        function getNameAvailability(fn) {
          name = element[0].value;
          if (name.length === 0) {
            angular.element('.nameInputDisplay').addClass('hidden');
          }else{
            Falcon.logRequest();
            Falcon.getEntityDefinition(type, name).success(function (data) {
              Falcon.logResponse('success', data, false, true);
              validationService.nameAvailable = false;
              if (name.length === 0) {
                angular.element('.nameInputDisplay').addClass('hidden');
              } else if (!validationService.nameAvailable && name.length > 0 && element.hasClass('ng-valid')) {
                angular.element('.nameInputDisplay').html('Name unavailable')
                    .removeClass('custom-success hidden').addClass('custom-danger');
              } else if (validationService.nameAvailable && name.length > 0 && element.hasClass('ng-valid')) {
                angular.element('.nameInputDisplay').html('Name available')
                    .removeClass('custom-danger hidden').addClass('custom-success');
              } else if (element.hasClass('ng-invalid-pattern') && name.length > 0) {
                angular.element('.nameInputDisplay').addClass('hidden');
              }
              if (fn) { fn(); } //>callback
            }).error(function (err) {
              Falcon.logResponse('error', err, false, true);
              validationService.nameAvailable = true;
              if (name.length === 0) {
                angular.element('.nameInputDisplay').addClass('hidden');
              } else if (!validationService.nameAvailable && name.length > 0 && element.hasClass('ng-valid')) {
                angular.element('.nameInputDisplay').html('Name unavailable')
                    .removeClass('custom-success hidden').addClass('custom-danger');
              } else if (validationService.nameAvailable && name.length > 0 && element.hasClass('ng-valid')) {
                angular.element('.nameInputDisplay').html('Name available')
                    .removeClass('custom-danger hidden').addClass('custom-success');
              } else if (element.hasClass('ng-invalid-pattern') && name.length > 0) {
                angular.element('.nameInputDisplay').addClass('hidden');
              }
              if (fn) { fn(); } //>callback
            });
          }

        }

        function getMessage() {
          if (name.length === 0) {
            element.addClass('empty');
            angular.element('.nameValidationMessage').html(validationService.messages.name.empty).addClass('hidden');

          } else if (!validationService.nameAvailable && name.length > 0 && element.hasClass('ng-valid')) {
            element.addClass('empty');
            angular.element('.nameValidationMessage')
              .html(validationService.messages.name.unavailable).addClass('hidden');

          } else if (element.hasClass('ng-invalid-pattern') && name.length > 0) {
            element.removeClass('empty');
            element.parent().addClass("showValidationStyle");
            angular.element('.nameValidationMessage')
              .html(validationService.messages.name.patternInvalid).removeClass('hidden');

          } else if (element.hasClass('ng-valid') && name.length > 0) {
            element.parent().removeClass("showValidationStyle");
            angular.element('.nameValidationMessage').addClass('hidden');
          }
        }
        function addListeners() {
          element.bind('keyup', function () {
            getNameAvailability();
            getMessage();
          });
          element.bind('focus', function () {
            element.removeClass('empty');
          });
          element.bind('blur', function () {
            if (element.hasClass('ng-valid') && validationService.nameAvailable) {
              angular.element('.nameValidationMessage').addClass('hidden');

            } else {
              element.parent().addClass("showValidationStyle");
              angular.element('.nameValidationMessage').removeClass('hidden');
              element.removeClass('empty');
            }
          });
        }

        function init() {
          getLabels();
          addListeners();
          getNameAvailability();
          getMessage();

          $timeout(function () { element.trigger('focus'); }, 20);
        }

        init();
      }
    };
  }]);

}());