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
/**
 * It uses showValidationStyle , validationMessageGral clases in common.less
 * root controller $scope.displayValidations = {show: false, nameShow: false};
 * form.Tpl has ng-class of above variable that adds and removes displayValidationStyle
 *
 * */
(function () {
  'use strict';

  var directivesModule = angular.module('app.directives.validation-message', [
    'app.services'
  ]);

  directivesModule.directive('validationMessage', ["ValidationService", function (validationService) {
    return {
      replace: false,
      scope: {
        validationMessage: "@"
      },
      restrict: 'A',
      link: function (scope, element) {

        var lastOne = 0,
          stringLabel,
          messageObject = angular.fromJson(scope.validationMessage);

        scope.messageSwitcher = { show: false };

        messageObject.patternInvalid = messageObject.patternInvalid || messageObject.empty;

        function getLabelElement() {
          lastOne = 0;
          element.parent().append(
            '<label ng-show="messageSwitcher.show" class="custom-danger validationMessageGral"></label>'
          );
          angular.forEach(element.parent().children(), function () {
            lastOne = lastOne + 1;
          });
          lastOne = lastOne - 1;
          stringLabel = $(element).parent().children()[lastOne];
        }

        function checkNameInList() {

          var name = element[0].value;

          if (name.length === 0) {

            scope.messageSwitcher.show = false;

            element.parent().removeClass("showMessage showValidationStyle validationMessageParent");

          } else if (name.length > 0 && element.hasClass('ng-valid')) {
            element.removeClass('empty');
            element.parent().removeClass("showMessage showValidationStyle validationMessageParent");
            scope.messageSwitcher.show = false;
            angular.element(stringLabel).addClass('valid');

          } else if (element.hasClass('ng-invalid-pattern') && name.length > 0) {
            scope.messageSwitcher.show = true;
            angular.element(stringLabel).html(messageObject.patternInvalid).removeClass('valid');
            element.removeClass('empty');
            element.parent().addClass("showMessage showValidationStyle validationMessageParent");

          } else {
            element.addClass('empty');
            element.parent().removeClass("showMessage");
            scope.messageSwitcher.show = false;
          }
        }
        function addListeners() {

          if (element[0].type === "select-one") {
            element.bind('change', function () {
              scope.messageSwitcher.show = false;
              angular.element(stringLabel).hide();
            });
          } else {
            element.bind('keyup', checkNameInList);
            element.bind('blur', function () {
              if (element[0].value.length === 0) {
                element.parent().addClass("showMessage showValidationStyle validationMessageParent");
                scope.messageSwitcher.show = true;
                angular.element(stringLabel).html(messageObject.empty).removeClass('valid');
              }
            });
            element.bind('focus', function () {
              element.removeClass('empty');
            });
          }
        }
        function normalize() {
          setTimeout(function () {
            if (element.hasClass('ng-valid') || element[0].value.length === 0) {
              scope.messageSwitcher.show = false;
            } else {
              scope.messageSwitcher.show = true;
            }
            if (element[0].value.length === 0) {
              angular.element(stringLabel).html(messageObject.empty);
              scope.messageSwitcher.show = true;
            }
          }, 100);
        }
        function init() {
          getLabelElement();
          addListeners();
          normalize();
        }
        init();

        scope.$watch(function () {
          return validationService.displayValidations;
        }, normalize);

        scope.$watch(function () {
          return element[0].value.length;
        }, function () {
          if (element[0].value.length === 0) {
            element.addClass('empty');
          }
        });
      }
    };
  }]);

  directivesModule.directive('validationOptionalMessage', [function () {
    return {
      replace: false,
      scope: {
        validationOptionalMessage: "@",
        required: "@"
      },
      restrict: 'A',
      link: function (scope, element, attrs) {

        var lastOne = 0,
          valLength = element[0].value.length,
          required = attrs.required,
          stringLabel,
          valid,
          invalidPattern,
          messageObject = angular.fromJson(scope.validationOptionalMessage);

        messageObject.patternInvalid = messageObject.patternInvalid || messageObject.empty;

        function getLabelElement() {
          lastOne = 0;
          element.parent().append(
            '<label ng-show="messageSwitcher.show" class="custom-danger validationMessageGral"></label>'
          );
          angular.forEach(element.parent().children(), function () {
            lastOne = lastOne + 1;
          });
          lastOne = lastOne - 1;
          stringLabel = $(element).parent().children()[lastOne];
        }

        function prepare() {

          valLength = element[0].value.length;
          required =  attrs.required;
          valid = element.hasClass('ng-valid');
          invalidPattern = element.hasClass('ng-invalid-pattern');

          if (valLength === 0 && required) {
            element.addClass('empty');
            angular.element(stringLabel).html(messageObject.empty).addClass('hidden');
            element.parent().removeClass("showMessage showValidationStyle validationMessageParent");

          } else if (valLength === 0 && !required) {
            element.addClass('empty');
            element.parent().removeClass("showMessage showValidationStyle validationMessageParent");
            angular.element(stringLabel).addClass('hidden');

          } else if (invalidPattern && valLength > 0) {
            element.removeClass('empty');
            angular.element(stringLabel).html(messageObject.patternInvalid).removeClass('hidden');
            element.parent().addClass("showMessage showValidationStyle validationMessageParent");

          } else if (valid && valLength > 0) {
            element.removeClass('empty');
            angular.element(stringLabel).addClass('hidden');
            element.parent().removeClass("showMessage showValidationStyle validationMessageParent");

          } else {
            console.log("else");
          }
        }
        function addListeners() {

          element.bind('keyup', prepare);
          element.bind('blur', function () {
            if (valLength === 0 && required) {
              element.removeClass('empty');
              angular.element(stringLabel).html(messageObject.empty).removeClass('hidden');
              element.parent().addClass("showMessage showValidationStyle validationMessageParent");
            }
          });
        }
        function normalize() {
          prepare();
          setTimeout(function () {
            if (valLength === 0 && required) {
              angular.element(stringLabel).removeClass('hidden');
              element.removeClass('empty');
            }
          }, 100);
        }
        function init() {
          getLabelElement();
          addListeners();
          prepare();
        }
        init();

        scope.$watch(function () {
          return scope.required;
        }, normalize);

        scope.$watch(function () {
          return element[0].value.length;
        }, function () {
          if (element[0].value.length === 0) {
            element.addClass('empty');
          }
        });
      }
    };
  }]);

}());