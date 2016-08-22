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

  checkNameModule.directive('checkName', [ "ValidationService", "$timeout", "Falcon", "EntityFalcon","$q", function (validationService, $timeout, Falcon, EntityFalcon,$q) {
    return {
      scope: {
        checkName: "="
      },
    require: ['ngModel','^form'],
    link: function(scope, element, attrs, ctrls) {
      var options = scope.checkName,
        //entities = scope.$parent.lists[options.type + 'List'],
        type = options.type,
        name = element[0].value,
        errorMsg,
        ngModelCtrl = ctrls[0],
        formCtrl = ctrls[1];
        if (!options.check) {
          return;
        }

        element.parent()
          .append("<div class='nameInputDisplay hidden'>" +
              "</div><label class='custom-danger nameValidationMessage'></label>");

        scope.$watch(function () {
          return element[0].value;
        }, function () {
          if (!scope.$parent.editXmlDisabled) {
            if (element[0].value.length === 0) {
              element.addClass('empty');
            }
          }
        });

        scope.$watch(function(){
          return ngModelCtrl.$valid;
        }, function(newValue, oldValue){
          if(ngModelCtrl.$dirty && newValue){
            errorMsg = "Name available";
            validationService.nameAvailable = true;
          }else {
              if(ngModelCtrl.$error.uniqueName){
                  errorMsg = validationService.messages.name.unavailable;
                  validationService.nameAvailable = false;
              }else if(ngModelCtrl.$error.required){
                  errorMsg = validationService.messages.name.empty;
              }else if(ngModelCtrl.$error.pattern){
                  errorMsg = validationService.messages.name.patternInvalid;
              }
          }
        });

        scope.$watch(function(){
          return errorMsg;
        },function(newValue, oldValue){
          if(newValue && newValue.length > 0){
            element.parent().addClass("showValidationStyle");
            element.removeClass('empty');
            if(errorMsg ==='Name available'){
              angular.element('.nameInputDisplay').removeClass('custom-danger hidden').addClass('custom-success');
              angular.element('.nameInputDisplay').text(newValue);
              angular.element('.nameValidationMessage').addClass("hidden");
            }else{
              angular.element('.nameValidationMessage').text(newValue);
              angular.element('.nameValidationMessage').removeClass('hidden').addClass('custom custom-danger');
              angular.element('.nameInputDisplay').addClass("hidden");
            }
          }else{
            element.parent().removeClass("showValidationStyle");
          }
        });

        ngModelCtrl.$asyncValidators.uniqueName = function isNameAvailable(modelValue,viewValue) {
          Falcon.logRequest();
          var def = $q.defer();
          Falcon.getEntityDefinition(type, modelValue).success(function (data) {
            Falcon.logResponse('success', data, false, true);
            def.reject();
          }).error(function (err) {
            Falcon.logResponse('error', err, false, true);
            def.resolve();
          });
          return def.promise;
        }

        element.bind('blur',function(event){
            if(ngModelCtrl.$valid){
              angular.element('.nameValidationMessage').addClass('hidden');
            }else{
              angular.element('.nameValidationMessage').removeClass('hidden');
            }
        });

        element.bind('focus',function(event){
            if(formCtrl.$submitted && ngModelCtrl.$pristine && ngModelCtrl.$error.required){
              angular.element('.nameValidationMessage').removeClass('hidden');
              element.parent().addClass('showValidationStyle');
            }else{
              angular.element('.nameValidationMessage').addClass('hidden');
              element.parent().removeClass('showValidationStyle');
            }
        });

        $timeout(function () { element.trigger('focus'); }, 0);
      }
    }
  }]);

}());
