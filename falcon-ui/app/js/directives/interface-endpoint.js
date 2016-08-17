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

  var interfaceEndpointModule = angular.module('app.directives.interface-endpoint', []);

  interfaceEndpointModule.directive('interfaceEndpoint', [ "$timeout", function ($timeout) {
    return {
      require: 'ngModel',

      link: function($scope, $element, $attrs, ngModelCtrl) {
        function applyDefaultStyle() {
          $element.removeClass('endpointChanged');
          $element.addClass('endpointDefault');
        }

        function applyModifiedStyle() {
          $element.removeClass('endpointDefault');
          $element.addClass('endpointChanged');
        }

        $scope.$watch(function(){
          return $scope.clusterForm.$submitted;
        },function(){
          if(!$element.attr('disabled') && ngModelCtrl.$pristine){
            $element.parent().find('.validationMessageGral').show();
            ngModelCtrl.$setValidity('pattern', false);
          }else if(ngModelCtrl.$dirty && ngModelCtrl.$error.pattern && !$element.attr('disabled')){
            $element.parent().find('.validationMessageGral').show();
            ngModelCtrl.$setValidity('pattern', false);
          }else if($element.attr('disabled')){
            $element.parent().find('.validationMessageGral').hide();
            ngModelCtrl.$setValidity('pattern', true);
          }
        });

        $scope.$watch(function(){
          if(!$scope.clusterForm.$submitted && ngModelCtrl.$pristine){
            $element.parent().find('.validationMessageGral').hide();
            ngModelCtrl.$setValidity('pattern', true);
          }else if(ngModelCtrl.$dirty && ngModelCtrl.$error.pattern){
            $element.parent().find('.validationMessageGral').show();
            ngModelCtrl.$setValidity('pattern', false);
          }
          return $element[0].value;
        },function(newValue, oldValue){
          if(newValue){
            if(newValue === oldValue && newValue.indexOf('<hostname>') >= 0){
              applyDefaultStyle();
            }else{
              applyModifiedStyle();
            }
          }
        });

        $element.focus(function(){
          applyModifiedStyle();
        });

        $element.bind('blur',function(){
          if(ngModelCtrl.$pristine && $element[0].value.indexOf('<hostname>') >= 0){
            applyDefaultStyle();
          }
        });
      }
    }
  }]);

}());
