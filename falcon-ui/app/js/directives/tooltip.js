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

  var module = angular.module('tooltip', []);

  module.directive('toogle', function () {
    return {
      restrict: 'A',
      link: function(scope, element, attrs){
        if (attrs.toggle=="tooltip"){
          $(element).tooltip();
        }
        if (attrs.toggle=="popover"){
          $(element).popover();
        }
      }
    };
  });

  module.directive("tooltip", ['TooltipMessages', function(TooltipMessages){
    return {
      restrict: "A",
      link: function(scope, element, attrs) {
        var message = TooltipMessages.messages[attrs.tooltip];
        if (!message) {
          console.warn('Message not defined for key ' + attrs.tooltip);
          return;
        }
        var tooltipElement = angular.element("<div class='entities-tooltip-theme'>");
        tooltipElement.append("<div class='arrow-up'></div>");
        tooltipElement.append("<div class='entities-tooltip'>" + message + "</div>");
        element.append(tooltipElement);
        element.on('mouseenter', function(){
             tooltipElement.show();
           }).on('mouseleave', function(){
             tooltipElement.hide();
           });
      }
    };
  }]);

})();
