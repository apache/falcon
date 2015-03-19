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

  var directivesModule = angular.module('app.directives', [
                                                            'app.services', 
                                                            'app.directives.entities-list',
                                                            'app.directives.server-messages', 
                                                            'app.directives.entity' 
                                                          ]);
	
	directivesModule.directive('navHeader', function () {
		return {
			replace:false,
			restrict: 'A',
			templateUrl: 'html/directives/navDv.html',
			controller: 'HeaderController'
		};
	});
	
	//Angular is not supporting file inputs on change binding that is why this directive
	directivesModule.directive('fileinputChange', function() {
    return {
			restrict: "A",
			link: function (scope, element, attrs) {
	      var onChangeFunc = element.scope()[attrs.fileinputChange];
				element.bind('change', onChangeFunc);	
				element.bind('click', function() {
				  this.value = '';
				});			
			}
    };
	});

  directivesModule.factory('EncodeService', function() {
    return {
      encode: function(data) {
        return encodeURIComponent(data);
      }
    };
  });
  
  directivesModule.directive('checkName', ["Falcon", function(Falcon) {
    return {
      replace: false,
      scope: {
        checkName: "@",
        checkNameFlag: "=",
        check: "="
      },
      restrict: 'A',
      link: function (scope, element) {          
        function checkName() {
          var name = element[0].value;
          Falcon.getEntityDefinition(scope.checkName, name)
          .success(function () {
            element.addClass('nameAlreadyRegistered');
            element.parent().append('<label class="text-danger" id="nameWarning">Name ' + name + ' already registered</label>');
            scope.checkNameFlag = false;        
          })
          .error(function () {  
            element.removeClass('nameAlreadyRegistered');
            scope.checkNameFlag = true;
          });
        }
        function restart() {
          element.removeClass('nameAlreadyRegistered'); 
          $('#nameWarning').remove();
        }
        if(scope.check) {
          element.bind('blur', checkName);       
          element.bind('focus', restart);
        }
        else {
          scope.checkNameFlag = true;
        }
             
      }
    };
  }]);
  
  directivesModule.directive('frequency', function() {
    return {
      replace: false,
      scope: {
        value: "=",
        prefix: "@"
      },
      restrict: 'E',
      template: '{{output}}',
      link: function(scope) {
        if(scope.value.quantity) {
          scope.output = scope.prefix + ' ' + scope.value.quantity + ' ' + scope.value.unit;
        } else {
          scope.output = 'Not specified';
        }
      }
    };
  });
    
  directivesModule.directive('timeZoneSelect', function() {
    return {
      restrict: 'E',
      replace: false,
      scope: {
        ngModel: '='
      },
      templateUrl: 'html/directives/timeZoneSelectDv.html'
    };
  });
    
})();