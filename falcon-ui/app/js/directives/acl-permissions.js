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

  var aclPermissionsModule = angular.module('app.directives.acl-permissions',[]);

  aclPermissionsModule.directive('aclPermissions', ['$timeout',function ($timeout) {
      return {
          restrict : 'EA',
          templateUrl: 'html/directives/aclPermissions.html',
          link: function ($scope, element, attrs) {

            var permission = {
              'owner' : 0,
              'groups' : 0,
              'others' : 0
            }

            function getNestedProperty(obj, path) {
              var paths = path.split('.'),
              current = obj;
              for (var i = 0; i < paths.length; i++) {
                if (current[paths[i]] == undefined) {
                  return undefined;
                } else {
                  current = current[paths[i]];
                }
              }
              return current;
            };

            function setNestedProperty(obj, path, value) {
              var paths = path.split('.'),
              current = obj;
              for (var i = 0; i < paths.length -1; i++) {
                  current = current[paths[i]];
              }
              current[paths[paths.length - 1]] = value;
            };

            var defaultPermission = getNestedProperty($scope, attrs.aclModel);

            function setPermission() {
              if(defaultPermission){
                var permissionArray = defaultPermission.substring(2).split("");
                permissionArray.forEach(function(value, index){
                  var permissionValue = parseInt(value);
                  var type = Object.keys(permission)[index];
                  permission[type] = value;
                  var checked = element.find('input[name=' + type + ']');
                  var readPermission = 4,
                      writePermission = 2,
                      executePermission = 1;
                  if(permissionValue & readPermission){
                    angular.element(checked[0]).attr('checked','checked');
                  }
                  if(permissionValue & writePermission){
                    angular.element(checked[1]).attr('checked','checked');
                  }
                  if(permissionValue & executePermission){
                    angular.element(checked[2]).attr('checked','checked');
                  }
                });
              }
            };

            $scope.calculatePermission = function(type){
              var checked = element.find('input[name=' + type + ']:checked');
              var total = 0;
              angular.forEach(checked,function(element){
                total += parseInt(element.value);
              });
              permission[type] = total;
              var effectivePermission = '0x';
              angular.forEach(permission, function(value,key){
                effectivePermission = effectivePermission + value;
              });
              setNestedProperty($scope, attrs.aclModel, effectivePermission);
            };

            $timeout(function () {
              setPermission();
            }, 0);
          }
      };
  }]);
})();
