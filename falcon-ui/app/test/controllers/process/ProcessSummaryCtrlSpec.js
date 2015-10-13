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
  var scope, controller, falconServiceMock;

  describe('ProcessSummaryCtrl', function () {
    beforeEach(module('app.controllers.process'));

    beforeEach(inject(function($q, $rootScope, $controller) {
      falconServiceMock = jasmine.createSpyObj('Falcon', ['postUpdateEntity', 'postSubmitEntity', 'logRequest', 'logResponse']);

      scope = $rootScope.$new();
      scope.process = {};
      scope.entityType = 'process';
      controller = $controller('ProcessSummaryCtrl', {
        $scope: scope,
        Falcon: falconServiceMock,
        $state: {
          $current:{
            name: 'main.forms.feed.general'
          },
          go: angular.noop
        }
      });
    }));


    describe('saveEntity', function() {
      it('Should save the update the entity if in edit mode', function() {
        falconServiceMock.postUpdateEntity.andReturn(successResponse({}));
        scope.editingMode = true;//---this line doesnt work
        scope.$parent.cloningMode = false;
        scope.process = { name:  'ProcessOne'};
        scope.xml = '<process/>';

        scope.saveEntity();

        expect(scope.editingMode).toBe(false);
        expect(falconServiceMock.postSubmitEntity).not.toHaveBeenCalled();
        expect(falconServiceMock.postUpdateEntity).toHaveBeenCalledWith('<?xml version="1.0" encoding="UTF-8" standalone="yes"?><process/>', 'process', 'ProcessOne');
      });

      it('Should save the update the entity if in cloning mode', function() {
        falconServiceMock.postSubmitEntity.andReturn(successResponse({}));
        scope.cloningMode = true;//---this line doesnt work
        scope.$parent.cloningMode = true;
        scope.process = { name:  'ProcessOne'};
        scope.xml = '<process/>';

        scope.saveEntity();

        expect(scope.cloningMode).toBe(false);
        expect(falconServiceMock.postSubmitEntity).toHaveBeenCalledWith('<?xml version="1.0" encoding="UTF-8" standalone="yes"?><process/>', 'process');
        expect(falconServiceMock.postUpdateEntity).not.toHaveBeenCalled();
      });

    });

    function successResponse(value) {
      var fakePromise = {};
      fakePromise.success = function(callback) {
        callback(value);
        return fakePromise;
      };
      fakePromise.error = angular.noop;
      return fakePromise;
    }


  });

})();