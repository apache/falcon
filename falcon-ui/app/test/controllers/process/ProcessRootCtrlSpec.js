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
  var scope;
  var controller;
  var controllerProvider;
  var entityFactoryMock;
  var serializerMock;

  describe('ProcessRootCtrl', function () {

    beforeEach(module('app.controllers.process'));

    beforeEach(inject(function($q, $rootScope, $controller) {
      scope = $rootScope.$new();
      scope.models = {};
      controllerProvider = $controller;
      entityFactoryMock = jasmine.createSpyObj('EntityFactory', ['newEntity']);
      serializerMock = jasmine.createSpyObj('EntitySerializer', ['preDeserialize']);

      controller = $controller('ProcessRootCtrl', {
        $scope: scope,
        $state: {
          $current:{
            name: 'forms.process.general'
          },
          go: angular.noop
        },
        EntityFactory: entityFactoryMock,
        EntitySerializer: serializerMock
      });
    }));


    it('Should be of type process', function() {
      expect(scope.entityType).toBe('process');
    });

    it('Should be initialized properly', function() {
      scope.init();

      expect(scope.editXmlDisabled).toBe(true);
    });

    it('Should toggle editXmlDisable value to true', function() {
      scope.editXmlDisabled = false;

      scope.toggleEditXml();

      expect(scope.editXmlDisabled).toBe(true);
    });

    it('Should return true when the current state is the general view', function() {
      expect(scope.isActive('forms.process.general')).toBe(true);
    });

    it('Should return true when the current state is not the general view', function() {
      expect(scope.isActive('forms.process.properties')).toBe(false);
    });

    it('Should deserialize the entity if the xml is found on the scope', function() {

      controller = createController();
      var createdProcess =  {};
      var deserialzedProcess =  {};
      var processModel = {name: 'ProcessName'};
      serializerMock.preDeserialize.andReturn(deserialzedProcess);
      entityFactoryMock.newEntity.andReturn(createdProcess);
      scope.models.processModel = processModel;

      var process = scope.loadOrCreateEntity();

      expect(serializerMock.preDeserialize).toHaveBeenCalledWith(processModel, 'process');
      expect(process).toNotBe(createdProcess);
      expect(process).toBe(deserialzedProcess);
    });

    it('Should not deserialize the entity if the xml is not found on the scope', function() {
      controller = createController();
      var createdProcess =  {};
      var deserialzedProcess =  {};
      serializerMock.preDeserialize.andReturn(deserialzedProcess);
      entityFactoryMock.newEntity.andReturn(createdProcess);

      var process = scope.loadOrCreateEntity();

      expect(serializerMock.preDeserialize).not.toHaveBeenCalled();
      expect(process).toBe(createdProcess);
      expect(process).toNotBe(deserialzedProcess);
    });

    it('Should clear the processModel from the scope', function() {
      controller = createController();
      entityFactoryMock.newEntity.andReturn({});
      scope.models.processModel = {};

      scope.loadOrCreateEntity();

      expect(scope.models.processModel).toBe(null);
    });


  });

  function createController() {
    return controllerProvider('ProcessRootCtrl', {
      $scope: scope,
      $state: {
        $current:{
          name: 'forms.process.general'
        },
        go: angular.noop
      },
      EntityFactory: entityFactoryMock,
      EntitySerializer: serializerMock
    });
  }


})();