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
  var falconServiceMock;

  describe('ProcessRootCtrl', function () {

    beforeEach(module('app.controllers.process','dateHelper','routeHelper'));

    beforeEach(inject(function($q, $rootScope, $controller, DateHelper, RouteHelper) {
      falconServiceMock = jasmine.createSpyObj('Falcon', ['postUpdateEntity', 'postSubmitEntity', 'logRequest', 'logResponse']);

      scope = $rootScope.$new();
      scope.models = {};
      scope.$parent = $rootScope.$new();
      scope.$parent.models = {};
      scope.process = {};
      scope.entityType = 'process';
      controllerProvider = $controller;
      entityFactoryMock = jasmine.createSpyObj('EntityFactory', ['newEntity']);
      serializerMock = jasmine.createSpyObj('EntitySerializer', ['preDeserialize']);

      controller = $controller('ProcessRootCtrl', {
        $scope: scope,
        $state: {
          current:{
            name: 'forms.process.general'
          },
          go: angular.noop
        },
        EntityFactory: entityFactoryMock,
        EntitySerializer: serializerMock,
        Falcon: falconServiceMock,
        ProcessModel : undefined
      });
    }));


    it('Should be of type process', function() {
      expect(scope.entityType).toBe('process');
    });

    describe('init', function() {
      it('Should be initialized properly', function() {
        scope.init();
        expect(scope.editXmlDisabled).toBe(true);
      });
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

      controller = createController({name: 'ProcessName'});
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
      controller = createController(undefined);
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
      controller = createController({name: 'ProcessName'});
      entityFactoryMock.newEntity.andReturn({});
      scope.models.processModel = {};

      scope.loadOrCreateEntity();

      expect(scope.$parent.models.processModel).toBe(null);
    });


    describe('saveEntity', function() {
      it('Should save the update the entity if in edit mode', function() {
        falconServiceMock.postUpdateEntity.andReturn(successResponse({}));
        scope.editingMode = true;//---this line doesnt work
        scope.$parent.cloningMode = false;
        scope.process = { name:  'ProcessOne'};
        scope.xml = '<process/>';

        scope.saveEntity();

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

  function createController(processModel) {
    return controllerProvider('ProcessRootCtrl', {
      $scope: scope,
      $state: {
        current:{
          name: 'forms.process.general'
        },
        go: angular.noop
      },
      EntityFactory: entityFactoryMock,
      EntitySerializer: serializerMock,
      ProcessModel : processModel
    });
  }


})();