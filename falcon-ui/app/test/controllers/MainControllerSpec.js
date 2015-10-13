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
  var q;
  var scope;
  var controller;
  var falconServiceMock = jasmine.createSpyObj('Falcon', ['getEntities', 'getEntityDefinition', 'logResponse', 'logRequest']);
  var x2jsServiceMock = jasmine.createSpyObj('X2jsService', ['xml_str2json']);
  var stateMock = jasmine.createSpyObj('state', ['go']);
  var entityModel = {};


  describe('MainController', function () {

    beforeEach(module('app.controllers'));

    beforeEach(inject(function($q, $rootScope, $controller) {
      q = $q;

      var promise = {};
      promise.success = function() {return {error: function() {}}};


      scope = $rootScope.$new();
      scope.$parent.refreshLists = angular.noop;
      scope.models = {};
      falconServiceMock.getEntities.andReturn(successResponse({}));

      controller = $controller('DashboardCtrl', {
        $scope: scope,
        $timeout: {},
        Falcon: falconServiceMock,
        FileApi: {},
        EntityModel: entityModel,
        $state: stateMock,
        X2jsService: x2jsServiceMock
      });
    }));



    //describe('editEntity', function() {
    //
    //  it('Should invoke the Falcon.getEntityDefinition', function() {
    //    falconServiceMock.getEntityDefinition.andReturn(successResponse({}));
    //
    //    scope.editEntity('feed', 'myFeed');
    //
    //    expect(falconServiceMock.getEntityDefinition).toHaveBeenCalled();
    //  });
    //
    //  describe('call to the api was successful', function() {
    //    it('Should set the retrieved entity from the server into EntityModel', function () {
    //      var myFeed = {};
    //      falconServiceMock.getEntityDefinition.andReturn(successResponse({}));
    //      x2jsServiceMock.xml_str2json.andReturn(myFeed);
    //
    //      scope.editEntity('feed', 'myFeed');
    //
    //      expect(entityModel.feedModel).toBe(myFeed);
    //    });
    //
    //    it('Should set editing mode to true', function () {
    //      falconServiceMock.getEntityDefinition.andReturn(successResponse({}));
    //      scope.editingMode = false;
    //
    //      scope.editEntity('feed', 'myFeed');
    //
    //      expect(scope.editingMode).toBe(true);
    //    });
    //
    //    it('Should navigate to the appropriate landing page for the entity type', function () {
    //      falconServiceMock.getEntityDefinition.andReturn(successResponse());
    //      scope.editingMode = false;
    //
    //      scope.editEntity('feed', 'myFeed');
    //
    //      expect(stateMock.go).toHaveBeenCalledWith('forms.feed.general');
    //    });
    //
    //    it('Should set a copy of the model into the scope', function () {
    //      var feedModel = {name: 'MyFeed'};
    //      falconServiceMock.getEntityDefinition.andReturn(successResponse());
    //      x2jsServiceMock.xml_str2json.andReturn(feedModel);
    //
    //      scope.editEntity('feed', 'myFeed');
    //
    //      expect(scope.models.feedModel).toNotBe(feedModel);
    //      expect(scope.models.feedModel).toEqual(feedModel);
    //    });
    //  });
    //
    //  xdescribe('call to the api errored out', function() {
    //    it('Should set the retrieved entity from the server into EntityModel', function () {
    //      var error = {result: 'error message'};
    //      falconServiceMock.success = true;
    //      falconServiceMock.getEntityDefinition.andReturn(errorResponse());
    //      x2jsServiceMock.xml_str2json.andReturn(error);
    //
    //      scope.editEntity('feed', 'myFeed');
    //
    //      expect(falconServiceMock.success).toBe(false);
    //      expect(falconServiceMock.serverResponse).toBe('error message');
    //    });
    //
    //  });
    //});
    //
    //describe('clone entity', function() {
    //  it('Should invoke the Falcon.getEntityDefinition', function() {
    //    var myFeed = {feed: {}};
    //    falconServiceMock.getEntityDefinition.andReturn(successResponse({}));
    //    x2jsServiceMock.xml_str2json.andReturn(myFeed);
    //
    //    scope.cloneEntity('feed', 'myFeed');
    //
    //    expect(falconServiceMock.getEntityDefinition).toHaveBeenCalled();
    //  });
    //
    //  describe('call to the api was successful', function() {
    //    it('Should set the retrieved entity from the server into EntityModel', function () {
    //      var myFeed = {feed: {}};
    //      falconServiceMock.getEntityDefinition.andReturn(successResponse({}));
    //      x2jsServiceMock.xml_str2json.andReturn(myFeed);
    //
    //      scope.cloneEntity('feed', 'myFeed');
    //
    //      expect(entityModel.feedModel).toBe(myFeed);
    //    });
    //
    //    it('Should set clone mode to true', function () {
    //      falconServiceMock.getEntityDefinition.andReturn(successResponse({}));
    //      scope.cloningMode = false;
    //
    //      scope.cloneEntity('feed', 'myFeed');
    //
    //      expect(scope.cloningMode).toBe(true);
    //    });
    //
    //    it('Should navigate to the appropriate landing page for the entity type', function () {
    //      falconServiceMock.getEntityDefinition.andReturn(successResponse());
    //      scope.cloningMode = false;
    //
    //      scope.cloneEntity('feed', 'myFeed');
    //
    //      expect(stateMock.go).toHaveBeenCalledWith('forms.feed.general');
    //    });
    //
    //    it('Should set a copy of the model into the scope', function () {
    //      var feedModel = {feed: {name: 'MyFeed'}};
    //      falconServiceMock.getEntityDefinition.andReturn(successResponse());
    //      x2jsServiceMock.xml_str2json.andReturn(feedModel);
    //
    //      scope.cloneEntity('feed', 'myFeed');
    //
    //      expect(scope.models.feedModel).toNotBe(feedModel);
    //      expect(scope.models.feedModel).toEqual(feedModel);
    //    });
    //  });
    //});


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

  function errorResponse(value) {
    var fakePromise = {};
    fakePromise.success = function() {
      return fakePromise;
    };
    fakePromise.error = function(callback) {
      callback(value);
      return fakePromise;
    };
    return fakePromise;
  }

})();