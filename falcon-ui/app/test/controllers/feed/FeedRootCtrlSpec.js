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

  describe('FeedRootCtrl', function () {
    var entityFactoryMock;
    var serializerMock;
    var controllerProvider;
    var falconServiceMock;

    beforeEach(module('app.controllers.feed', 'dateHelper'));

    beforeEach(inject(function($q, $rootScope, $controller, DateHelper) {
      scope = $rootScope.$new();
      scope.models = {};
      entityFactoryMock = jasmine.createSpyObj('EntityFactory', ['newEntity']);
      serializerMock = jasmine.createSpyObj('EntitySerializer', ['preDeserialize']);
      falconServiceMock = jasmine.createSpyObj('Falcon', ['postUpdateEntity', 'postSubmitEntity', 'logRequest', 'logResponse']);
      controllerProvider = $controller;

      controller = $controller('FeedController', {
        $scope: scope,
        $state: {
          $current:{
            name: 'main.forms.feed.general'
          },
          go: angular.noop
        },
        Falcon: falconServiceMock
      });
    }));

    it('Should be of type feed', function() {
      expect(scope.entityType).toBe('feed');
    });

    describe('init', function() {
      it('Should be initialized properly', function() {

        scope.init();

        expect(scope.feed.name).toBe("");
        expect(scope.feed.description).toBe(null);
        expect(scope.feed.groups).toBe(null);
      });
    });

    it('Should return true when the current state is the general view', function() {
      expect(scope.isActive('main.forms.feed.general')).toBe(true);
    });

    it('Should return true when the current state is not the general view', function() {
      expect(scope.isActive('main.forms.feed.location')).toBe(false);
    });


    describe('loadOrCreateEntity()', function() {
      it('Should deserialize the entity if the xml is found on the scope', function() {

        controller = createController();
        var createdFeed =  {};
        var deserialzedFeed =  {};
        var feedModel = {name: 'FeedName'};


        serializerMock.preDeserialize.andReturn(deserialzedFeed);
        entityFactoryMock.newEntity.andReturn(createdFeed);
        scope.models.feedModel = feedModel;

        var feed = scope.loadOrCreateEntity();

        expect(serializerMock.preDeserialize).toHaveBeenCalledWith(feedModel, 'feed');
        expect(feed).toNotBe(createdFeed);
        expect(feed).toBe(deserialzedFeed);
      });

      it('Should not deserialize the entity if the xml is not found on the scope', function() {
        controller = createController();
        var createdFeed =  {};
        var deserialzedFeed =  {};
        serializerMock.preDeserialize.andReturn(deserialzedFeed);
        entityFactoryMock.newEntity.andReturn(createdFeed);


        var feed = scope.loadOrCreateEntity();

        expect(serializerMock.preDeserialize).not.toHaveBeenCalled();
        expect(feed).toBe(createdFeed);
        expect(feed).toNotBe(deserialzedFeed);
      });

      it('Should clear the feedModel from the scope', function() {
        controller = createController();
        entityFactoryMock.newEntity.andReturn({});
        scope.models.feedModel = {};

        scope.loadOrCreateEntity();

        expect(scope.models.feedModel).toBe(null);
      });


    });

    describe('saveEntity', function() {
      it('Should save the update the entity if in edit mode', function() {
        falconServiceMock.postUpdateEntity.andReturn(successResponse({}));
        scope.editingMode = true;//i think this one should be deprecated, because it doesnt work in the real app, just in the tests
        scope.cloningMode = false;
        scope.feed = { name:  'FeedOne'};
        scope.xml = '<feed>' +
                      '<clusters>' +
                        '<cluster>' +
                          '<locations>' +
                            '<location type="data" /></locations>' +
                        '</cluster>' +
                      '</clusters>' +
                      '<locations><location /></locations>' +
                      '<properties><property></properties>' +
                    '</feed>';

        scope.saveEntity();

        expect(scope.editingMode).toBe(false);
        expect(falconServiceMock.postSubmitEntity).not.toHaveBeenCalled();
        expect(falconServiceMock.postUpdateEntity).toHaveBeenCalledWith('<?xml version="1.0" encoding="UTF-8" standalone="yes"?><feed><clusters><cluster></cluster></clusters></feed>', 'feed', 'FeedOne');
      });

      it('Should save the update the entity if in cloning mode', function() {
        falconServiceMock.postSubmitEntity.andReturn(successResponse({}));
        scope.cloningMode = true;//i think this one should be deprecated, because it doesnt work in the real app, just in the tests
        scope.feed = { name:  'FeedOne'};
        scope.xml = '<feed>' +
                      '<clusters>' +
                        '<cluster>' +
                          '<locations>' +
                            '<location type="data" /></locations>' +
                        '</cluster>' +
                      '</clusters>' +
                      '<locations><location /></locations>' +
                      '<properties><property></properties>' +
                    '</feed>';
        scope.$parent.cloningMode = true;

        scope.saveEntity();

        expect(scope.cloningMode).toBe(false);
        expect(falconServiceMock.postSubmitEntity).toHaveBeenCalledWith('<?xml version="1.0" encoding="UTF-8" standalone="yes"?><feed><clusters><cluster></cluster></clusters></feed>', 'feed');
        expect(falconServiceMock.postUpdateEntity).not.toHaveBeenCalled();
      });

    });

    function createController() {
      return controllerProvider('FeedController', {
        $scope: scope,
        $state: {
          $current:{
            name: 'main.forms.feed.general'
          },
          go: angular.noop
        },
        Falcon: {},
        EntityFactory: entityFactoryMock,
        EntitySerializer: serializerMock
      });
    }

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