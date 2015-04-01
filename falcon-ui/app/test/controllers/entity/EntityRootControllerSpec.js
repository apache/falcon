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

  describe('EntityRootCtrl', function () {

    beforeEach(module('app.controllers.entity'));

    beforeEach(inject(function($q, $rootScope, $controller) {
      scope = $rootScope.$new();
      scope.models = {};
      controllerProvider = $controller;
      entityFactoryMock = jasmine.createSpyObj('EntityFactory', ['newEntity']);
      serializerMock = jasmine.createSpyObj('EntitySerializer', ['preDeserialize']);


      controller = $controller('EntityRootCtrl', {
        $scope: scope,
        $state: {
          $current: {
            name: 'forms.feed.general'
          },
          go: angular.noop
        }
      });
    }));


    describe('toggleEditXml', function() {
      it('Should toggle editXmlDisable value to true', function() {
        scope.editXmlDisabled = false;

        scope.toggleEditXml();

        expect(scope.editXmlDisabled).toBe(true);
      });
    });


    describe('baseInit', function() {
      it('Should be initialized properly', function() {
        scope.baseInit();

        expect(scope.editXmlDisabled).toBe(true);
      });
    });


    it('Should capitalize properly', function() {
      expect(scope.capitalize('hello')).toBe('Hello');
    });


    describe('cancel', function() {
      it('Should clear the feed from the scope when cancelling', function() {
        scope.entityType = 'feed';
        scope.feed = {};

        scope.cancel();

        expect(scope.feed).toBe(null);
      });

    });


  });

})();