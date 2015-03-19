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

  describe('FeedPropertiesController', function () {
    beforeEach(module('app.controllers.feed'));

    beforeEach(inject(function($q, $rootScope, $controller) {
      scope = $rootScope.$new();
      scope.feed = {
        customProperties: [{key: null, value: null}]
      };

      controller = $controller('FeedPropertiesController', {
        $scope: scope,
        $state: {}
      });
    }));

    it('Should add a new empty property', function() {
      expect(scope.feed.customProperties.length).toEqual(1);

      scope.addCustomProperty();

      expect(scope.feed.customProperties.length).toEqual(2);
      expect(scope.feed.customProperties[1]).toEqual({key: null, value: null});
    });

    it('Should remove a property at the specified index', function() {
      scope.feed.customProperties = [
        {key: 'key0', value: 'value0'},
        {key: 'key1', value: 'value1'},
        {key: 'key2', value: 'value2'}
      ];

      scope.removeCustomProperty(1);

      expect(scope.feed.customProperties.length).toEqual(2);
      expect(scope.feed.customProperties).toEqual([{key: 'key0', value: 'value0'}, {key: 'key2', value: 'value2'}]);
    });

    it('Should not delete if there is only one element', function() {
      scope.feed.customProperties = [{key: 'key', value: 'value'}];

      scope.removeCustomProperty(0);

      expect(scope.feed.customProperties).toEqual([{key: 'key', value: 'value'}]);
    });

    it('Should not delete if index is not passed in', function() {
      scope.feed.customProperties = [
        {key: 'key0', value: 'value0'},
        {key: 'key1', value: 'value1'}
      ];

      scope.removeCustomProperty();

      expect(scope.feed.customProperties).toEqual([{key: 'key0', value: 'value0'}, {key: 'key1', value: 'value1'}]);
    });

  });

})();