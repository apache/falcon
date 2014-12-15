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

  describe('FeedSummaryController', function () {
    beforeEach(module('app.controllers.feed'));

    beforeEach(inject(function($q, $rootScope, $controller) {
      scope = $rootScope.$new();
      scope.feed = {};

      controller = $controller('FeedSummaryController', {
        $scope: scope,
        $state: {}
      });
    }));


    it('Should return false when there are no tags on the feed', function() {
      scope.feed.tags = [];

      expect(scope.hasTags()).toBe(false);
    });

    it('Should return false when there are with empty keys on the feed', function() {
      scope.feed.tags = [{key: null, value: null}];

      expect(scope.hasTags()).toBe(false);
    });


    it('Should return "Not specified if not present" if empty string', function() {
      expect(scope.optional('')).toBe('Not specified');
    });

    it('Should return "Not specified if not present" if empty string', function() {
      expect(scope.optional()).toBe('Not specified');
    });

    it('Should return the specified value if the expression is true', function() {
      expect(scope.optional(true, 'Up 2 hours')).toBe('Up 2 hours');
    });

    it('Should return "Not specified if the expression is false', function() {
      expect(scope.optional(false, 'Up 2 hours')).toBe('Not specified');
    });

  });

})();