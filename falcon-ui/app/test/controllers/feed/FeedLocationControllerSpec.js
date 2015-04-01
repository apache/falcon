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

  describe('FeedLocationController', function () {
    beforeEach(module('app.controllers.feed'));

    beforeEach(inject(function($q, $rootScope, $controller) {
      scope = $rootScope.$new();
      scope.feed = {
       storage: {
         fileSystem: {
           active: true,
           locations: [
             {type: 'data', path: '/'},
             {type: 'stats', path: '/'},
             {type: 'meta', path: '/'}
           ]
         },
         catalog: {
           active: false,
           catalogTable: {
             uri: null
           }
         }
       }
      };

      controller = $controller('FeedLocationController', {
        $scope: scope,
        $state: {}
      });
    }));

    it('Should toggle the default selected storage from file system to catalog', function() {
      scope.toggleStorage();

      expect(scope.feed.storage.catalog.active).toBe(true);
      expect(scope.feed.storage.fileSystem.active).toBe(false);
    });

  });

})();