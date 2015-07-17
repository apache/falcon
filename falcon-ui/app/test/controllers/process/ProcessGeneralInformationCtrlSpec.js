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


  describe('ProcessGeneralInformationCtrl', function () {
    beforeEach(module('app.controllers.process'));

    beforeEach(inject(function($q, $rootScope, $controller) {
      scope = $rootScope.$new();
      scope.entityType = 'process';
      scope.process = {
        tags: [{key: null, value: null}]
      };

      controller = $controller('ProcessGeneralInformationCtrl', {
        $scope: scope
      });
    }));


    it('Should add a new empty tag', function() {
      expect(scope.process.tags.length).toEqual(1);

      scope.addTag();

      expect(scope.process.tags.length).toEqual(2);
      expect(scope.process.tags[1]).toEqual({key: null, value: null});
    });

    it('Should remove a tag at the specified index', function() {
      scope.process.tags = [
        {key: 'key0', value: 'value0'},
        {key: 'key1', value: 'value1'},
        {key: 'key2', value: 'value2'}
      ];

      scope.removeTag(1);

      expect(scope.process.tags.length).toEqual(2);
      expect(scope.process.tags).toEqual([{key: 'key0', value: 'value0'}, {key: 'key2', value: 'value2'}]);
    });

    it('Should not delete if there is only one element', function() {
      scope.process.tags = [{key: 'key', value: 'value'}];

      scope.removeTag(0);

      expect(scope.process.tags).toEqual([{key: 'key', value: 'value'}]);
    });

    it('Should not delete if index is not passed in', function() {
      scope.process.tags = [
        {key: 'key0', value: 'value0'},
        {key: 'key1', value: 'value1'}
      ];

      scope.removeTag();

      expect(scope.process.tags).toEqual([{key: 'key0', value: 'value0'}, {key: 'key1', value: 'value1'}]);
    });
  })

  describe('Versions', function() {
    it('Should display the oozie versions when oozie workflow is selected', function() {
      var expectedVersions = ['4.2.0', '4.1.0', '4.0.1', '4.0.0'];
      scope.process.workflow = {engine: 'oozie'};

      scope.selectWorkflow();

      expect(scope.versions).toEqual(expectedVersions);
    });

    it('Should display the pig versions when pig workflow is selected', function() {
      var expectedVersions = ['pig-0.13.0', 'pig-0.12.1', 'pig-0.12.0', 'pig-0.11.1', 'pig-0.11.0', 'pig-0.10.1'];
      scope.process.workflow = {engine: 'pig'};

      scope.selectWorkflow();

      expect(scope.versions).toEqual(expectedVersions);
    });

    it('Should display the hive versions when hive workflow is selected', function() {
      var expectedVersions = ['hive-0.13.1', 'hive-0.13.0', 'hive-0.12.0', 'hive-0.11.0', 'hive-0.10.0'];
      scope.process.workflow = {engine: 'hive'};

      scope.selectWorkflow();

      expect(scope.versions).toEqual(expectedVersions);
    });
  });

})();