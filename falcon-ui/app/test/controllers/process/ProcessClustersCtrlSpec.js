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

  describe('ProcessClustersCtrl', function () {
    beforeEach(module('app.controllers.process'));

    beforeEach(inject(function($q, $rootScope, $controller) {
      scope = $rootScope.$new();
      scope.process = {};
      controller = $controller('ProcessClustersCtrl', {
        $scope: scope,
        clustersList: []
      });
    }));

    describe('init', function() {
      it('Should add date format', function() {
        scope.init();

        expect(scope.dateFormat).toBe('dd-MMMM-yyyy');
      });
    });

    describe('openDatePicker', function() {
      it('Should set the container opened property to true', function() {
        var eventMock = {
          preventDefault: angular.noop,
          stopPropagation: angular.noop
        };
        var container = {
          opened: false
        };

        scope.openDatePicker(eventMock, container);

        expect(scope.dateFormat).toBe('dd-MMMM-yyyy');
      });
    });

    describe('addCluster', function() {

      it('Should add a new empty cluster', function() {
        scope.process.clusters = [{}];

        scope.addCluster();

        expect(scope.process.clusters.length).toEqual(2);
      });
    });

    describe('removeCluster', function() {
      it('Should remove a cluster at the specified index', function() {
        scope.process.clusters = [
          {name: 'cluster1'},
          {name: 'cluster2'},
          {name: 'cluster3'}
        ];

        scope.removeCluster(1);

        expect(scope.process.clusters.length).toEqual(2);
        expect(scope.process.clusters).toEqual([{name: 'cluster1'}, {name: 'cluster3'}]);
      });

      it('Should not delete if there is only one element', function() {
        scope.process.clusters = [{name: 'cluster1'}];

        scope.removeCluster(0);

        expect(scope.process.clusters).toEqual([{name: 'cluster1'}]);
      });

      it('Should not delete if index is not passed in', function() {
        scope.process.clusters= [
          {name: 'cluster1'},
          {name: 'cluster2'}
        ];

        scope.removeCluster();

        expect(scope.process.clusters).toEqual([{name: 'cluster1'}, {name: 'cluster2'}]);
      });

    });

  });

})();