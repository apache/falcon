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
  
  describe('tagFilter used in entities list', function () {
    beforeEach(module('app.directives'));
 
    it('should adds empty tags if not normalized', inject(function (tagFilterFilter) {
      expect(tagFilterFilter([{name:'ABCD'}])).toEqual([{name:'ABCD', list:{tag:[""]}}]);
      expect(tagFilterFilter([{name:'ABCD', list:{}}])).toEqual([{name:'ABCD', list:{tag:[""]}}]); 
      expect(tagFilterFilter([{name:'DEFG', list:[]}])).toEqual([{name:'DEFG', list:{tag:[""]}}]); 
    }));
    it('should not replace values if exists', inject(function (tagFilterFilter) {
      expect(tagFilterFilter([{name:'ABCD', list:{tag:["TEST"]}}])).not.toEqual([{name:'ABCD', list:{tag:[""]}}]);
      expect(tagFilterFilter([{name:'ABCD', list:{tag:["TEST"]}}])).toEqual([{name:'ABCD', list:{tag:["TEST"]}}]);
    }));
 
  });
  
  describe('Frequency Directive', function () {

    var element, scope, compile, falconServiceMock, entitiesListController;
    var windowMock, encoderServiceMock;

    beforeEach(module('app.directives'));


    beforeEach(inject(function($rootScope, $compile, $controller) {
      falconServiceMock = jasmine.createSpyObj('Falcon', ['getEntityDefinition', 'logRequest', 'logResponse']);
      encoderServiceMock = jasmine.createSpyObj('EncoderService', ['encode']);
      windowMock = createWindowMock();

      scope = $rootScope.$new();
      compile = $compile;

      entitiesListController = $controller('EntitiesListCtrl', {
        $scope: scope,
        Falcon: falconServiceMock,
        EncodeService: encoderServiceMock,
        $window: windowMock
      });

    }));
    
    it('Should render 2 hours', function() {
      scope.someFrequency = {unit: 'hours', quantity: 2};
      element = newElement('<frequency value="someFrequency" prefix="at"/>', scope);

      expect(element.text()).toBe('at 2 hours');
    });

    it('Should render "Not specified"', function() {
      scope.someFrequency = {unit: 'hours', quantity: null};
      element = newElement('<frequency value="someFrequency"/>', scope);

      expect(element.text()).toBe('Not specified');
    });

    describe('EntitiesListController', function() {
      it('Should invoke the entity definition service', function() {
        falconServiceMock.getEntityDefinition.andReturn(successResponse({}));
        var type = 'feed';
        var name = 'FeedOne';

        scope.downloadEntity(type, name);

        expect(falconServiceMock.getEntityDefinition).toHaveBeenCalledWith(type, name);
      });

      it('Should encode the response', function() {
        var type = 'feed';
        var name = 'FeedOne';
        falconServiceMock.getEntityDefinition.andReturn(successResponse({}));


        scope.downloadEntity(type, name);

        expect(encoderServiceMock.encode).toHaveBeenCalled();
      });

      it('Should do a full page reload to a data uri to trigger the download', function() {
        falconServiceMock.getEntityDefinition.andReturn(successResponse({}));
        encoderServiceMock.encode.andReturn('[encodedResponse]');
        windowMock.location.href = '';

        scope.downloadEntity('feed', 'FeedOne');

        expect(windowMock.location.href).toBe('data:application/octet-stream,[encodedResponse]');
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

    function newElement(html) {
      var element = compile(html)(scope);
      scope.$digest();
      return element;
    }

    function createWindowMock() {
     return {
       location: {
         href: ''
       }
     };
    }

  });

})();