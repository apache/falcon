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

  describe('EntityModel', function () {

    var EntityModel, httpBackend, X2jsServiceMock;

    beforeEach(module('app.services.entity.model', function($provide) {
      X2jsServiceMock = jasmine.createSpyObj('X2jsService', ['xml_str2json']);
      $provide.value('X2jsService', X2jsServiceMock);
    }));

    beforeEach(inject(function($httpBackend, _EntityModel_) {
      EntityModel = _EntityModel_;
      httpBackend = $httpBackend;
    }));


    it('Should set type as not recognized if the entity is not feed, cluster or process', function() {
      EntityModel.identifyType({});

      expect(EntityModel.type).toBe('Type not recognized');
    });


    it('Should contain the proper Feed Model', function() {
      var feed = EntityModel.feedModel.feed;

      expect(feed).toNotBe(null);
      expect(feed._name).toEqual("");
    });

  });
})();