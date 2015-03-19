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

  describe('Falcon', function () {
    var httpBackendMock;
    var service;
    var cookiesMock;

    beforeEach(module('ngCookies', 'app.services.falcon'));

    beforeEach(inject(function($httpBackend, Falcon) {
      httpBackendMock = $httpBackend;
      service = Falcon;      
    }));

    describe('initialize', function() {
      it('Should set the object Falcon.responses', function() {
        expect(service.responses).toEqual({
          display : true,
          queue:[], 
          count: {pending: 0, success:0, error:0},
          multiRequest: {cluster:0, feed:0, process:0},
          listLoaded: {cluster:false, feed:false, process:false}   
        });
      });

    });
    describe('.logRequest()', function() {
      it('Should log the pending request', function() {
        service.logRequest();
        expect(service.responses.count.pending).toEqual(1);
        service.logRequest();
        service.logRequest();
        service.logRequest();
        expect(service.responses.count.pending).toEqual(4);  
      });

      it('Should throw an error when the category does not exist', function() {
        
      });

    });
    describe('Falcon.logResponse(type, messageObject, hide)', function() {
      it('Should log resolve pending request', function() {
        var responseOne = {"status":"SUCCEEDED","message":"default/TEST2(FEED) suspended successfully\n","requestId":"default/b3a31c93-23e0-450d-bb46-b3e1be0525ff\n"};
        service.logRequest();
        service.logRequest();
        service.logRequest();
        expect(service.responses.count.pending).toEqual(3);
        service.logResponse('success', responseOne, "cluster");
        expect(service.responses.count.success).toEqual(1);
        expect(service.responses.count.pending).toEqual(2);
        
        service.logResponse('success', responseOne, "cluster");
        service.logResponse('success', responseOne, "cluster");
        expect(service.responses.multiRequest.cluster).toEqual(-3);
      });
   });
  });
})();