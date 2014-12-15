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

  describe('Entity Summary Directive', function () {

    var scope, compile, controller;

    beforeEach(module('app.directives.entity'));

    beforeEach(inject(function($rootScope, $compile, $controller) {
      scope = $rootScope.$new();
      compile = $compile;
      scope.entities = [{"type":"feed","name":"feedOne","status":"SUBMITTED"},{"type":"feed","name":"feedTwo","status":"SUBMITTED"},{"type":"feed","name":"feedThree","status":"SUBMITTED"}, {"type":"FEED","name":"rawEmailFeed","status":"RUNNING","list":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},{"type":"FEED","name":"cleansedEmailFeed","status":"RUNNING","list":{"tag":["owner=USMarketing","classification=Secure","externalSource=USProdEmailServers","externalTarget=BITools"]}}];
      controller = $controller('EntitySummaryCtrl', {
        $scope: scope 
      });
    }));

    describe('EntitySummaryCtrl', function() {
    
      it('Should determine correct list of status', function() {

        scope.calculateAmount();
        expect(scope.entities.length).toEqual(5);       
        expect(scope.statusCount).toEqual({ SUBMITTED : 3, RUNNING : 2, SUSPENDED : 0, UNKNOWN : 0, TOTAL_AMOUNT : scope.entities.length});
        
        scope.entities = [{"type":"feed","name":"feedOne","status":"SUSPENDED"},{"type":"feed","name":"feedTwo","status":"SUSPENDED"},{"type":"feed","name":"feedThree","status":"SUBMITTED"}, {"type":"FEED","name":"rawEmailFeed","status":"RUNNING","list":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},{"type":"FEED","name":"cleansedEmailFeed","status":"RUNNING","list":{"tag":["owner=USMarketing","classification=Secure","externalSource=USProdEmailServers","externalTarget=BITools"]}}];
        scope.calculateAmount();      
        expect(scope.statusCount).toEqual({ SUBMITTED : 1, RUNNING : 2, SUSPENDED : 2, UNKNOWN : 0, TOTAL_AMOUNT : scope.entities.length});

        scope.entities = [{"type":"feed","name":"feedOne","status":"NEWSTATUS"},{"type":"feed","name":"feedTwo","status":"NEWSTATUS"},{"type":"feed","name":"feedThree","status":"SUBMITTED"}, {"type":"FEED","name":"rawEmailFeed","status":"RUNNING","list":{"tag":["externalSystem=USWestEmailServers","classification=secure"]}},{"type":"FEED","name":"cleansedEmailFeed","status":"ANOTHERNEWSTATUS","list":{"tag":["owner=USMarketing","classification=Secure","externalSource=USProdEmailServers","externalTarget=BITools"]}}];
        scope.calculateAmount();       
        expect(scope.statusCount).toEqual({ SUBMITTED : 1, RUNNING : 1, SUSPENDED : 0, UNKNOWN : 0, TOTAL_AMOUNT : scope.entities.length, NEWSTATUS:2, ANOTHERNEWSTATUS: 1});
      
      });
      it('Should return empty default list of status', function() {
        
        scope.entities = [];     
        scope.calculateAmount();
        expect(scope.entities.length).toEqual(0);       
        expect(scope.statusCount).toEqual({ SUBMITTED : 0, RUNNING : 0, SUSPENDED : 0, UNKNOWN : 0, TOTAL_AMOUNT : scope.entities.length});
        
        scope.entities = "";     
        scope.calculateAmount();
        expect(scope.entities.length).toEqual(0);       
        expect(scope.statusCount).toEqual({ SUBMITTED : 0, RUNNING : 0, SUSPENDED : 0, UNKNOWN : 0, TOTAL_AMOUNT : 0});
      
      });
      it('Should return empty default but one in total itemas in entities.length', function() {
        
        scope.entities = [{}];     
        scope.calculateAmount(); 
        expect(scope.statusCount).toEqual({ SUBMITTED : 0, RUNNING : 0, SUSPENDED : 0, UNKNOWN : 0, TOTAL_AMOUNT : 1});
      
      });
      it('Should ignore undefined from adding it to statuses but allow the rest', function() {
        
        scope.entities = [{"type":"feed"},{"type":"feed","status":"NEWSTATUS"}, {"type":"feed","status":"RUNNING"}];     
        scope.calculateAmount();
        expect(scope.entities.length).toEqual(3);       
        expect(scope.statusCount).toEqual({ SUBMITTED : 0, RUNNING : 1, SUSPENDED : 0, UNKNOWN : 0, TOTAL_AMOUNT : 3, NEWSTATUS: 1});  
        
        scope.entities = [{"type":["feed"]},{"type":"feed","name":"feedOne","status":"UNKNOWN"},{"type":"feed","name":"feedTwo","status":"TESTSTATUS"}];     
        scope.calculateAmount(); 
        expect(scope.statusCount).toEqual({ SUBMITTED : 0, RUNNING : 0, SUSPENDED : 0, UNKNOWN : 1, TOTAL_AMOUNT : 3, TESTSTATUS: 1});
      
      });
    });

  });
})();