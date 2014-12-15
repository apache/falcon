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

  describe('HeaderController', function () {
    var controller,
        entityModel = {},
        scope;

    beforeEach(module('app.controllers.navHeader'));
    
    beforeEach(inject(function($rootScope, $controller) {

      scope = $rootScope.$new();
   
      controller = $controller('HeaderController', { 
        $scope: scope,
        EntityModel: entityModel,
        $state: {
          $current: {
            name: 'forms.feed.general'
          },
          go: angular.noop
        }
      });
      
    }));

    it('should reset EntityModel.clusterModel', function() {
      expect(entityModel).toEqual({});
      expect(entityModel.clusterModel).toBeUndefined();
      scope.resetCluster();
      expect(entityModel.clusterModel).not.toBeUndefined();
      expect(entityModel.clusterModel).toEqual(
        {cluster:{tags: "",interfaces:{interface:[
            {_type:"readonly",_endpoint:"hftp://sandbox.hortonworks.com:50070",_version:"2.2.0"},
            {_type:"write",_endpoint:"hdfs://sandbox.hortonworks.com:8020",_version:"2.2.0"},
            {_type:"execute",_endpoint:"sandbox.hortonworks.com:8050",_version:"2.2.0"},
            {_type:"workflow",_endpoint:"http://sandbox.hortonworks.com:11000/oozie/",_version:"4.0.0"},
            {_type:"messaging",_endpoint:"tcp://sandbox.hortonworks.com:61616?daemon=true",_version:"5.1.6"}
          ]},locations:{location:[{_name: "staging", _path: ""},{_name: "temp", _path: ""},{_name: "working", _path: ""}]},
          ACL: {_owner: "",_group: "",_permission: ""},properties: {property: [{ _name: "", _value: ""}]},
          _xmlns:"uri:falcon:cluster:0.1",_name:"",_description:"",_colo:""}
        }
      );
    });
    
  });
  
})();