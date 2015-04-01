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
  var q,
      scope,
      controller,
      falconServiceMock = jasmine.createSpyObj('Falcon', ['getEntities', 'getEntityDefinition']),
      x2jsServiceMock = jasmine.createSpyObj('X2jsService', ['xml_str2json', 'json2xml_str']),
      stateMock = jasmine.createSpyObj('state', ['go']),
      entityModelArrangeMock = jasmine.createSpyObj('EntityModel', ['arrangeFieldsOrder']),
      entityModel = {clusterModel : 
        {cluster:{tags: "",interfaces:{interface:[
            {_type:"readonly",_endpoint:"hftp://sandbox.hortonworks.com:50070",_version:"2.2.0"},
            {_type:"write",_endpoint:"hdfs://sandbox.hortonworks.com:8020",_version:"2.2.0"},
            {_type:"execute",_endpoint:"sandbox.hortonworks.com:8050",_version:"2.2.0"},
            {_type:"workflow",_endpoint:"http://sandbox.hortonworks.com:11000/oozie/",_version:"4.0.0"},
            {_type:"messaging",_endpoint:"tcp://sandbox.hortonworks.com:61616?daemon=true",_version:"5.1.6"}
          ]},locations:{location:[{_name: "staging", _path: ""},{_name: "temp", _path: ""},{_name: "working", _path: ""}]},
          ACL: {_owner: "",_group: "",_permission: ""},properties: {property: [{ _name: "", _value: ""}]},
          _xmlns:"uri:falcon:cluster:0.1",_name:"",_description:"",_colo:""},
      }},
      validationService,
      backupRegistryObject;

  describe('ClusterFormCtrl', function () {

    beforeEach(function () {
      module('app.controllers.cluster');
      module('app.services.validation');
    });

    beforeEach(inject(function($q, $rootScope, $controller, ValidationService) {
      q = $q;
      validationService = ValidationService;
      var promise = {};
      promise.success = function() {return {error: function() {}}};

      scope = $rootScope.$new();
   
      controller = $controller('ClusterFormCtrl', { 
        $scope: scope,
        Falcon: falconServiceMock,
        EntityModel: entityModel,
        $state: stateMock,
        X2jsService: x2jsServiceMock,
        validationService:ValidationService
      });
      //
    }));

    describe('initialize', function() {       
      it('Should initialize $scope variables', function() {
        scope.clusterEntity.cluster = scope.clusterEntity.clusterModel.cluster;      
        expect(scope.clusterEntity.clusterModel).toBeDefined();       
        expect(scope.clusterEntity.clusterModel.cluster.tags).toEqual("");        
        expect(scope.clusterEntity.clusterModel.cluster).toEqual(scope.clusterEntity.clusterModel.cluster);        
        expect(scope.secondStep).toEqual(false);
        expect(scope.clusterEntity.clusterModel.cluster.properties.property).toEqual([{ _name: "", _value: ""}]);
        
        expect(scope.registry).toEqual({ check: false });
        expect(scope.registry).toEqual({ check: false });
      });      
    });
    describe('tags', function() {      
      describe('$scope.addTag', function() {       
        it('should init with one empty tag in tagsArray', function() {              
          expect(scope.tagsArray.length).toEqual(1);         
          expect(scope.tagsArray).toEqual([{key: null, value: null}]);             
          scope.addTag();
          expect(scope.tagsArray.length).toEqual(2);
          expect(scope.tagsArray).toEqual([{key: null, value: null}, {key: null, value: null}]);             
        });
           
      });
      describe('$scope.convertTags', function() {       
        it('should convert correctly each pair of tags on each add', function() { 
          scope.tagsArray =[{key: 'something', value: 'here'}, {key: 'another', value: 'here'}];      
          scope.convertTags();
          expect(scope.clusterEntity.clusterModel.cluster.tags).toEqual("something=here,another=here");       
          scope.tagsArray =[{key: 'something', value: 'here'}, {key: 'another', value: 'here'}, {key: 'third', value: 'tag'}]; 
          scope.convertTags(); 
          expect(scope.clusterEntity.clusterModel.cluster.tags).toEqual("something=here,another=here,third=tag");           
        });        
      });
      describe('$scope.splitTags', function() {       
        it('should split correctly the string in pair of tags', function() { 
          scope.clusterEntity.clusterModel.cluster.tags = 'some=tag';         
          scope.splitTags();
          expect(scope.tagsArray).toEqual([{key: 'some', value: 'tag'}]); 
          
          scope.clusterEntity.clusterModel.cluster.tags = 'some=tag,another=tag,third=value';         
          scope.splitTags();
          expect(scope.tagsArray).toEqual([{key: 'some', value: 'tag'},{key: 'another', value: 'tag'},{key: 'third', value: 'value'}]);                        
        });        
      });
      describe('scope.removeTags', function() {       
        it('should ignore if empty or if undefined, string or null also if index doesnt exists in array', function() {            
          scope.tagsArray = [{key: "first", value: "value"}, {key: "second", value: "value"}];                          
          scope.removeTag();
          scope.removeTag("string");
          scope.removeTag(null);
          scope.removeTag(undefined);
          scope.removeTag(10);
          scope.removeTag(4);
          scope.removeTag(100);
          expect(scope.tagsArray).toEqual([{key: "first", value: "value"}, {key: "second", value: "value"}]);                   
        });
        it('should remove correct tags by index', function() {              
          scope.tagsArray = [{key: "first", value: "value"}, {key: "second", value: "value"}, {key: "third", value: "value"}, {key: "fourth", value: "value"}];                
          scope.removeTag(1);
          expect(scope.tagsArray).toEqual([{key: "first", value: "value"}, {key: "third", value: "value"}, {key: "fourth", value: "value"}]);          
          scope.removeTag(2);
          expect(scope.tagsArray).toEqual([{key: "first", value: "value"}, {key: "third", value: "value"}]);
          scope.removeTag(0);
          expect(scope.tagsArray).toEqual([{key: "third", value: "value"}]);       
        });   
      });      
    });
    describe('locations', function() { 
      describe('initialization', function() {       
        it('should init with default locations and correct values', function() {           

          expect(scope.clusterEntity.clusterModel.cluster.locations.location).toEqual(
            [{ _name : 'staging', _path : '' }, { _name : 'temp', _path : '' }, { _name : 'working', _path : '' }, { _name : '', _path : '' }]
          );
        });   
      });    
      describe('$scope.addLocation', function() {       
        it('$scope.addLocation should add locations', function() {       
          scope.clusterEntity.clusterModel.cluster.locations.location = [{ _name : 'staging', _path : '' }, { _name : 'temp', _path : '' }, { _name : 'working', _path : '' }, { _name : 'something', _path : 'here' }];
          
          scope.addLocation();
          expect(scope.clusterEntity.clusterModel.cluster.locations.location).toEqual([ 
            { _name : 'staging', _path : '' }, { _name : 'temp', _path : '' }, 
            { _name : 'working', _path : '' }, {_name:"something", _path: "here"}, {_name:"", _path: ""}]);
        });  
        it('$scope.addLocation should ignore if _name or _location in newLocation are empty', function() {       
          scope.clusterEntity.clusterModel.cluster.locations.location = [{ _name : 'staging', _path : '' }, { _name : 'temp', _path : '' }, { _name : 'working', _path : '' }, { _name : 'something', _path : 'here' }, {_name:"", _path: ""}];         
          scope.addLocation();
          expect(scope.clusterEntity.clusterModel.cluster.locations.location).toEqual([ 
            { _name : 'staging', _path : '' }, { _name : 'temp', _path : '' }, 
            { _name : 'working', _path : '' }, {_name:"something", _path: "here"}, {_name:"", _path: ""}]);
          
          scope.clusterEntity.clusterModel.cluster.locations.location = [{ _name : 'staging', _path : '' }, { _name : 'temp', _path : '' }, { _name : 'working', _path : '' }, { _name : 'something', _path : 'here' }, {_name:"noPath", _path: ""}];
          scope.addLocation();
          expect(scope.clusterEntity.clusterModel.cluster.locations.location).toEqual([ 
            { _name : 'staging', _path : '' }, { _name : 'temp', _path : '' }, 
            { _name : 'working', _path : '' }, {_name:"something", _path: "here"}, {_name:"noPath", _path: ""}]);     
        });    
      });      
      describe('$scope.removeLocation', function() {       
        it('$scope.removeLocation should remove locations', function() {   
          scope.clusterEntity.clusterModel.cluster.locations.location = [
            { _name : 'staging', _path : '' }, { _name : 'temp', _path : '' }, 
            { _name : 'working', _path : '' }, { _name : 'something', _path : 'here' }, {_name:"noPath", _path: ""}
          ];
              
          scope.removeLocation(3);                  
          expect(scope.clusterEntity.clusterModel.cluster.locations.location).toEqual([ 
            { _name : 'staging', _path : '' }, { _name : 'temp', _path : '' }, 
            { _name : 'working', _path : '' }, {_name:"noPath", _path: ""}]);     
        });   
        it('$scope.removeLocation should not remove if empty or default values', function() {       
          //default values cant be removed as the delete button doesnt appears if one of them due to ng-if in template, so no testing here
          scope.removeLocation();
          scope.removeLocation("string");
          scope.removeLocation(null);
          scope.removeLocation(undefined);
          scope.removeLocation(10);
          scope.removeLocation(4);          
          expect(scope.clusterEntity.clusterModel.cluster.locations.location).toEqual([ 
            { _name : 'staging', _path : '' }, { _name : 'temp', _path : '' }, 
            { _name : 'working', _path : '' }, {_name:"noPath", _path: ""}]);      
        });    
      });     
    });  
    describe('properties', function() {  
      describe('initialization', function() {       
        it('should init with default properties and correct values', function() {    
          expect(scope.clusterEntity.clusterModel.cluster.properties.property).toNotBe(undefined);       
          expect(scope.clusterEntity.clusterModel.cluster.properties.property[0]).toEqual({ _name: "", _value: ""});         
        });   
      });   
      describe('$scope.addProperty', function() {      
        it('$scope.addProperty should not add if values are empty or are not valid', function() {
          scope.clusterEntity.clusterModel.cluster.properties.property = [{ _name: "", _value: ""}];
          scope.addProperty();         
          scope.clusterEntity.clusterModel.cluster.properties.property = [{ _name: "something", _value: ""}];
          scope.addProperty();        
          scope.clusterEntity.clusterModel.cluster.properties.property = [{ _name: "", _value: "something"}];
          scope.addProperty();      
          scope.clusterEntity.clusterModel.cluster.properties.property = [{ _name: null, _value: "something"}];
          scope.addProperty(); 
          scope.clusterEntity.clusterModel.cluster.properties.property = [{ _name: "something", _value: undefined}];
          scope.addProperty();       
          expect(scope.clusterEntity.clusterModel.cluster.properties.property.length).toEqual(1);
        });         
        it('$scope.addProperty should add correct values', function() { 
          scope.clusterEntity.clusterModel.cluster.properties.property = [{ _name: "name1", _value: "value1"}];      
   
          scope.addProperty();            
                   
          expect(scope.clusterEntity.clusterModel.cluster.properties.property).toEqual([{ _name: "name1", _value: "value1"}, { _name: "", _value: ""}]);
        });   
      });   
      describe('$scope.removeProperty', function() {      
        it('should not remove if called with invalid arguments', function() {
          scope.removeProperty();
          scope.removeProperty(-10);
          scope.removeProperty(5);
          scope.removeProperty(1543);
          scope.removeProperty("string");
          scope.removeProperty(null);
          scope.removeProperty(undefined);
          expect(scope.clusterEntity.clusterModel.cluster.properties.property).toEqual([{ _name: "name1", _value: "value1"}, { _name: "", _value: ""}]);     
        });         
        it('should remove correct values', function() {       
           scope.removeProperty(1);
           expect(scope.clusterEntity.clusterModel.cluster.properties.property).toEqual([{ _name: "name1", _value: "value1"}]);
           
        });   
      });     
    });     
    describe('goSummaryStep', function() {

      describe('$scope.goSummaryStep', function() {

        it('should activate second step flag', function() {
          scope.validations = validationService;
          scope.goSummaryStep(); 
          expect(scope.secondStep).toBe(true);
        }); 
        it('should not call x2jsService yet', function() {
          scope.validations = validationService;
          scope.goSummaryStep(); 
          expect(x2jsServiceMock.json2xml_str).not.toHaveBeenCalled(); 
        }); 
      });
      describe('private cleanModel()', function() {
        it('should delete tags if empty and leave them if not', function() {
          scope.validations = validationService;
          scope.clusterEntity.clusterModel.cluster.tags = "";    
          expect(scope.clusterEntity.clusterModel.cluster.tags).toEqual("");                 
          scope.goSummaryStep(); 
          expect(scope.clusterEntity.clusterModel.cluster.tags).toBe(undefined);   
          
        });  
        it('should delete registry interface only if not checked', function() {
          scope.validations = validationService;
          scope.clusterEntity.clusterModel.cluster.tags = ""; 
          expect(scope.registry.check).toBe(true);
          expect(scope.clusterEntity.clusterModel.cluster.interfaces.interface.length).toEqual(6); 
          expect(scope.clusterEntity.clusterModel.cluster.interfaces.interface[5]).toEqual({ _type : 'registry', _endpoint : '', _version : '' });         
          scope.goSummaryStep();  
          expect(scope.clusterEntity.clusterModel.cluster.interfaces.interface[5]).toEqual({ _type : 'registry', _endpoint : '', _version : '' }); 
          scope.registry.check = false;
          scope.clusterEntity.clusterModel.cluster.ACL = { _owner : '', _group : '', _permission : '' };
          scope.clusterEntity.clusterModel.cluster.tags = ""; 
          scope.goSummaryStep();  
          expect(scope.clusterEntity.clusterModel.cluster.interfaces.interface[5]).toBeUndefined(); 
          expect(scope.clusterEntity.clusterModel.cluster.interfaces.interface.length).toEqual(5);                       
        }); 
        
        it('should delete properties if empty and leave them if not', function() {
          scope.validations = validationService;
          scope.clusterEntity.clusterModel.cluster.properties.property=[{ _name : '', _value : '' }];          
          scope.goSummaryStep(); 
          expect(scope.clusterEntity.clusterModel.cluster.properties).toBe(undefined);      
        });  
        it('should delete ACL if empty and leave them if not', function() {
          scope.validations = validationService;
          expect(scope.clusterEntity.clusterModel.cluster.ACL).toEqual({ _owner : '', _group : '', _permission : '' }); 
          scope.goSummaryStep();      
          expect(scope.clusterEntity.clusterModel.cluster.ACL).toEqual(undefined);                  
        }); 
        it('should move properties to be the last if coexists with ACL', function() {
          scope.validations = validationService;
          function testACLandPropertiesOrder() {
            var i;
            for (i in scope.clusterEntity.clusterModel.cluster) { //first one out
              if(i === "ACL"){ return true; }
              if(i === "properties"){return false;}  
            };
          }
          delete scope.clusterEntity.clusterModel.cluster.properties;
          delete scope.clusterEntity.clusterModel.cluster.ACL;
          scope.clusterEntity.clusterModel.cluster.properties = {};
          scope.clusterEntity.clusterModel.cluster.properties.property = [{ _name : '2nd', _value : '2nd' }];  
          scope.clusterEntity.clusterModel.cluster.ACL = { _owner : 'this', _group : 'that', _permission : '0755' }; 
          expect(testACLandPropertiesOrder()).toEqual(false); 
          scope.goSummaryStep();             
          expect(scope.clusterEntity.clusterModel.cluster).toEqual(jasmine.objectContaining({ACL:{ _owner : 'this', _group : 'that', _permission : '0755' }}));
          expect(scope.clusterEntity.clusterModel.cluster.properties).toEqual(jasmine.objectContaining({property:[{ _name : '2nd', _value : '2nd' }]}));
          expect(testACLandPropertiesOrder()).toBe(true);                  
        });   
      });
      describe('$scope.jsonString', function() {
        it('should transform the json string to show in the preview', function() {
          scope.validations = validationService;
          expect(scope.jsonString).toEqual(undefined);
          scope.goSummaryStep(); 
          expect(scope.jsonString).toEqual(undefined);
        });   
      });
    });
    
    describe('$scope.xmlPreview.editXML', function() {
      it('should toggle the attribute variable', function() {       
        expect(scope.xmlPreview.edit).toBe(false);
        scope.xmlPreview.editXML(); 
        expect(scope.xmlPreview.edit).toBe(true);
        scope.xmlPreview.editXML(); 
        expect(scope.xmlPreview.edit).toBe(false);
      });   
      
      
    });
  });

})();