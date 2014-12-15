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

  describe('JsonTransformerFactory', function () {
    var httpBackendMock;
    var factory;

    beforeEach(module('app.services.json.transformer'));

    beforeEach(inject(function($httpBackend, JsonTransformerFactory) {
      httpBackendMock = $httpBackend;
      factory = JsonTransformerFactory;
    }));

    describe('Field transformation', function() {


      it('Should create a new field transformation from source and target', function() {
        var transformation = factory.transform('name', '_name');
        var  source = {name: 'someName'}, target = {};

        transformation.apply(source, target);

        expect(target).toEqual({_name: 'someName'});
      });

      it('Should not add the element if it does not exist', function() {
        var transformation = factory.transform('name', '_name');
        var  source = {}, target = {};

        transformation.apply(source, target);

        expect(target).toEqual({});
      });

      it('Should not add the element if it is null', function() {
        var transformation = factory.transform('name', '_name');
        var  source = {name: null}, target = {};

        transformation.apply(source, target);

        expect(target).toEqual({});
      });

      it('Should create a new field transformation implying the target field', function() {
        var transformation = factory.transform('name');
        var  source = {name: 'someName'}, target = {};

        transformation.apply(source, target);

        expect(target).toEqual({name: 'someName'});
      });


      it('Should create a new field transformation for a nested property', function() {
        var transformation = factory.transform('nested.key', 'nested1.nested2.nested3.key');
        var  source = {nested: {key: 'key', key2: 'key2'}}, target = {};

        transformation.apply(source, target);

        expect(target).toEqual({nested1: {nested2: { nested3: {key: 'key'}}}});
      });

      it('Should be able to apply many transformations one after the other', function() {
        var transformation1 = factory.transform('nested.key', 'nested1.nested2.nested3.key');
        var transformation2 = factory.transform('nested.key2', 'nested1.nested2._key');
        var  source = {nested: {key: 'key', key2: 'key2'}}, target = {};

        transformation1.apply(source, target);
        transformation2.apply(source, target);

        expect(target).toEqual({nested1: {nested2: { _key: 'key2', nested3: {key: 'key'}}}});
      });

      it('Should work for arrays too', function() {
        var transformation = factory.transform('nested.key', 'key');
        var  source = {nested: {key: ['a', 'b']}}, target = {};

        transformation.apply(source, target);

        expect(target).toEqual({key: ['a','b']});
      });

      it('Should support custom mapping functions', function() {
        var mapping = function (input) {
          return input.key + '=' + input.value;
        };
        var transformation =  factory.transform('nested.value', 'nested1.nested2', mapping);
        var  source = {nested: {value: {key: 'key', value: 'value'}}}, target = {};

        transformation.apply(source, target);

        expect(target).toEqual({nested1: {nested2: 'key=value'}});
      });


      it('Should be able to chain transformations', function() {
        var  source = {nested: {key: 'key', key2: 'key2'}}, target = {};

        var transformation = factory
          .transform('nested.key', 'nested1.nested2.nested3.key')
          .transform('nested.key2', 'nested1.nested2._key')
          .transform('nested.key2', 'nested1.nested2._key')
          .transform('nested.key2', 'nested1.nested2._key');

        transformation.apply(source, target);

        expect(target).toEqual({nested1: {nested2: { _key: 'key2', nested3: {key: 'key'}}}});
      });

      it('Should be able transform arrays', function() {
        var  source = {nested: {list: [{key: 'key'}], key2: 'key2'}}, target = {};
        var nestedTransformation = factory.transform('key', '_key');
        var transformation = factory.transform('nested.list', 'nested1.nested2.list', function (list) {
          return list.map(function(input) { return nestedTransformation.apply(input, {}); });
        });


        transformation.apply(source, target);

        expect(target).toEqual({nested1: {nested2: {list: [{_key: 'key'}]}}});
      });


    });
  });
})();