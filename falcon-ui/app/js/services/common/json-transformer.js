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
  var module = angular.module('app.services.json.transformer', []);

  module.factory('JsonTransformerFactory', function() {
    return {
      transform: newFieldTransformation
    };
  });

  function newFieldTransformation(sourceField, targetField, mappingCallback) {
    return new FieldTransformation(sourceField, targetField || sourceField, mappingCallback);
  }

  function FieldTransformation(sourceField, targetField, mappingCallback) {
    var self = this;

    self.sourceFieldPath = sourceField.split(".");
    self.targetFieldPath = targetField.split(".");
    self.mappingCallback = mappingCallback;

    self.transform = function(sourceField, targetField, mappingCallback) {
      return new ComposedFieldTransformation(self, newFieldTransformation(sourceField, targetField, mappingCallback));
    };

    self.apply = function(source, target) {

      var sourceHolder = find(source, self.sourceFieldPath);

      var sourceValue = sourceHolder.get();
      sourceValue = sourceValue && self.mappingCallback ? self.mappingCallback(sourceValue) : sourceValue;

      if (sourceValue) {
        var targetHolder = find(target, self.targetFieldPath);
        targetHolder.set(sourceValue);
      }

      return target;
    };

    function find(target, path) {
      var current = target;
      var child;
      for(var i = 0, n = path.length - 1; i < n; i++) {
        child = path[i];
        createIfNotExists(current, child);
        current = current[child];
      }
      return new Holder(current, path[path.length - 1]);
    }

    function createIfNotExists(current, child) {
      if (!current[child]) {
        current[child] = {};
      }
    }

    function Holder(target, child) {
      this.target = target;
      this.child = child;

      this.get = function() {
        return target[child];
      };

      this.set = function(value) {
        target[child] = value;
      };

    }

    function ComposedFieldTransformation (firstTransformation, secondTransformation) {
      var self = this;

      self.firstTransformation = firstTransformation;
      self.secondTransformation = secondTransformation;

      self.apply = function(source, target) {
        self.firstTransformation.apply(source, target);
        self.secondTransformation.apply(source, target);
        return target;
      };

      self.transform = function(sourceField, targetField, mappingCallback) {
        return new ComposedFieldTransformation(self, newFieldTransformation(sourceField, targetField, mappingCallback));
      };

    }

  }

})();