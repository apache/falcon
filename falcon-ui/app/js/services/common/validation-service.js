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
  var module = angular.module('app.services.validation', []);

  module.factory('ValidationService', function() {
    return {
      define: function() {
        return defineValidations();
      }
    };
  });

  function defineValidations() {
    return {
      id: validate(/^(([a-zA-Z]([\\-a-zA-Z0-9])*){1,39})$/, 39, 0, true),
      password: validate(/^(([a-zA-Z]([\\-a-zA-Z0-9])*){1,39})$/, 39, 0, true),
      freeText: validate(/^([\sa-zA-Z0-9]){1,40}$/),
      alpha: validate(/^([a-zA-Z0-9]){1,100}$/),
      commaSeparated: validate(/^[a-zA-Z0-9,]{1,80}$/),
      unixId: validate(/^([a-z_][a-z0-9-_\.\-]{0,30})$/),
      unixPermissions: validate(/^((([0-7]){1,4})|(\*))$/),
      osPath: validate(/^[^\0]+$/),
      twoDigits: validate(/^([0-9]){1,2}$/),
      tableUri: validate(/^[^\0]+$/),
      versionNumbers: validate(/^[0-9]{1,2}\.[0-9]{1,2}\.[0-9]{1,2}$/)
    };
  }

  function validate(pattern, maxlength, minlength, required) {
    return {
      pattern: pattern,
      maxlength: maxlength || 1000,
      minlength: minlength || 0,
      required: required || false
    };
  }

})();