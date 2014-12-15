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

  describe('ValidationService', function () {
    var validationService;

    beforeEach(module('app.services.validation'));

    beforeEach(inject(function(ValidationService) {
      validationService = ValidationService;
    }));

    describe('define', function() {
      it('Should return a new  validation object', function() {
        expect(validationService.define()).toNotBe(null);
      });
    });
    describe('misc validations', function() {

      it('Should have default validations definitions', function() {

        var validations = validationService.define();

        expect(validations.id).toEqual({
          pattern: /^(([a-zA-Z]([\\-a-zA-Z0-9])*){1,39})$/,
          maxlength: 39,
          minlength: 0,
          required: true
        });

        expect(validations.freeText).toEqual({
          pattern: /^([\sa-zA-Z0-9]){1,40}$/,
          maxlength: 1000,
          minlength: 0,
          required: false
        });
      });
    });


  });
})();