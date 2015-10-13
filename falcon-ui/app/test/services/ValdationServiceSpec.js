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

    beforeEach(inject(function (ValidationService) {
      validationService = ValidationService;
    }));

    describe('init', function () {
      it('Should return a validation object service', function () {
        expect(validationService).toBeDefined();
      });
    });
    describe('messages', function () {
      it('Should return a messages validation object', function () {
        expect(validationService.messages).toBeDefined();
      });
    });
    describe('patterns', function () {
      it('Should return an object with the validation patterns', function () {
        expect(validationService.patterns).toBeDefined();
        expect(validationService.patterns.name).toEqual(/^[a-zA-Z0-9-_]{1,60}$/);

      });
    });
    describe('nameAvailable', function () {
      it('Should return a boolean with the name availability', function () {
        expect(validationService.nameAvailable).toBe(true);
      });
    });
    describe('displayValidations', function () {
      it('Should return an object with the displays in false', function () {
        expect(validationService.displayValidations).toEqual({show: false, nameShow: false});
      });
    });
    describe('acceptOnlyNumber', function () {
      it('Should return an method to prevent default if no number typed', function () {
        var isFunction = validationService.acceptOnlyNumber instanceof Function;
        expect(isFunction).toBe(true);
      });
    });
    describe('acceptNoSpaces', function () {
      it('Should return a method prevent default if space typed', function () {
        var isFunction = validationService.acceptNoSpaces instanceof Function;
        expect(isFunction).toBe(true);
      });
    });
  });
}());