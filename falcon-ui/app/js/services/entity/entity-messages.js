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
  var module = angular.module('app.services.messages', []);

  var service = new MessagesService();

  module.factory('MessagesService', [function() {
    return service;
  }]);

  function MessagesService() {
    this.messages = {
      error: [],
      info: []
    };
  }

  MessagesService.prototype.validateCategory = function(category) {
    if(!this.messages[category]) {
      throw new Error('Category not registered');
    }
  };

  MessagesService.prototype.push = function(category, title, detail) {
    this.validateCategory(category);
    this.messages[category].push(new Message(title, detail));
  };

  MessagesService.prototype.pop = function(category) {
    this.validateCategory(category);
    return this.messages[category].pop();
  };


  function Message(title, detail) {
    this.title = title;
    this.detail = detail;
  }

})();
