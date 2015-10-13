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

  module.factory('ValidationService', ["$window", function ($window) {
    var checkMessages = {
        name: {
          patternInvalid: "The name has an invalid format.",
          unavailable: "The name you choosed is not available",
          empty: "You need to specify a name"
        },
        colo: {
          empty: "You need to provide a colo",
          patternInvalid: "The Colo has an invalid format. "
        },
        description: {
          empty: "You need to provide a description",
          patternInvalid: "The Description has an invalid format. "
        },
        path: {
          empty: "You need to provide a path",
          patternInvalid: "The Path has an invalid format. "
        },
        key: {
          empty: "You need to provide a key",
          patternInvalid: "The Key has an invalid format. "
        },
        value: {
          empty: "You need to provide a value",
          patternInvalid: "The Value has an invalid format. "
        },
        location: {
          empty: "You need to provide a  location",
          patternInvalid: "The Location has an invalid format. "
        },
        provider: {
          empty: "You need to provide a provider",
          patternInvalid: "The provider has an invalid format. "
        },
        acl: {
          owner: {
            empty: "You need to provide an owner",
            patternInvalid: "The Owner has an invalid format. "
          },
          group: {
            empty: "You need to provide a group",
            patternInvalid: "The Group has an invalid format. "},
          permission: {
            empty: "You need to provide a Permission",
            patternInvalid: "The Permission has an invalid format. "
          }
        },
        engine: { empty: "You need to select an engine" },
        cluster: { empty: "You need to select a cluster" },
        feed: { empty: "You need to select a feed" },
        date: {
          empty: "You need to select a date",
          startAfterEnd: "Start date must be before end date.",
          patternInvalid: "The start Date has an invalid format. "
        },
        number: {
          empty: "You need to provide a number",
          patternInvalid: "The number needs to be one or two digits "
        },
        option: { empty: "You need to select an option" },
        user: {
          empty: "Please enter your user name.",
          patternInvalid: "The User has an invalid format."
        },
        password: {
          empty: "Please enter your password.",
          patternInvalid: "The Password has an invalid format."
        },
        email: {
          patternInvalid: "The email is invalid."
        },
        allocationNumber: {
          empty: "You need to provide a number",
          patternInvalid: "The number you provided is invalid "
        },
        permission: {
          empty: "You need to provide a Permission",
          patternInvalid: "The Permission has an invalid format. "
        },
        url: {
          empty: "You need to provide a URL",
          patternInvalid: "The URL has an invalid format. "
        },
        databases: {
          empty: "You need to provide the databases",
          patternInvalid: "The Databases have an invalid format. "
        },
        database: {
          empty: "You need to provide the database",
          patternInvalid: "The database has an invalid format. "
        },
        tables: {
          empty: "You need to provide the tables",
          patternInvalid: "The tables have an invalid format. "
        },
        s3: {
          empty: "You need to provide a S3 URL",
          patternInvalid: "The URL has an invalid format. It needs to start with 's3' and end with '.amazonaws.com'"
        },
        azure: {
          empty: "You need to provide an Azure URL",
          patternInvalid: "The URL has an invalid format. It needs to end with '.blob.core.windows.net'"
        }
      },
      checkPatterns = {
        name: new RegExp("^[a-zA-Z0-9-_]{1,60}$"),
        id: new RegExp("^(([a-zA-Z]([\\-a-zA-Z0-9_])*){1,39})$"),
        password: new RegExp("^(([a-zA-Z]([\\-a-zA-Z0-9])*){1,39})$"),
        freeText: new RegExp("^([\\sa-zA-Z0-9]){1,40}$"),
        textarea: new RegExp("^([\\sa-zA-Z0-9,]){1,100}$"),
        alpha: new RegExp("^([a-zA-Z0-9-_]){1,100}$"),
        commaSeparated: new RegExp("^[a-zA-Z0-9,]{1,80}$"),
        unixId: new RegExp("^([a-zA-Z_][a-zA-Z0-9-_\\.\\-]{0,30})$"),
        unixPermissions: new RegExp("^((([x0-7]){1,5})|(\\*))$"),
        osPath: new RegExp("^[^\\0 ]+$"),
        twoDigits: new RegExp("^([0-9]){1,4}$"), //>> requirement change to 4 digits, just to dont change all inputs that reference this
        tableUri: new RegExp("^[^\\0]+$"),
        versionNumbers: new RegExp("^[0-9]{1,2}\\.[0-9]{1,2}\\.[0-9]{1,2}$"),
        url: new RegExp("^[^\\0 ]+\\.[a-zA-Z0-9]{1,3}$"),
        number: new RegExp("^([-0-9]){1,40}$"),
        email: new RegExp("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,4}$"),
        s3: new RegExp("^s3[a-zA-Z0-9._%+-:\\/]+\\.amazonaws.com$"),
        azure: new RegExp("^[a-zA-Z0-9._%+-:@\\/]+\\.blob.core.windows.net$")
      };

    function acceptOnlyNumber(evt) {
      var theEvent = evt || $window.event,
        key = theEvent.keyCode || theEvent.which,
        BACKSPACE = 8,
        DEL = 46,
        ENTER = 13,
        ARROW_KEYS = {left: 37, right: 39},
        TAB = 9,
        regex = /[0-9]|\./;

      if (key === BACKSPACE || key === DEL || key === ARROW_KEYS.left || key === ARROW_KEYS.right || key === TAB || key === ENTER) {
        return true;
      }

      key = String.fromCharCode(key);

      if (!regex.test(key)) {
        theEvent.returnValue = false;
        if (theEvent.preventDefault) { theEvent.preventDefault(); }
      }
    }
    function acceptNoSpaces(evt) {
      var theEvent = evt || $window.event,
        key = theEvent.keyCode || theEvent.which,
        SPACE = 32;

      if (key === SPACE) {
        theEvent.returnValue = false;
        theEvent.preventDefault();
        return false;
      }
    }

    return {
      messages: checkMessages,
      patterns: checkPatterns,
      nameAvailable: true,
      displayValidations: {show: false, nameShow: false},
      acceptOnlyNumber: acceptOnlyNumber,
      acceptNoSpaces: acceptNoSpaces
    };

  }]);
}());





