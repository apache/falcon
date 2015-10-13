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

  angular.module('dateHelper', [])
    .factory('DateHelper', function () {

      var formatDigit = function(digit){
        if(digit<10){
          digit = "0"+digit;
        }
        return digit;
      };

      var dateHelper = {};

      dateHelper.importDate = function (date, tz) {
        if (!tz || tz === 'UTC') {
          tz = "GMT+00:00";
        }
        var rawDate = Date.parse(date);
        var tzN = parseInt(tz.slice(3));
        var tzDate = new Date (rawDate + (3600000*tzN));

        return new Date(
          tzDate.getUTCFullYear(),
          tzDate.getUTCMonth(),
          tzDate.getUTCDate(),
          tzDate.getUTCHours(),
          tzDate.getUTCMinutes(),
          0, 0);

      };

      dateHelper.createISO = function (date, time, tz) {
        var UTC = new Date(
              Date.UTC(
                date.getUTCFullYear(),
                date.getUTCMonth(),
                date.getUTCDate(),
                time.getHours(),
                time.getMinutes(),
                0, 0
              )
            ).toUTCString() + tz.slice(3),
            UTCRaw = Date.parse(UTC);

        var dateWithSecs = new Date(UTCRaw).toISOString();

        return dateWithSecs.slice(0, -8) + "Z";

      };

      //i.e. 2015-09-10T16:35:21.235Z
      dateHelper.createISOString = function (date, time) {
        var result = date.getFullYear() + "-" + formatDigit(date.getMonth()+1) + "-" + formatDigit(date.getDate())
          + "T" + formatDigit(time.getHours()) + ":" + formatDigit(time.getMinutes()) + "Z";
        return result;
      };

      return dateHelper;

    });

})();
