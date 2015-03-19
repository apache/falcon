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
  
  var fileModule = angular.module('app.services.fileapi', ['app.services.entity.model']);

  fileModule.factory('FileApi', ["$http", "$q", "EntityModel", function ($http, $q, EntityModel) {

    var FileApi = {};

    FileApi.supported = (window.File && window.FileReader && window.FileList && window.Blob);
    FileApi.errorMessage = 'The File APIs are not fully supported in this browser.';

    FileApi.fileDetails = "No file loaded";
    FileApi.fileRaw = "No file loaded";

    FileApi.loadFile = function (evt) {

      if (FileApi.supported) {
        var deferred = $q.defer(),
          reader = new FileReader(),
          file = evt.target.files[0];

        reader.onload = (function (theFile) {

          reader.readAsText(theFile, "UTF-8");

          return function (e) {
            FileApi.fileRaw = e.target.result;
            FileApi.fileDetails = theFile;
            EntityModel.getJson(FileApi.fileRaw);
            deferred.resolve();
          };

        })(file);

        return deferred.promise;
      }
      else {
        alert(FileApi.errorMessage);
      }
    };
    return FileApi;
  }]);

})();
