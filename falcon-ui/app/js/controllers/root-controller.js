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

  var app = angular.module('app.controllers.rootCtrl', ['app.services']);

  app.controller('RootCtrl', [
    "$scope", "$timeout", "Falcon", "FileApi", "EntityModel",
    "$state", "X2jsService", "ValidationService", "SpinnersFlag", "EntityFalcon",
    function ($scope, $timeout, Falcon, FileApi,
              EntityModel, $state, X2jsService, validationService, SpinnersFlag, EntityFalcon) {

      var resultsPerPage = 10;

      $scope.server = Falcon;
      $scope.validations = validationService;
      $scope.buttonSpinners = SpinnersFlag;
      $scope.models = {};

      $scope.pages = [];
      $scope.nextPages = false;

      $scope.handleFile = function (evt) {
        Falcon.logRequest();
        FileApi.loadFile(evt).then(function () {
          if (EntityModel.type === 'Type not recognized') {
            Falcon.logResponse('error', {status: 'ERROR', message:'Invalid xml. File not uploaded'}, false);
          } else {
            Falcon.postSubmitEntity(FileApi.fileRaw, EntityModel.type).success(function (response) {
              Falcon.logResponse('success', response, false);
              $scope.refreshList($scope.tags);
            }).error(function (err) {
              Falcon.logResponse('error', err, false);
            });
          }

        });
      };

      $scope.goPage = function(page){
        $scope.loading = true;
        var offset = (page-1) * resultsPerPage;
        EntityFalcon.searchEntities($scope.entityName, $scope.entityTags, $scope.entityType, offset).then(function() {
          if (EntityFalcon.data !== null) {
            $scope.actualPage = page;
            $scope.searchList = EntityFalcon.data.entity;
            var totalPages = Math.ceil(EntityFalcon.data.totalResults/resultsPerPage);
            $scope.pages = []
            for(var i=0; i<totalPages; i++){
              $scope.pages[i] = {};
              $scope.pages[i].index = (i+1);
              $scope.pages[i].label = ""+(i+1);
              if(page === (i+1)){
                $scope.pages[i].enabled = false;
              }else{
                $scope.pages[i].enabled = true;
              }
            }
            if($scope.searchList.length === 0){
              Falcon.warningMessage("No results matched the search criteria.");
            }
            $timeout(function() {
              angular.element('#tagsInput').focus();
            }, 0, false);
            Falcon.responses.listLoaded = true;
            $scope.loading = false;
          }
        });
      };

      $scope.refreshList = function (tags) {

        $scope.nameFounded = false;
        $scope.typeFounded = false;
        $scope.entityName = "";
        $scope.entityType = "";
        var tagsSt = "";

        $scope.searchList = [];

        if(tags === undefined || tags.length === 0){
          $timeout(function() {
            angular.element('#tagsInput').focus();
          }, 0, false);
          return;
        }

        for(var i=0; i<tags.length; i++){
          var tag = tags[i].text;
          if(tag.indexOf("Name:") !== -1){
            $scope.nameFounded = true;
            tag = tag.substring(5);
            $scope.entityName = tag;
          }else if(tag.indexOf("Type:") !== -1){
            $scope.typeFounded = true;
            tag = tag.substring(5);
            $scope.entityType = tag;
          }else{
            tag = tag.substring(4);
            tagsSt += tag;
            if(i < tags.length-1){
              tagsSt += ",";
            }
          }

        }

        $scope.entityTags = tagsSt;

        $scope.goPage(1);

      };

      $scope.cancel = function (type, state) {
        var cancelInfo = {
          state: state || $state.current.name,
          message: type + ' edition canceled '
        };
        Falcon.logResponse('cancel', cancelInfo, type, false);
      };

    }]);

}());