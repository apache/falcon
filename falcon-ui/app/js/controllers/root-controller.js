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
    "$state", "X2jsService", "ValidationService", "SpinnersFlag", "EntityFalcon", '$localStorage',
    function ($scope, $timeout, Falcon, FileApi,
              EntityModel, $state, X2jsService, validationService, SpinnersFlag, EntityFalcon, $localStorage) {

      var resultsPerPage = 20;

      $scope.server = Falcon;
      $scope.validations = validationService;
      $scope.buttonSpinners = SpinnersFlag;
      $scope.models = {};

      $scope.pages = [];
      $scope.nextPages = false;
      $scope.hasClusters = true;

      $scope.handleFile = function (evt) {
        Falcon.logRequest();
        FileApi.loadFile(evt).then(function () {
          if (EntityModel.type === 'Type not recognized') {
            Falcon.logResponse('error', {status: 'ERROR', message:"Entity type not recognized"}, false);
          } else {
            var entityType = EntityModel.type;
            $state.go("forms." + entityType + ".general", {'action':'import'}, {reload: true});
          }
        });
      };

      $scope.goPage = function(page, type){
        $scope.currentPage = page;
        if (!(type && type == 'list')) {
          $scope.loading = true;
        }

        var offset = (page-1) * resultsPerPage;
        return EntityFalcon.searchEntities($scope.entityName, $scope.entityTags, $scope.entityType, offset).then(function() {
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
            if($scope.searchList.length === 0 && !(type && type == 'list')){
              Falcon.warningMessage("No results matched the search criteria.");
            }
            $timeout(function() {
              angular.element('#tagsInput').focus();
            }, 0, false);
            Falcon.responses.listLoaded = true;
            if (!(type && type == 'list')) {
              $scope.loading = false;
            }
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
        var message = type + ' edition cancelled';
        if(type === 'cluster'){
          message = 'Create New Cluster operation cancelled'
        }
        var cancelInfo = {
          state: state || $state.current.name,
          message: message
        };
        Falcon.logResponse('cancel', cancelInfo, type, false);
      };

      $scope.displayResults = function (remove) {
        $state.go("main");
        $scope.refreshList($scope.tags);
        (!remove) ? $scope.persistSearch($scope.tags) : '';

      };

      $scope.persistSearch = function(newTag){
        var storedTags = $localStorage['SearchedTag'],
            flagExit = false,
            newval = newTag[newTag.length -1].text;


        if(storedTags !== undefined && newTag !== undefined && newTag.length > 0){

          for(var t=0; t<storedTags.length; t++){
            if(storedTags[t].toLowerCase() === newval.toLowerCase()) {
              flagExit = true;
              break;
            }
          }
          if(!flagExit ) {
            storedTags.push(newval);
            $localStorage['SearchedTag'] = storedTags;
          }

          if(storedTags.length > 5) { storedTags.splice(0,1); $localStorage['SearchedTag'] = storedTags; }

        } else if(newTag !== undefined && newTag.length > 0) {
          $localStorage['SearchedTag'] = [newval];
        }

      };

      $scope.clearTags = function(){
        $scope.tags = [];
        $scope.refreshList($scope.tags);
      };

$scope.loadTags = function(query) {
        var tags = new Array(), storedTags = $localStorage['SearchedTag'], tagAdded = false, tempTags = [];
        if(!$scope.$parent.nameFounded){
          tags.push({ text: 'Name:' + query });
          tempTags.push('Name:' + query);
        }

        if(storedTags != undefined && storedTags.length > 0){
          for(var e=0; e < storedTags.length; e++){
              if(storedTags[e].toLowerCase().indexOf(query.toLowerCase()) != -1){
                 if(tempTags.indexOf(storedTags[e]) == -1) {
                    tags.push({ text: storedTags[e] });
                    tempTags.push('Name:' + query);
                    tagAdded = true;
               }
              }
          }
        }


        if(!$scope.$parent.typeFounded && !tagAdded){
          var queryAux = query.toUpperCase();
          if(queryAux === "F" || queryAux === "FE" || queryAux === "FEE" || queryAux === "FEED"){
            tags.push({ text: 'Type:feed'});
          }
          if(queryAux === "P" || queryAux === "PR" || queryAux === "PRO" || queryAux === "PROC" || queryAux === "PROCE"
              || queryAux === "PROCES" || queryAux === "PROCESS"){
            tags.push({ text: 'Type:process'});
          }
          if(queryAux === "M" || queryAux === "MI" || queryAux === "MIR" || queryAux === "MIRR" || queryAux === "MIRRO"
              || queryAux === "MIRROR"){
            tags.push({ text: 'Type:mirror'});
          }
        }
        if(query !== "*"){
          tags.push({ text: 'Tag:' + query });
        }
        return tags;
      };

      $scope.clusterInterfaceLabels = function(interfaceType) {
        switch (interfaceType) {
          case "readonly":
            return "File System Read Endpoint Address";
          case "write":
            return "File System Default Address";
          case "execute":
            return "Yarn Resource Manager Address";
          case "workflow":
            return "Workflow Address";
          case "messaging":
            return "Message Broker Address";
          case "registry":
            return "Metadata Catalog Registry";
          case "spark":
            return "Spark";
          default:
            return "";
        }
      };

      $scope.displayEntities = function (type) {
        $state.go("main", { 'fromAction' : 'listEntities'});
        $scope.entityType = type;
        $scope.entityName = '';
        $scope.entityTags = '';
        $scope.goPage(1, 'list');
      };

      $scope.feedPropertiesLabels = {
        queueName: 'Queue Name',
        jobPriority: 'Job Priority',
        timeout: 'Timeout',
        parallel: 'Parallel',
        maxMaps: 'Max Maps',
        mapBandwidthKB: 'Map Bandwidth KB'
      };

    }]);

}());