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

  /***
   * @ngdoc controller
   * @name app.controllers.feed.FeedController
   * @requires EntityModel the entity model to copy the feed entity from
   * @requires Falcon the falcon service to talk with the Falcon REST API
   */
  var clusterModule = angular.module('app.controllers.cluster', [ 'app.services' ]);

  clusterModule.controller('ClusterFormCtrl', [
    "$scope", "$interval", "Falcon", "EntityModel", "$state", "FileApi", "X2jsService", "ValidationService",
    "SpinnersFlag", "$timeout", "$rootScope", "$cookieStore", "$stateParams", "ClusterModel",
    function ($scope, $interval, Falcon, EntityModel, $state, FileApi, X2jsService, validationService,
      SpinnersFlag, $timeout, $rootScope, $cookieStore, $stateParams, clusterModel) {

      if (clusterModel && $stateParams.action === 'clone') {
        $scope.cloningMode = true;
        $scope.editingMode = false;
        $scope.clusterEntity = { 'clusterModel' : clusterModel };
        $scope.clusterEntity.clusterModel.name = "";
      } else if(clusterModel && $stateParams.action === 'edit') {
        $scope.editingMode = true;
        $scope.cloningMode = false;
        $scope.clusterEntity = { 'clusterModel' : clusterModel };
      } else{
        $scope.editingMode = false;
        $scope.cloningMode = false;
        $scope.clusterEntity = EntityModel;
      }

      if ($rootScope.secureMode) {
        $scope.clusterEntity.clusterModel.cluster.properties.property[0]
          = {_name : 'dfs.namenode.kerberos.principal', _value : 'nn/_HOST@EXAMPLE.COM'}
      }
      $scope.$watch("clusterEntity.clusterModel.cluster._name",function(){
        if ($scope.editingMode) {
          return;
        }
        $scope.clusterEntity.clusterModel.cluster.locations.location.forEach (function(loc, index) {
          if (loc._name === "staging") {
            loc._path = "/apps/falcon/"
              + ($scope.clusterEntity.clusterModel.cluster._name ? $scope.clusterEntity.clusterModel.cluster._name + "/" : "")
              + "staging";
          } else if (loc._name === "working") {
            loc._path = "/apps/falcon/"
              + ($scope.clusterEntity.clusterModel.cluster._name ? $scope.clusterEntity.clusterModel.cluster._name + "/" : "")
              + "working";
          }
        });
      });

      $scope.xmlPreview = { edit: false };
      $scope.secondStep = false;

      $scope.enableCustomSparkInterface = function() {
        $scope.customSparkInterfaceEnabled = !$scope.customSparkInterfaceEnabled;
      }
      function normalizeModel() {
        //------------INTERFACE-----------//
        var requiredInterfaceFields = ["readonly", "write", "execute", "workflow", "messaging", "registry", "spark"],
          requiredLocationFields = ["staging", "temp", "working"],
          modelInterfaceArray = $scope.clusterEntity.clusterModel.cluster.interfaces.interface,
          modelLocationsArray = $scope.clusterEntity.clusterModel.cluster.locations.location;

        modelInterfaceArray.forEach(function (element) {
          requiredInterfaceFields.forEach(function (requiredField) {
            if (element._type === requiredField) { requiredInterfaceFields.splice(requiredField, 1); }
          });
        });
        $scope.registry = { check: false };
        $scope.spark = { check: false };
        $scope.customSparkInterfaceEnabled = false;
        requiredInterfaceFields.forEach(function (fieldToPush) {
          var fieldObject = { _type: fieldToPush, _endpoint: "", _version: "" };
          //if (fieldToPush === "registry") { $scope.registry = { check: true }; }
          modelInterfaceArray.push(fieldObject);
        });
        //--------------TAGS--------------//
        if ($scope.clusterEntity.clusterModel.cluster.tags === "" ||
            $scope.clusterEntity.clusterModel.cluster.tags === undefined) {
          $scope.clusterEntity.clusterModel.cluster.tags = "";
          $scope.tagsArray = [{key: null, value: null}];
        } else {
          $scope.splitTags();
        }
        //-------------ACL----------------//
        if (!$scope.clusterEntity.clusterModel.cluster.ACL) {
          angular.copy(EntityModel.defaultValues.cluster.cluster.ACL, $scope.clusterEntity.clusterModel.cluster.ACL);
          /*$scope.clusterEntity.clusterModel.cluster.ACL = {
            _owner: "", _group: "", _permission: ""
          };*/
        }
        if ($cookieStore.get('userToken') && !$scope.clusterEntity.clusterModel.cluster.ACL._owner) {
          $scope.clusterEntity.clusterModel.cluster.ACL._owner = $cookieStore.get('userToken').user;
        }
        //------------Location------------//
        modelLocationsArray.forEach(function(element) {
          requiredLocationFields.forEach(function(requiredField, index) {
            if(element._name === requiredField) { requiredLocationFields.splice(index, 1); }
          });
        });
        requiredLocationFields.forEach(function(fieldToPush) {
          var fieldObject = {_name: fieldToPush, _path: ""};
          modelLocationsArray.push(fieldObject);
        });
        //----------Properties -------------//
        if(!$scope.clusterEntity.clusterModel.cluster.properties) {
          $scope.clusterEntity.clusterModel.cluster.properties = { property : [{ _name: "", _value: ""}] };
        }

      }

      function cleanModel() {

         if (!$scope.clusterEntity.clusterModel.cluster._description) {
          $scope.clusterEntity.clusterModel.cluster._description = '';
        }

        //if registry check is false backups the object and removes it from array
        if ($scope.registry && !$scope.registry.check) {
          $scope.clusterEntity.clusterModel.cluster.interfaces.interface.forEach(function(registry, index) {
            if (registry._type === "registry") {
              $scope.backupRegistryObject = $scope.clusterEntity.clusterModel.cluster.interfaces.interface[index];
              $scope.clusterEntity.clusterModel.cluster.interfaces.interface.splice(index, 1);
            }
          });
        }
        if ($scope.spark && !$scope.spark.check) {
          $scope.clusterEntity.clusterModel.cluster.interfaces.interface.forEach(function(spark, index) {
            if (spark._type === "spark") {
              $scope.backupSparkObject = $scope.clusterEntity.clusterModel.cluster.interfaces.interface[index];
              $scope.clusterEntity.clusterModel.cluster.interfaces.interface.splice(index, 1);
            }
          });
        }
        //deletes property empty last object and array if empty
        if($scope.clusterEntity.clusterModel.cluster.properties){
          var lastOne = $scope.clusterEntity.clusterModel.cluster.properties.property.length - 1;
          if (
            $scope.clusterEntity.clusterModel.cluster.properties.property[lastOne]._name === "" ||
            $scope.clusterEntity.clusterModel.cluster.properties.property[lastOne]._name === undefined ||
            $scope.clusterEntity.clusterModel.cluster.properties.property[lastOne]._value === "" ||
            $scope.clusterEntity.clusterModel.cluster.properties.property[lastOne]._value === undefined
          ) {

            $scope.removeProperty(lastOne);
          }
          if ($scope.clusterEntity.clusterModel.cluster.properties.property.length === 0) {
            delete $scope.clusterEntity.clusterModel.cluster.properties;
          }
        }

        var lastLocationIndex = $scope.clusterEntity.clusterModel.cluster.locations.location.length - 1;
        if (
          $scope.clusterEntity.clusterModel.cluster.locations.location[lastLocationIndex]._name === "" ||
          $scope.clusterEntity.clusterModel.cluster.locations.location[lastLocationIndex]._name === undefined ||
          $scope.clusterEntity.clusterModel.cluster.locations.location[lastLocationIndex]._path === "" ||
          $scope.clusterEntity.clusterModel.cluster.locations.location[lastLocationIndex]._path === undefined
        ) {
          $scope.removeLocation(lastLocationIndex);
        }
        //deletes ACL if empty
        /*if ($scope.clusterEntity.clusterModel.cluster.ACL &&
            $scope.clusterEntity.clusterModel.cluster.ACL._owner === "") {
          delete $scope.clusterEntity.clusterModel.cluster.ACL;
        }*/
        //deletes tags if empty
        if (!$scope.clusterEntity.clusterModel.cluster.tags) {
          delete $scope.clusterEntity.clusterModel.cluster.tags;
        }
        //moves properties to be the last element if acl exists
        $scope.arrangeFieldsOrder();
      }
      $scope.arrangeFieldsOrder = function (xmlObj) {

        var BK,
            orderedObj = {};

        if (xmlObj) {
          BK = xmlObj.cluster;
        } else {
          BK = $scope.clusterEntity.clusterModel.cluster;
        }

        orderedObj._xmlns = 'uri:falcon:cluster:0.1';
        orderedObj._name = BK._name;
        orderedObj._description = BK._description;
        orderedObj._colo = BK._colo;

        if (BK.tags) { orderedObj.tags = BK.tags; }
        if (BK.interfaces) { orderedObj.interfaces = BK.interfaces; }
        if (BK.locations) { orderedObj.locations = BK.locations; }
        if (BK.ACL) { orderedObj.ACL = BK.ACL; }
        if (BK.properties) { orderedObj.properties = BK.properties; }

        delete $scope.clusterEntity.clusterModel.cluster;
        $scope.clusterEntity.clusterModel.cluster = orderedObj;

      };
      //--------------TAGS------------------------//

      $scope.convertTags = function () {
        var result = [];
        if($scope.tagsArray) {
          $scope.tagsArray.forEach(function(element) {
            if(element.key && element.value) {
              result.push(element.key + "=" + element.value);
            }
          });
        }
        result = result.join(",");
        $scope.clusterEntity.clusterModel.cluster.tags = result;
      };
      $scope.splitTags = function () {
        $scope.tagsArray = [];
        if ($scope.clusterEntity.clusterModel.cluster.tags) {
          $scope.clusterEntity.clusterModel.cluster.tags.split(",").forEach(function (fieldToSplit) {
            var splittedString = fieldToSplit.split("=");
            $scope.tagsArray.push({key: splittedString[0], value: splittedString[1]});
          });
        }

      };
      $scope.addTag = function () {
        $scope.tagsArray.push({key: null, value: null});
      };
      $scope.removeTag = function (index) {
        if (!isNaN(index) && index !== undefined && index !== null) {
          $scope.tagsArray.splice(index, 1);
          $scope.convertTags();
        }
      };
       $scope.toggleclick = function () {
         $('.formBoxContainer').toggleClass('col-xs-14 ');
         $('.xmlPreviewContainer ').toggleClass('col-xs-10 hide');
         $('.preview').toggleClass('pullOver pullOverXml');
         ($('.preview').hasClass('pullOver')) ? $('.preview').find('button').html('Preview XML') : $('.preview').find('button').html('Hide XML');
         ($($("textarea")[0]).attr("ng-model") == "prettyXml" ) ? $($("textarea")[0]).css("min-height", $(".formBoxContainer").height() - 40 ) : '';
       };

      //-------------------------------------//
      //----------LOCATION-------------------//

      $scope.addLocation = function () {
        var lastOneIndex = $scope.clusterEntity.clusterModel.cluster.locations.location.length - 1;

        if (!$scope.clusterEntity.clusterModel.cluster.locations.location[lastOneIndex]._name ||
            !$scope.clusterEntity.clusterModel.cluster.locations.location[lastOneIndex]._path) {
          //console.log('location empty');
        } else {
          $scope.clusterEntity.clusterModel.cluster.locations.location.push({_name: "", _path: ""});
        }
      };
      $scope.removeLocation = function (index) {
        if(!isNaN(index) && index !== undefined && index !== null) {
          $scope.clusterEntity.clusterModel.cluster.locations.location.splice(index, 1);
        }
      };
      //-----------PROPERTIES----------------//
      $scope.addProperty = function () {
        var lastOne = $scope.clusterEntity.clusterModel.cluster.properties.property.length - 1;
        if($scope.clusterEntity.clusterModel.cluster.properties.property[lastOne]._name && $scope.clusterEntity.clusterModel.cluster.properties.property[lastOne]._value){
          $scope.clusterEntity.clusterModel.cluster.properties.property.push({ _name: "", _value: ""});
        // $scope.tempPropModel = { _name: "", _value: ""};
        }
      };
      $scope.removeProperty = function(index) {
        if(index !== null && $scope.clusterEntity.clusterModel.cluster.properties.property[index]) {
          $scope.clusterEntity.clusterModel.cluster.properties.property.splice(index, 1);
        }
      };

      $scope.isActive = function (route) {
          return route === $state.current.name;
      };

      $scope.isCompleted = function (route) {
          return $state.get(route).data && $state.get(route).data.completed;
      };

      $scope.validateLocations = function(){
        var stagingLoc;
        var workingLoc;
        $scope.clusterEntity.clusterModel.cluster.locations.location.forEach(function(location){
          if(location._name == "staging"){
            stagingLoc = location._path;
          }
          if(location._name == "working"){
            workingLoc = location._path;
          }
        });
        if(stagingLoc && workingLoc && stagingLoc == workingLoc){
          $scope.locationsEqualError = true;
        }else{
          $scope.locationsEqualError = false;
        }
        return $scope.locationsEqualError;
      };
                                                  //--------------------------------------//
      $scope.goSummaryStep = function (formInvalid) {
          $state.current.data = $state.current.data || {};
          $state.current.data.completed = !formInvalid;

          SpinnersFlag.show = true;
          if($scope.validateLocations()){
          SpinnersFlag.show = false;
          return;
        }

          if (!$scope.validations.nameAvailable || formInvalid) {
          validationService.displayValidations.show = true;
          validationService.displayValidations.nameShow = true;
          SpinnersFlag.show = false;
          return;
        }
        cleanModel();
        $scope.secondStep = true;
        $state.go("forms.cluster.summary");
        $timeout(function () {
          angular.element('.nextBtn').trigger('focus');
        }, 500);

      };

      $scope.hideMessage = function (){
        angular.element('.nameValidationMessage').hide();
      };


      $scope.goGeneralStep = function () {
        SpinnersFlag.backShow = true;
        $scope.secondStep = false;
        validationService.displayValidations.show = false;
        validationService.displayValidations.nameShow = false;
        $scope.validations.nameAvailable = true;
        if(!$scope.registry.check) {
          //recovers previously deleted registry object
          if($scope.backupRegistryObject){
            $scope.clusterEntity.clusterModel.cluster.interfaces.interface.push($scope.backupRegistryObject);
          }
        }
        if(!$scope.spark.check) {
          //recovers previously deleted spark object
          if($scope.backupSparkObject){
            $scope.clusterEntity.clusterModel.cluster.interfaces.interface.push($scope.backupSparkObject);
          }
        }
        if(!$scope.clusterEntity.clusterModel.cluster.tags) {
          $scope.clusterEntity.clusterModel.cluster.tags = "";
        }
        if(!$scope.clusterEntity.clusterModel.cluster.properties) {
          $scope.clusterEntity.clusterModel.cluster.properties = {property : [{ _name: "", _value: ""}]};
        }
        var lastLocationIndex = $scope.clusterEntity.clusterModel.cluster.locations.location.length - 1;
        if($scope.clusterEntity.clusterModel.cluster.locations.location[lastLocationIndex]._name !== "") {
          $scope.addLocation();
        }
      };
      $scope.saveCluster = function () {
        SpinnersFlag.show = true;
        $scope.saveModelBuffer();
        Falcon.logRequest();
        if($scope.editingMode) {
          Falcon.postUpdateEntity($scope.jsonString, "cluster", $scope.clusterEntity.clusterModel.cluster._name)
            .success(function (response) {
              $scope.skipUndo = true;
              Falcon.logResponse('success', response, false);
              $state.go('main');
            })
            .error(function(err) {
              SpinnersFlag.show = false;
              Falcon.logResponse('error', err, false);
              angular.element('body, html').animate({scrollTop: 0}, 300);
            });
        } else {
          Falcon.postSubmitEntity($scope.jsonString, "cluster")
            .success(function (response) {
              $scope.skipUndo = true;
              Falcon.logResponse('success', response, false);
              $state.go('main');
            })
            .error(function(err) {
              SpinnersFlag.show = false;
              Falcon.logResponse('error', err, false);
              angular.element('body, html').animate({scrollTop: 0}, 300);
            });
        }
      };

      //--------------------------------------//
      //----------XML preview-----------------//

      $scope.xmlPreview.editXML = function () {
          $scope.xmlPreview.edit = !$scope.xmlPreview.edit;
      };

      $scope.revertXml = function() {
        if($scope.clusterEntity.clusterModel.cluster.tags !== undefined) {
            $scope.convertTags();
        }
        $scope.showInPreview();
        $scope.invalidXml = false;
      };

      $scope.showInPreview = function() {
        var xmlStr = X2jsService.json2xml_str(angular.copy($scope.clusterEntity.clusterModel));
        $scope.prettyXml = X2jsService.prettifyXml(xmlStr);
        $scope.xml = xmlStr;
      };
      $scope.transformBack = function() {

        try {
          var xmlObj = X2jsService.xml_str2json($scope.prettyXml);

          if (!xmlObj.cluster.ACL || !xmlObj.cluster.ACL._owner || !xmlObj.cluster.ACL._group || !xmlObj.cluster.ACL._permission) {
            xmlObj.cluster.ACL = angular.copy(EntityModel.defaultValues.cluster.cluster.ACL);
            xmlObj.cluster.ACL._owner = EntityModel.getUserNameFromCookie();
          }

          $scope.arrangeFieldsOrder(xmlObj);

          if($scope.clusterEntity.clusterModel.cluster.properties && $scope.clusterEntity.clusterModel.cluster.properties.property[0] === '') {
            $scope.clusterEntity.clusterModel.cluster.properties.property=[];
          }
          $scope.invalidXml = false;
        }
        catch(err) {
          $scope.invalidXml = true;
          console.log('xml malformed');
        }

      };
      $scope.saveModelBuffer = function () {
        $scope.jsonString = angular.toJson($scope.clusterEntity.clusterModel);
        //goes back to js to have x2js parse it correctly
        $scope.jsonString = JSON.parse($scope.jsonString);
        $scope.jsonString = X2jsService.json2xml_str($scope.jsonString);
      };
      function xmlPreviewCallback() {
        if(!$scope.xmlPreview.edit) {
          if($scope.clusterEntity.clusterModel.cluster.tags !== undefined) { $scope.convertTags(); }
          $scope.showInPreview();
        }
        else {
          $scope.splitTags();
          $scope.transformBack();
        }
      }
      $scope.$watch('clusterEntity.clusterModel.cluster', xmlPreviewCallback, true);
      $scope.$watch('tagsArray', xmlPreviewCallback, true);
      $scope.$watch('prettyXml', xmlPreviewCallback, true);

      $scope.skipUndo = false;
      $scope.$on('$destroy', function () {
        var model = angular.copy($scope.clusterEntity.clusterModel.cluster),
            defaultModel = angular.toJson(EntityModel.defaultValues.cluster.cluster);

        model.interfaces.interface.forEach(function (item, index) {
          if (item._type === "registry" && item._endpoint === "" && item._version === "") {
            model.interfaces.interface.splice(index, 1);
          }
        });

        model = angular.toJson(model);

        if (!$scope.skipUndo && !angular.equals(model, defaultModel)) {
          $scope.$parent.cancel('cluster', $rootScope.previousState);
        }
      });

      //------------init------------//
      if($state.params && $state.params.action === 'import'){
          $scope.prettyXml = FileApi.fileRaw;
          $scope.transformBack();
      }else{
        normalizeModel();
      }
      if($state.current.name !=="forms.cluster.general"){
        $state.go("forms.cluster.general");
      }

      $scope.capitalize = function(input) {
          return input ? input.charAt(0).toUpperCase() + input.slice(1) : "";
      };

      $scope.upperCase = function(input) {
          return input ? input.toUpperCase() : "";
      };

    }
  ]);
})();
