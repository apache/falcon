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

  var app = angular.module('app', [
    'ui.bootstrap',
    'ui.router',
    'ngCookies',
    'ngAnimate',
    'ngStorage',
    'ngMessages',
    'checklist-model',
    'app.controllers',
    'app.directives',
    'app.services',
    'ngTagsInput',
    'nsPopover',
    'ngMask',
    'dateHelper',
    'focus-if',
	'routeHelper'
  ]);
  app.constant('APP_CONSTANTS',{
    enableXSRFHeader : true
  });
  app.config(["$stateProvider", "$urlRouterProvider", "$httpProvider", 'APP_CONSTANTS', function ($stateProvider, $urlRouterProvider, $httpProvider, AppConstants) {

    $httpProvider.defaults.headers.common['X-CSRF-Token'] = $('meta[name=csrf-token]').attr('content');

    if (AppConstants.enableXSRFHeader ){
      $httpProvider.defaults.headers.common['X-XSRF-HEADER'] = Math.round(Math.random()*100000);
    }
    $httpProvider.defaults.headers.common["X-Requested-By"] = 'X-Requested-By';

    $urlRouterProvider.otherwise("/");

    $stateProvider
      .state('main', {
        url: '/?fromAction',
        templateUrl: 'html/mainTpl.html',
        controller: 'DashboardCtrl'
      })
      .state('authenticating', {
        templateUrl: 'html/authenticating.html'
      })
      .state('login', {
        controller: 'LoginFormCtrl',
        templateUrl: 'html/login.html'
      })
      .state('entityDefinition', {
        controller: 'EntityDefinitionCtrl',
        templateUrl: 'html/entityDefinitionTpl.html'
      })
      .state('forms', {
        templateUrl: 'html/formsTpl.html'
      })
      .state('forms.cluster', {
        url : '/cluster?name&action',
        controller: 'ClusterFormCtrl',
        templateUrl: 'html/cluster/clusterFormTpl.html',
        resolve : {
          ClusterModel : ['$stateParams', 'EntityDetails', function($stateParams, EntityDetails){
            if($stateParams.name !== null){
              return EntityDetails.getEntityDefinition("cluster", $stateParams.name);
            }
          }]
        }
      })
      .state('forms.cluster.general', {
        templateUrl: 'html/cluster/clusterFormGeneralStepTpl.html'
      })
      .state('forms.cluster.summary', {
        templateUrl: 'html/cluster/clusterFormSummaryStepTpl.html'
      })
      .state('forms.feed', {
        url : '/feed?name&action',
        templateUrl: 'html/feed/feedFormTpl.html',
        controller: 'FeedController',
        resolve : {
          FeedModel : ['$stateParams', 'EntityDetails', 'FileApi','X2jsService', function($stateParams, EntityDetails, FileApi, X2jsService){
            if($stateParams.action === 'import'){
              var feedJson = X2jsService.xml_str2json(FileApi.fileRaw);
              return feedJson;
            }
            if($stateParams.name !== null){
              var modelPromise = EntityDetails.getEntityDefinition("feed", $stateParams.name);
              return modelPromise.then(function(model){
                if($stateParams.action === "edit"){
                  model.edit = true;
                }else if($stateParams.action === "clone"){
                  model.clone = true;
                }
                return model;
              });
            }
          }],
          datasourcesList: ['Falcon', function (Falcon) {
            return Falcon.getEntities('datasource').then(
              function (response) {
                return response.data;
              });
          }]
        }
      })
      .state('forms.feed.general', {
        templateUrl: 'html/feed/feedFormGeneralStepTpl.html',
        controller: 'FeedGeneralInformationController',
        resolve: {
          clustersList: ['Falcon', function (Falcon) {
            return Falcon.getEntities('cluster').then(
              function (response) {
                return response.data;
              });
          }]
        }
      })
      .state('forms.feed.advanced', {
        templateUrl: 'html/feed/feedFormAdvancedStepTpl.html',
        controller: 'FeedAdvancedController'
      })
      .state('forms.feed.properties', {
        templateUrl: 'html/feed/feedFormPropertiesStepTpl.html',
        controller: 'FeedPropertiesController'
      })
      .state('forms.feed.location', {
        templateUrl: 'html/feed/feedFormLocationStepTpl.html',
        controller: 'FeedLocationController'
      })
      .state('forms.feed.clusters', {
        templateUrl: 'html/feed/feedFormClustersStepTpl.html',
        controller: 'FeedClustersController',
        resolve: {
          clustersList: ['Falcon', function (Falcon) {
            return Falcon.getEntities('cluster').then(
              function (response) {
                return response.data;
              });
          }]
        }
      })
      .state('forms.feed.summary', {
        templateUrl: 'html/feed/feedFormSummaryStepTpl.html',
        controller: 'FeedSummaryController'
      })
      .state('forms.process', {
        url : '/process?name&action',
        templateUrl: 'html/process/processFormTpl.html',
        controller: 'ProcessRootCtrl',
        resolve : {
          ProcessModel : ['$stateParams', 'EntityDetails', 'FileApi','X2jsService', function($stateParams, EntityDetails, FileApi, X2jsService){
            if($stateParams.action === 'import'){
              var processJson = X2jsService.xml_str2json(FileApi.fileRaw);
              return processJson;
            }
            if($stateParams.name !== null){
              var modelPromise = EntityDetails.getEntityDefinition("process", $stateParams.name);
              return modelPromise.then(function(model){
                if($stateParams.action === "edit"){
                  model.edit = true;
                }else if($stateParams.action === "clone"){
                  model.clone = true;
                }
                return model;
              });
            }
          }]
        }
      })
      .state('forms.process.general', {
        templateUrl: 'html/process/processFormGeneralStepTpl.html',
        controller: 'ProcessGeneralInformationCtrl',
        resolve: {
          clustersList: ['Falcon', function (Falcon) {
            return Falcon.getEntities('cluster').then(
              function (response) {
                return response.data;
              });
          }],
          feedsList: ['Falcon', function (Falcon) {
            return Falcon.getEntities('feed').then(
              function (response) {
                return response.data;
              });
          }]
        }
      })
      .state('forms.process.advanced', {
        templateUrl: 'html/process/processFormAdvancedStepTpl.html',
        controller: 'ProcessAdvancedCtrl'
      })
      .state('forms.process.summary', {
        templateUrl: 'html/process/processFormSummaryStepTpl.html',
        controller: 'ProcessSummaryCtrl'
      })
      .state('entityDetails', {
        url : '/entity?name&type',
        views: {
          '': {
            controller: 'EntityDetailsCtrl',
            templateUrl: 'html/entityDetailsTpl.html'
          },
          'feedSummary@entityDetails': {
            templateUrl: 'html/feed/feedSummary.html'
          },
          'processSummary@entityDetails': {
            templateUrl: 'html/process/processSummary.html'
          },
          'clusterSummary@entityDetails': {
            templateUrl: 'html/cluster/clusterSummary.html'
          },
          'snapshotSummary@entityDetails': {
            templateUrl: 'html/snapshot/snapshotSummary.html'
          },
          'datasetSummary@entityDetails': {
            templateUrl: 'html/dataset/datasetSummary.html'
          }
        },
        resolve : {
          entity : ['$stateParams', 'EntityDetails', function($stateParams, EntityDetails){
            return EntityDetails.getEntityDetails($stateParams.name, $stateParams.type.toLowerCase());
          }]
        }
      })
      .state('forms.dataset', {
        url : '/dataset?name&action',
        controller: 'DatasetCtrl',
        templateUrl: 'html/dataset/datasetFormTpl.html',
        resolve: {
          clustersList: ['Falcon', function (Falcon) {
            return Falcon.getEntities('cluster').then(
              function (response) {
                return response.data.entity;
              });
          }],
          DatasetModel : ['$stateParams', 'EntityDetails', function($stateParams, EntityDetails){
            if($stateParams.name !== null){
              var modelPromise = EntityDetails.getEntityDefinition("process", $stateParams.name);
              return modelPromise.then(function(model){
                if($stateParams.action === "edit"){
                  model.edit = true;
                }else if($stateParams.action === "clone"){
                  model.clone = true;
                }
                return model;
              });
            }
          }]
        }
      })
      .state('forms.dataset.general', {
        templateUrl: 'html/dataset/datasetFormGeneralStepTpl.html'
      })
      .state('forms.dataset.summary', {
        templateUrl: 'html/dataset/datasetFormSummaryStepTpl.html'
      })
      .state('forms.snapshot', {
	url : '/snapshot?name&action',
	controller: 'SnapshotController',
	templateUrl: 'html/snapshot/snapshotFormTpl.html',
	resolve: {
	  clustersList: ['Falcon', function (Falcon) {
			return Falcon.getEntities('cluster').then(
			  function (response) {
				  return response.data;
			  });
	  }],
	  SnapshotModel : ['$stateParams', 'EntityDetails', function($stateParams, EntityDetails){
			if ($stateParams.name !== null) {
			  var modelPromise = EntityDetails.getEntityDefinition("process", $stateParams.name);
			  return modelPromise.then(function(model){
				if ($stateParams.action === "edit") {
				  model.edit = true;
				} else if($stateParams.action === "clone") {
				  model.clone = true;
				}
				return model;
			  });
			}
		}]
	}
      })
      .state('forms.snapshot.general', {
        templateUrl: 'html/snapshot/snapshotFormGeneralStepTpl.html'
      })
      .state('forms.snapshot.advanced', {
        templateUrl: 'html/snapshot/snapshotFormAdvancedStepTpl.html'
      })
      .state('forms.snapshot.summary', {
        templateUrl: 'html/snapshot/snapshotFormSummaryStepTpl.html'
      })
      .state('instanceDetails', {
        templateUrl: 'html/instanceDetails.html',
        controller: 'InstanceDetailsCtrl'
      })
      .state('forms.datasource', {
        url : '/datasource?name&action',
        templateUrl: 'html/datasource/datasourceFormTpl.html',
        controller: 'DatasourceController',
        resolve : {
          DatasourceModel : ['$stateParams', 'EntityDetails', 'FileApi', 'X2jsService', function($stateParams, EntityDetails, FileApi, X2jsService){
            if($stateParams.action === 'import'){
              var dataSourceJson = X2jsService.xml_str2json(FileApi.fileRaw);
              return dataSourceJson;
            }
            if($stateParams.name !== null){
              var modelPromise = EntityDetails.getEntityDefinition("datasource", $stateParams.name);
              return modelPromise.then(function(model){
                if($stateParams.action === "edit"){
                  model.edit = true;
                } else if($stateParams.action === "clone"){
                  model.clone = true;
                }
                return model;
              });
            }
          }]
        }
      })
      .state('forms.datasource.general', {
        templateUrl: 'html/datasource/datasourceFormGeneralStepTpl.html',
        controller: 'DatasourceGeneralInformationController'
      })
      .state('forms.datasource.advanced', {
        templateUrl: 'html/datasource/datasourceFormAdvancedStepTpl.html',
        controller: 'DatasourceAdvancedController'
      })
      .state('forms.datasource.summary', {
        templateUrl: 'html/datasource/datasourceFormSummaryStepTpl.html',
        controller: 'DatasourceSummaryController'
      })
    ;

  }]);

  app.run(['$rootScope', '$state', '$location', '$http', '$stateParams', '$cookieStore', 'SpinnersFlag', 'ServerAPI', '$timeout', '$interval',
    function ($rootScope, $state, $location, $http, $stateParams, $cookieStore, SpinnersFlag, ServerAPI, $timeout, $interval) {

      $rootScope.ambariView = function () {
        var location_call = $location.absUrl();
        var index_call = location_call.indexOf("views/");
        if (index_call !== -1) {
          return true;
        } else {
          return false;
        }
      };

      var location = $location.absUrl();
      var index = location.indexOf("views/");
      if (index !== -1) {
        index = index + 6;
        var path = location.substring(index);
        var servicePaths = path.split("/");
        $rootScope.serviceURI = '/api/v1/views/' + servicePaths[0] + '/versions/' + servicePaths[1] + '/instances/' + servicePaths[2] + '/resources/proxy';
      }

      if(!$rootScope.secureModeDefined){
        $rootScope.secureMode = false;
        ServerAPI.clearUser().then(function() {
          ServerAPI.getServerConfig().then(function() {
            if (ServerAPI.data) {
              ServerAPI.data.properties.forEach(function(property) {
                if(property.key == 'authentication'){
                  if(property.value == 'kerberos'){
                    $rootScope.secureMode = true;
                  }
                }
                if (property.key == 'safemode') {
                  if (property.value == 'true') {
                    $rootScope.safeMode = true;
                  } else {
                    $rootScope.safeMode = false;
                  }
                }
              });
            }
            $rootScope.secureModeDefined = true;
          });
        });
      }

      $rootScope.isSecureMode = function () {
        if(!$rootScope.secureModeDefined){
          return false;
        }else if ($rootScope.secureMode) {
          return true;
        }else {
          return false;
        }
      };

      $rootScope.userLogged = function () {
        if (angular.isDefined($cookieStore.get('userToken')) && $cookieStore.get('userToken') !== null) {
          return true;
        } else {
          return false;
        }
      };

      //$rootScope.$on('$stateChangeSuccess', function (ev, to, toParams, from, fromParams) {
      $rootScope.$on('$stateChangeSuccess', function (ev, to, toParams, from) {
        SpinnersFlag.show = false;
        SpinnersFlag.backShow = false;

        $rootScope.previousState = from.name;
        $rootScope.currentState = to.name;
      });

      $rootScope.$on('$stateChangeError',
        //function(event, toState, toParams, fromState, fromParams, error){
        function (event, toState, toParams, fromState, error) {
          console.log('Manual log of stateChangeError: ' + error);
        });

      var checkRedirect = function(event, toState){
        if (toState.name !== 'login') {
          if ($rootScope.ambariView()) {

            if (angular.isDefined($cookieStore.get('userToken')) && $cookieStore.get('userToken') !== null) {

            } else {
              event.preventDefault();
              $http.get($rootScope.serviceURI).success(function (data) {
                var userToken = {};
                userToken.user = data;
                $cookieStore.put('userToken', userToken);
                $state.transitionTo('main');
              });
            }

          }else if ($rootScope.secureMode) {

            ServerAPI.getCurrentUser().then(function() {
              var userToken = {};
              userToken.user = ServerAPI.user;
              $cookieStore.put('userToken', userToken);
              $state.transitionTo('main');
            });

          }else if ($rootScope.userLogged()) {

            var userToken = $cookieStore.get('userToken');
            var timeOut = new Date().getTime();
            timeOut = timeOut - userToken.timeOut;
            if (timeOut > userToken.timeOutLimit) {
              $cookieStore.put('userToken', null);
              event.preventDefault();
              $state.transitionTo('login');
            } else {
              userToken.timeOut = new Date().getTime();
              $cookieStore.put('userToken', userToken);
            }

          } else {
            event.preventDefault();
            $state.transitionTo('login');
          }
        }
      };

      $rootScope.$on('$stateChangeStart',
        function (event, toState) {
          if ($rootScope.userLogged()) {
            var userToken = $cookieStore.get('userToken');
            var timeOut = new Date().getTime();
            timeOut = timeOut - userToken.timeOut;
            if (timeOut > userToken.timeOutLimit) {
              $cookieStore.put('userToken', null);
              event.preventDefault();
              $state.transitionTo('login');
            } else {
              userToken.timeOut = new Date().getTime();
              $cookieStore.put('userToken', userToken);
            }
          }else{
            var interval;
            if(!$rootScope.secureModeDefined){
              if (toState.name !== 'authenticating') {
                event.preventDefault();
                $state.transitionTo('authenticating');
              }
              interval = $interval(function() {
                if($rootScope.secureModeDefined){
                  $interval.cancel(interval);
                  checkRedirect(event, toState);
                }
              }, 1000);
            }
          }
        });

    }]);

})();
