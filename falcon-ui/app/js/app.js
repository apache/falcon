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
    'ngMessages',
    'checklist-model',
    'app.controllers',
    'app.directives',
    'app.services',
    'ngTagsInput',
    'nsPopover', 'ngAnimate', 'ngMask', 'dateHelper'
  ]);

  app.config(["$stateProvider", "$urlRouterProvider", "$httpProvider", function ($stateProvider, $urlRouterProvider, $httpProvider) {

    $httpProvider.defaults.headers.common['X-CSRF-Token'] = $('meta[name=csrf-token]').attr('content');

    $httpProvider.defaults.headers.common["X-Requested-By"] = 'X-Requested-By';

    $urlRouterProvider.otherwise("/");

    $stateProvider
      .state('main', {
        url: '/',
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
        controller: 'ClusterFormCtrl',
        templateUrl: 'html/cluster/clusterFormTpl.html'
      })
      .state('forms.cluster.general', {
        templateUrl: 'html/cluster/clusterFormGeneralStepTpl.html'
      })
      .state('forms.cluster.summary', {
        templateUrl: 'html/cluster/clusterFormSummaryStepTpl.html'
      })
      .state('forms.feed', {
        templateUrl: 'html/feed/feedFormTpl.html',
        controller: 'FeedController'
      })
      .state('forms.feed.general', {
        templateUrl: 'html/feed/feedFormGeneralStepTpl.html',
        controller: 'FeedGeneralInformationController'
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
        templateUrl: 'html/process/processFormTpl.html',
        controller: 'ProcessRootCtrl'
      })
      .state('forms.process.general', {
        templateUrl: 'html/process/processFormGeneralStepTpl.html',
        controller: 'ProcessGeneralInformationCtrl'
      })
      .state('forms.process.properties', {
        templateUrl: 'html/process/processFormPropertiesStepTpl.html',
        controller: 'ProcessPropertiesCtrl'
      })
      .state('forms.process.clusters', {
        templateUrl: 'html/process/processFormClustersStepTpl.html',
        controller: 'ProcessClustersCtrl',
        resolve: {
          clustersList: ['Falcon', function (Falcon) {
            return Falcon.getEntities('cluster').then(
              function (response) {
                return response.data;
              });
          }]
        }
      })
      .state('forms.process.io', {
        templateUrl: 'html/process/processFormInputsAndOutputsStepTpl.html',
        controller: 'ProcessInputsAndOutputsCtrl',
        resolve: {
          feedsList: ['Falcon', function (Falcon) {
            return Falcon.getEntities('feed').then(
              function (response) {
                return response.data;
              });
          }]
        }
      })
      .state('forms.process.summary', {
        templateUrl: 'html/process/processFormSummaryStepTpl.html',
        controller: 'ProcessSummaryCtrl'
      })
      .state('entityDetails', {
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
          }
        }
      })
      .state('forms.dataset', {
        controller: 'DatasetCtrl',
        templateUrl: 'html/dataset/datasetFormTpl.html',
        resolve: {
          clustersList: ['Falcon', function (Falcon) {
            return Falcon.getEntities('cluster').then(
              function (response) {
                return response.data.entity;
              });
          }]
        }
      })
      .state('forms.dataset.general', {
        templateUrl: 'html/dataset/datasetFormGeneralStepTpl.html'
      })
      .state('forms.dataset.summary', {
        templateUrl: 'html/dataset/datasetFormSummaryStepTpl.html'
      })
      .state('instanceDetails', {
        templateUrl: 'html/instanceDetails.html',
        controller: 'InstanceDetailsCtrl'
      })
    ;

  }]);

  app.run(['$rootScope', '$state', '$location', '$http', '$stateParams', '$cookieStore', 'SpinnersFlag', 'ServerAPI', '$timeout', '$interval',
    function ($rootScope, $state, $location, $http, $stateParams, $cookieStore, SpinnersFlag, ServerAPI, $timeout, $interval) {

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
              });
            }
            $rootScope.secureModeDefined = true;
          });
        });
      }

      var location = $location.absUrl();
      var index = location.indexOf("views/");
      if (index !== -1) {
        index = index + 6;
        var path = location.substring(index);
        var servicePaths = path.split("/");
        $rootScope.serviceURI = '/api/v1/views/' + servicePaths[0] + '/versions/' + servicePaths[1] + '/instances/' + servicePaths[2] + '/resources/proxy';
      }

      $rootScope.ambariView = function () {
        var location_call = $location.absUrl();
        var index_call = location_call.indexOf("views/");
        if (index_call !== -1) {
          return true;
        } else {
          return false;
        }
      };

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
        if($rootScope.ambariView()){
          return true;
        } else {
          if (angular.isDefined($cookieStore.get('userToken')) && $cookieStore.get('userToken') !== null) {
            return true;
          } else {
            return false;
          }
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