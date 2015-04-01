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
    'ui.bootstrap', 'ui.router', 'ngCookies', 'ngAnimate', 'ngMessages', 'checklist-model', 'app.controllers', 'app.directives', 'app.services'
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
      .state('entityDetails', {
        controller: 'EntityDetailsCtrl',
        templateUrl: 'html/entityDetailsTpl.html'
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
          clustersList: ['Falcon', function(Falcon) {
            return Falcon.getEntities('cluster').then(
              function(response) {
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
          clustersList: ['Falcon', function(Falcon) {
            return Falcon.getEntities('cluster').then(
              function(response) {
                return response.data;
              });
          }]
        }
      })
      .state('forms.process.io', {
        templateUrl: 'html/process/processFormInputsAndOutputsStepTpl.html',
        controller: 'ProcessInputsAndOutputsCtrl',
        resolve: {
          feedsList: ['Falcon', function(Falcon) {
            return Falcon.getEntities('feed').then(
              function(response) {
                return response.data;
              });
          }]
        }
      })
      .state('forms.process.summary', {
        templateUrl: 'html/process/processFormSummaryStepTpl.html',
        controller: 'ProcessSummaryCtrl'
      });
    
  }]);

  app.run(['$rootScope', 
           function ($rootScope) {	
    
    $rootScope.$on('$stateChangeError',
      function(event, toState, toParams, fromState, fromParams, error){
        console.log('Manual log of stateChangeError: ' + error);
      });
		
  }]);

})();