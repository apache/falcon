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

  var directivesModule = angular.module('app.directives', [
    'app.services',
    //'app.directives.entities-list',
    'app.directives.entities-search-list',
    'app.directives.instances-list',
    'app.directives.server-messages',
    'app.directives.entity',
    'app.directives.check-name',
    'app.directives.validation-message',
    'chart-module',
    'app.directives.dependencies-graph',
    'app.directives.lineage-graph',
    'tooltip',
    'app.directives.feed-cluster-partitions',
    'app.directives.acl-permissions',
    'app.directives.interface-endpoint'
  ]);

  directivesModule.directive('errorNav', function () {
    return {
      replace: false,
      restrict: 'A',
      templateUrl: 'html/error.html'
    };
  });

  directivesModule.directive('navHeader', function () {
    return {
      replace: false,
      restrict: 'A',
      templateUrl: 'html/directives/navDv.html',
      controller: 'HeaderController'
    };
  });

  //Angular is not supporting file inputs on change binding that is why this directive
  directivesModule.directive('fileinputChange', function () {
    return {
      restrict: "A",
      link: function (scope, element, attrs) {
        var onChangeFunc = element.scope()[attrs.fileinputChange];
        element.bind('change', onChangeFunc);
        element.bind('click', function () {
          this.value = '';
        });
      }
    };
  });

  directivesModule.factory('EncodeService', function () {
    return {
      encode: function (data) {
        return encodeURIComponent(data);
      }
    };
  });

  directivesModule.directive('frequency', function () {
    return {
      replace: false,
      scope: {
        value: "=",
        prefix: "@"
      },
      restrict: 'E',
      template: '{{output}}',
      link: function (scope) {
        if (scope.value.quantity) {
          scope.output = (scope.prefix ? scope.prefix + ' ' : '') + scope.value.quantity + ' ' + scope.value.unit;
        } else {
          scope.output = 'Not specified';
        }
      }
    };
  });

  directivesModule.directive('timeZoneSelect', function () {
    return {
      restrict: 'E',
      replace: false,
      scope: {
        ngModel: '=',
        required: '='
      },
      templateUrl: 'html/directives/timeZoneSelectDv.html'
    };
  });

  directivesModule.directive('simpleDate', ['$filter','DateHelper', function ($filter, DateHelper) {
    return {
      require: 'ngModel',
      link: function (scope, element, attrs, ngModelController) {
        var dateFormat = DateHelper.getLocaleDateFormat();

        element.attr('title','Date should be entered in '+ dateFormat.toLowerCase() + ' format.');

        ngModelController.$parsers.push(function (data) {
          //convert data from view format to model format
          return data;
        });
        ngModelController.$formatters.push(function (date) {
          //convert data from model format to view format
          if (date !== "") {
            date = $filter('date')(date, dateFormat);
          }
          return date;
        });
      }
    };
  }]);

  directivesModule.directive('ngEnter', function () {
    return function (scope, element, attrs) {
      element.bind("keydown keypress", function (event) {
        if (event.which === 13) {
          scope.$apply(function () {
            scope.$eval(attrs.ngEnter);
          });
          event.preventDefault();
        }
      });
    };
  });

  directivesModule.directive('elastic', ['$timeout', function ($timeout) {
    return {
      restrict: 'A',
      link: function ($scope, element) {
        $scope.$watch(function () {
          return element[0].value;
        }, function () {
          resize();
        });
        var resize = function () {
          element[0].style.resize = "vertical";
          element[0].style.height = "250px";
          return element[0].style.height = "" + element[0].scrollHeight + "px";
        };
        $timeout(resize, 0);
      }
    };
  }
  ]);

  directivesModule.directive('autofocus', ['$timeout', function ($timeout) {
    return {
      restrict: 'A',
      link: function ($scope, element) {
        $timeout(function () {
          element.trigger('focus');
        }, 20);
      }
    };
  }
  ]);

  directivesModule.filter('dateFormatter', function () {
    return function (date) {
      console.log(date);
      var dates = date.split('T')[0],
        time = date.split('T')[1].split('Z')[0].split('.')[0];
      return dates + ' ' + time;
    };
  });

  directivesModule.directive('onBlur', [function () {
    return {
      restrict: 'A',
      link: function (scope, elm, attrs) {
        elm.bind('blur', function () {
          if (attrs.onBlur)
            scope[attrs.onBlur]();
          else
            return false;
        });
      }
    };
  }]);

  directivesModule.directive('scrollToError', ['$timeout',function ($timeout) {
      return {
          require : "^form",
          restrict : 'A',
          link: function (scope, element,attrs,form) {
              element.on('mousedown',function(event){
                event.preventDefault();
              });
              element.on('click', function () {
                  var formElement = angular.element('form[name="' + form.$name + '"]');
                  var firstInvalid = formElement[0].querySelector('.ng-invalid');
                  $timeout(function() {
                    if (firstInvalid) {
                      firstInvalid.blur();
                      firstInvalid.focus();
                    }
                  },0)
              });
          }
      };
  }]);

  directivesModule.directive('feedFormClusterDetails', function () {
    return {
      replace: false,
      restrict: 'EA',
      templateUrl: 'html/feed/feedFormClusterDetailsTpl.html',
      link: function ($scope, $element) {
        $scope.$on('forms.feed.clusters:submit', function() {
          $scope.cluster.isAccordionOpened = $element.find('.ng-invalid').length > 0;
        });
      }
    };
  });

  directivesModule.directive('feedFormDataSource', function () {
    return {
      replace: false,
      restrict: 'EA',
      templateUrl: 'html/feed/feedFormDataSourceTpl.html',
      link: function ($scope, $element) {
        if($scope.feed.dataTransferType === 'import'){
          $scope.dataSourceType = 'source';
          $scope.dataTransferAction = 'extract';
          if ($scope.feed.import === undefined) {
            $scope.feed.import = { 'source' : {
              'extract' : {'type' : 'full', 'mergepolicy' : 'snapshot'}, 'columnsType' : 'all', 'fields' : {} } };
          } else {
            if ($scope.feed.import.source.fields && $scope.feed.import.source.fields.exlcudes) {
              $scope.feed.import.source.columnsType = 'exclude';
            } else if ($scope.feed.import.source.fields && $scope.feed.import.source.fields.includes) {
              $scope.feed.import.source.columnsType = 'include';
            } else {
              $scope.feed.import.source.columnsType = 'all';
            }
          }
        } else {
          $scope.dataSourceType = 'target';
          $scope.dataTransferAction = 'load';
          if ($scope.feed.export === undefined) {
            $scope.feed.export = { 'target' : {
              'load' : {'type' : 'updateonly' }, 'columnsType' : 'all', 'fields' : {} } };
          } else {
            if ($scope.feed.export.target.fields && $scope.feed.export.target.fields.exlcudes) {
              $scope.feed.export.target.columnsType = 'exclude';
            } else if ($scope.feed.export.target.fields && $scope.feed.export.target.fields.includes) {
              $scope.feed.export.target.columnsType = 'include';
            } else {
              $scope.feed.export.target.columnsType = 'all';
            }
          }
        }
      }
    };
  });

  directivesModule.directive('feedFormHiveStorage', ['EntityFactory',function (entityFactory) {
    return {
      replace: false,
      scope: {
        storageInfo:"=",
        add:"&",
        show:'&',
        toggleAdvancedOptions:'&',
        openDatePicker:'&',
        constructDate:'&',
        reset:'&',
        validations:'=',
        required:'='
      },
      restrict: 'EA',
      templateUrl: 'html/directives/feedFormHiveStorage.html',
      link: function ($scope) {
        $scope.valiationMessage= JSON.stringify($scope.validations.messages.number);
        if($scope.storageInfo.type ==='target'){
          $scope.buttonText = 'Destination';
        }else{
          $scope.buttonText = 'Source'
        }
        if(!$scope.storageInfo.clusterStorage){
          $scope.cluster = entityFactory.newCluster($scope.storageInfo.type, 'hive', "", null);
        }else{
          $scope.cluster = $scope.storageInfo.clusterStorage;
        }
        $scope.toggleAdvancedOptions = function(){
          $scope.showingAdvancedOptions = !$scope.showingAdvancedOptions;
        };
        $scope.reset = function(){
          $scope.storageInfo.feedClusters.forEach(function (cluster, index) {
            if (cluster.name === $scope.cluster.name && cluster.type === $scope.cluster.type) {
              $scope.storageInfo.feedClusters[index]
                = entityFactory.newCluster($scope.storageInfo.type, 'hive', '', null);
            }
          });
        };
        $scope.checkDuplicateClusterOnTarget = function() {
          if ($scope.cluster.type === 'target'
            && $scope.cluster.name !== ''
            && $scope.storageInfo.feedClusters.filter(function (cluster) {
            return cluster.name === $scope.cluster.name && cluster.type === 'source';
          }).length > 0) {
            return true;
          }
          return false;
        };
        $scope.findClusterExists = function(newClusterName, newClusterType, clusterList) {
          $scope.clusterExists = clusterList.filter(function (cluster) {
            return cluster.name === newClusterName && cluster.type === newClusterType;
          }).length > 1;
        };
        $scope.addCluster = function(clusterDetails){
          var cluster = entityFactory.newCluster(clusterDetails.type, clusterDetails.dataTransferType, "", null);
          $scope.storageInfo.feedClusters.unshift(cluster);
          //$scope.add({value : clusterDetails});
          //$scope.reset();
        };
        $scope.deleteCluster = function() {
          $scope.storageInfo.feedClusters.forEach(function (cluster, index) {
            if (cluster.name === $scope.cluster.name && cluster.type === $scope.cluster.type) {
              $scope.storageInfo.feedClusters.splice(index, 1);
            }
          });
        };
      }
    };
  }]);

  directivesModule.directive('feedFormHdfsStorage', ['EntityFactory',function (entityFactory) {
    return {
      replace: false,
      scope: {
        storageInfo:"=",
        add:"&",
        show:'&',
        toggleAdvancedOptions:'&',
        openDatePicker:'&',
        constructDate:'&',
        reset:'&',
        validations:'=',
        required:'='
      },
      restrict: 'EA',
      templateUrl: 'html/directives/feedFormHdfsStorage.html',
      require:"^form",
      link: function ($scope, $element, $attrs, $form) {
        $scope.valiationMessage= JSON.stringify($scope.validations.messages.number);
        if($scope.storageInfo.type ==='target'){
          $scope.buttonText = 'Destination';
        }else{
          $scope.buttonText = 'Source'
        }
        if(!$scope.storageInfo.clusterStorage){
          $scope.cluster = entityFactory.newCluster($scope.storageInfo.type, 'hdfs', "", null);
        }else{
          $scope.cluster = $scope.storageInfo.clusterStorage;
          if (!$scope.cluster.storage.fileSystem) {
            $scope.cluster.storage = { 'fileSystem' : entityFactory.newClusterFileSystem() };
          }
        }
        $scope.toggleAdvancedOptions = function(){
          $scope.showingAdvancedOptions = !$scope.showingAdvancedOptions;
        };
        $scope.reset = function(){
          $scope.storageInfo.feedClusters.forEach(function (cluster, index) {
            if (cluster.name === $scope.cluster.name && cluster.type === $scope.cluster.type) {
              $scope.storageInfo.feedClusters[index]
                = entityFactory.newCluster($scope.storageInfo.type, 'hdfs', '', null);
            }
          });
        };
        $scope.checkDuplicateClusterOnTarget = function() {
          if ($scope.cluster.type === 'target'
            && $scope.cluster.name !== ''
            && $scope.storageInfo.feedClusters.filter(function (cluster) {
            return cluster.name === $scope.cluster.name && cluster.type === 'source';
          }).length > 0) {
            return true;
          }
          return false;
        };
        $scope.findClusterExists = function(newClusterName, newClusterType, clusterList) {
          $scope.clusterExists = clusterList.filter(function (cluster) {
            return cluster.name === newClusterName && cluster.type === newClusterType;
          }).length > 1;
        };
        $scope.addCluster = function(clusterDetails, feedForm){
          var cluster = entityFactory.newCluster(clusterDetails.type, clusterDetails.dataTransferType, "", null);
          $scope.storageInfo.feedClusters.unshift(cluster);
          //$scope.add({value : clusterDetails});
          //$scope.reset();
        };
        $scope.deleteCluster = function() {
          $scope.storageInfo.feedClusters.forEach(function (cluster, index) {
            if (cluster.name === $scope.cluster.name && cluster.type === $scope.cluster.type) {
              $scope.storageInfo.feedClusters.splice(index, 1);
            }
          });
        };
      }
    };
  }]);

  directivesModule.directive('simpleDatePicker', ['$filter','DateHelper', function ($filter, DateHelper) {
    return {
      require: 'ngModel',
      link: function ($scope, $element, $attrs, ngModelController) {
        $element.datepicker();

        var dateFormat = DateHelper.getLocaleDateFormat();

        $element.attr('title','Date should be entered in '+ dateFormat.toLowerCase() + ' format.');

        ngModelController.$parsers.push(function (date) {
          //convert data from view format to model format
          return new Date(date);
        });
        ngModelController.$formatters.push(function (date) {
          //convert data from model format to view format
          if (date !== "") {
            date = $filter('date')(date, dateFormat);
          }
          return date;
        });
      }
    };
  }]);

  directivesModule.directive('mandatoryField', function () {
    return {
      replace: false,
      restrict: 'E',
      template: '<span>*</span>'
    };
  });

}());
