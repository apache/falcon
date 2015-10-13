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

  var falconModule = angular.module('app.services.falcon', ['app.services.x2js', 'ngCookies']);

  falconModule.factory('Falcon', ["$http", "X2jsService", "$location", '$rootScope', '$cookieStore', '$timeout',
    function ($http, X2jsService, $location, $rootScope, $cookieStore, $timeout) {

      var Falcon = {},
        NUMBER_OF_ENTITIES = 10,
        NUMBER_OF_INSTANCES = 11; // 10 + 1 for next page

      function buildURI(uri) {
        if ($rootScope.ambariView()) {
          uri = uri.substring(2);
          uri = $rootScope.serviceURI + uri;
        } else {
          if(!$rootScope.secureMode){
            uri = add_user(uri);
          }
        }
        console.log(uri);
        return uri;
      }

      function add_user(uri) {
        var userToken = $cookieStore.get('userToken');
        var paramSeparator = (uri.indexOf('?') !== -1) ? '&' : '?';
        uri = uri + paramSeparator + 'user.name=' + userToken.user;
        return uri;
      }

      //response Order
      Falcon.orderBy = {
        enable: false,
        name: "asc"
      };

      //-------------Server RESPONSE----------------------//
      Falcon.responses = {
        display: true,
        queue: [],
        count: {pending: 0, success: 0, error: 0},
        multiRequest: {cluster: 0, feed: 0, process: 0},
        listLoaded: {cluster: false, feed: false, process: false}
      };

      Falcon.logRequest = function () {
        Falcon.responses.count.pending = Falcon.responses.count.pending + 1;
      };

      Falcon.responses.showAll = false;
      Falcon.responses.isVisible = false;

      Falcon.hide = function () {
        Falcon.hideTimeout = $timeout(function () {
          $(".notifs").fadeOut(300);
        }, 5000);
      };

      Falcon.responses.unreaded = 0;
      Falcon.notify = function (showAll) {
        $(".notifs").stop();
        $timeout.cancel(Falcon.hideTimeout);

        if (showAll) {
          Falcon.responses.unreaded = 0;
          if (Falcon.responses.isVisible) {
            Falcon.responses.isVisible = false;
            $(".notifs").fadeOut(300);
          } else {
            Falcon.responses.isVisible = true;
            $(".notifs").hide();
            $(".notifs").fadeIn(300);
          }
          Falcon.responses.showAll = true;
        } else {
          Falcon.responses.unreaded++;
          Falcon.responses.isVisible = false;
          $(".notifs").stop();
          $(".notifs").hide();
          $(".notifs").fadeIn(300);
          $(".notifs").fadeOut(300);
          $(".notifs").fadeIn(300);
          $(".notifs").fadeOut(300);
          $(".notifs").fadeIn(300);
          Falcon.hide();
          Falcon.responses.showAll = false;
        }
      };

      Falcon.hideNotifs = function () {
        $(".notifs").stop();
        $timeout.cancel(Falcon.hideTimeout);
        Falcon.responses.isVisible = false;
        $(".notifs").fadeOut(300);
        Falcon.responses.queue.forEach(function(notifMsg) {
          notifMsg.readed = true;
        });
      };

      Falcon.logResponse = function (type, messageObject, entityType, hide) {
        if (type === 'success') {
          if (!hide) {
            var response = {
              success: true,
              type: "success",
              status: messageObject.status,
              message: messageObject.message,
              requestId: messageObject.requestId,
              readed: false
            };
            Falcon.responses.queue.push(response);
            Falcon.responses.count.success = Falcon.responses.count.success + 1;
            Falcon.notify(false);
          }
          Falcon.responses.count.pending = Falcon.responses.count.pending - 1;
        }
        if (type === 'cancel') {
          if (!hide) {
            var response = {
              success: true,
              type: "cancel",
              status: messageObject.state,
              state: messageObject.state,
              message: messageObject.message,
              model: messageObject.model,
              readed: false
            };
            Falcon.responses.queue.push(response);
            Falcon.notify(false);
            return;
          }
        }
        if (type === 'error') {
          if (messageObject.status !== undefined) {
            var response = {
              success: false,
              type: "error",
              status: messageObject.status,
              message: messageObject.message,
              requestId: messageObject.requestId,
              readed: false
            };
          } else {
            if (messageObject.slice(0, 6) !== "Cannot") {
              var errorMessage = X2jsService.xml_str2json(messageObject);
              var response = {
                success: false,
                type: "error",
                status: errorMessage.result ? errorMessage.result.status : 'error',
                message: errorMessage.result ? errorMessage.result.message : 'Unexpected Error',
                requestId: errorMessage.result ? errorMessage.result.requestId : 0,
                readed: false
              };
            }
            else {
              var response = {
                success: false,
                type: "error",
                status: "No connection",
                message: messageObject,
                requestId: "no ID",
                readed: false
              };
            }
          }
          if (hide) {
            Falcon.responses.count.pending = Falcon.responses.count.pending - 1;
            return; //>> just takes out the pending request and returns from the func
          }
          Falcon.responses.queue.push(response);
          Falcon.responses.count.error = Falcon.responses.count.error + 1;
          Falcon.responses.count.pending = Falcon.responses.count.pending - 1;
          Falcon.notify(false);
        }
        if (type === 'warning') {
          if (!hide) {
            var response = {
              success: true,
              type: "warning",
              status: messageObject.status,
              message: messageObject.message,
              model: '',
              readed: false
            };
            Falcon.responses.queue.push(response);
            Falcon.notify(false);
            return;
          }
        }
        if (entityType && entityType !== false) {
          entityType = entityType.toLowerCase();
          Falcon.responses.multiRequest[entityType] = Falcon.responses.multiRequest[entityType] - 1;
        }

      };
      Falcon.removeMessage = function (index) {
        if (index === -1) {
          index = Falcon.responses.queue.length - 1;
        }
        if (Falcon.responses.queue[index].success) {
          Falcon.responses.count.success = Falcon.responses.count.success - 1;
        }
        else {
          Falcon.responses.count.error = Falcon.responses.count.error - 1;
        }
        Falcon.responses.queue.splice(index, 1);
      };


      Falcon.errorMessage = function (message) {
        var response = {
          type: "error",
          message: message,
          readed: false
        };
        Falcon.responses.queue.push(response);
        Falcon.notify(false);
      };
      Falcon.warningMessage = function (message) {
        var response = {
          type: "warning",
          message: message,
          readed: false
        };
        Falcon.responses.queue.push(response);
        Falcon.notify(false);
      };

      //-------------METHODS-----------------------------//

      Falcon.getServerConfig = function () {
        return $http.get('../api/admin/version?user.name=falcon');
      };

      Falcon.getServerStack = function () {
        return $http.get(buildURI('../api/admin/stack'));
      };

      Falcon.clearUser = function () {
        return $http.get('../api/admin/clearuser?user.name=falcon');
      };

      Falcon.getCurrentUser = function () {
        return $http.get('../api/admin/getuser');
      };

      Falcon.postValidateEntity = function (xml, type) {
        return $http.post(buildURI('../api/entities/validate/' + type), xml, {headers: {'Content-Type': 'text/plain'}});
      };
      Falcon.postSubmitEntity = function (xml, type) {
        return $http.post(buildURI('../api/entities/submit/' + type), xml, {headers: {'Content-Type': 'text/plain'}});
      };
      Falcon.postUpdateEntity = function (xml, type, name) {
        return $http.post(buildURI('../api/entities/update/' + type + '/' + name), xml, {headers: {'Content-Type': 'text/plain'}});
      };

      Falcon.postScheduleEntity = function (type, name) {
        return $http.post(buildURI('../api/entities/schedule/' + type + '/' + name));
      };
      Falcon.postSuspendEntity = function (type, name) {
        return $http.post(buildURI('../api/entities/suspend/' + type + '/' + name));
      };
      Falcon.postResumeEntity = function (type, name) {
        return $http.post(buildURI('../api/entities/resume/' + type + '/' + name));
      };

      Falcon.deleteEntity = function (type, name) {
        return $http.delete(buildURI('../api/entities/delete/' + type + '/' + name));
      };

      Falcon.getEntities = function (type) {
        return $http.get(buildURI('../api/entities/list/' + type + '?fields=status,tags&numResults=' + NUMBER_OF_ENTITIES));
      };

      Falcon.getEntityDefinition = function (type, name) {
        return $http.get(buildURI('../api/entities/definition/' + type + '/' + name), {headers: {'Accept': 'text/plain'}});
      };

      Falcon.searchEntities = function (name, tags, entityType, offset, order) {
        var searchUrl = "../api/entities/list/";
        if (entityType !== undefined && entityType !== "") {
          if (entityType === "mirror") {
            if (tags === undefined || tags === "") {
              tags = "_falcon_mirroring_type";
            } else {
              tags += ",_falcon_mirroring_type";
            }
            searchUrl += "process";
          } else {
            searchUrl += entityType;
          }
        } else {
          //searchUrl += "all";
          //searchUrl += "schedulable";
          searchUrl += "feed,process";
        }
        searchUrl += "?fields=clusters,tags,status";
        if (name !== undefined && name !== "") {
          if (name !== "*") {
            searchUrl += "&nameseq=" + name;
          }
        }
        if (tags !== undefined && tags !== "") {
          searchUrl += "&tagkeys=" + tags;
        }
        if (offset !== undefined && offset !== "") {
          searchUrl += '&offset=' + offset + '&numResults=' + NUMBER_OF_ENTITIES;
        }
        if (Falcon.orderBy.enable) {
          searchUrl += '&orderBy=name&sortOrder=' + Falcon.orderBy.name;
        }
        return $http.get(buildURI(searchUrl));
      };

      Falcon.searchInstances = function (type, name, offset, start, end, status, orderBy, sortOrder) {
        //var searchUrl = "../api/instance/running/" + type + "/" + name + "?colo=*";
        //var searchUrl = "../api/instance/list/" + type + "/" + name + "?colo=*";
        var searchUrl = "../api/instance/logs/" + type + "/" + name + "?colo=*";
        if (start !== undefined && start !== "") {
          searchUrl += "&start=" + start;
        }
        if (end !== undefined && end !== "") {
          searchUrl += "&end=" + end;
        }
        if (status !== undefined && status !== "") {
          searchUrl += "&filterBy=STATUS:" + status;
        }
        if (orderBy !== undefined && orderBy !== "") {
          searchUrl += "&orderBy=" + orderBy;
        } else {
          searchUrl += "&orderBy=startTime";
        }
        if (sortOrder !== undefined && sortOrder !== "") {
          searchUrl += "&sortOrder=" + sortOrder;
        }
        searchUrl += '&offset=' + offset + '&numResults=' + NUMBER_OF_INSTANCES;
        return $http.get(buildURI(searchUrl));
      };

      Falcon.getInstanceLog = function (logUrl) {
        return $http.get(buildURI(logUrl), {headers: {'Accept': 'text/plain'}});
      };

      Falcon.postResumeInstance = function (entityType, entityName, start, end) {
        return $http.post(buildURI('../api/instance/resume/' + entityType + '/' + entityName + '?colo=*&start=' + start + '&end=' + end));
      };

      Falcon.postSuspendInstance = function (entityType, entityName, start, end) {
        return $http.post(buildURI('../api/instance/suspend/' + entityType + '/' + entityName + '?colo=*&start=' + start + '&end=' + end));
      };

      Falcon.postKillInstance = function (entityType, entityName, start, end) {
        return $http.post(buildURI('../api/instance/kill/' + entityType + '/' + entityName + '?colo=*&start=' + start + '&end=' + end));
      };

      Falcon.postReRunInstance = function (entityType, entityName, start, end) {
        return $http.post(buildURI('../api/instance/rerun/' + entityType + '/' + entityName + '?colo=*&start=' + start + '&end=' + end));
      };

      Falcon.getEntityDependencies = function (type, name) {
        return $http.get(buildURI('../api/entities/dependencies/' + type + '/' + name));
      };

      Falcon.getInstanceVertices = function (value) {
        return $http.get(buildURI('../api/metadata/lineage/vertices?key=name&value=' + value));
      };

      Falcon.getInstanceVerticesDirection = function (id, direction) {
        return $http.get(buildURI('../api/metadata/lineage/vertices/' + id + '/' + direction));
      };

      Falcon.getInstanceVerticesProps = function (id) {
        return $http.get(buildURI('../api/metadata/lineage/vertices/properties/' + id + '?relationships=true'));
      };

      //----------------------------------------------//
      Falcon.getInstancesSummary = function (type, mode, from, to) {
        return $http.get(buildURI('../api/instance/summary/' + type + '/' + mode + '?start=' + from + '&end=' + to));
      };
      Falcon.getTopEntities = function (entityType, from, to) {
        return $http.get(buildURI('../api/entities/top/' + entityType + '?start=' + from + '&end=' + to));
      };
      //----------------------------------------------//
      Falcon.postSubmitRecipe = function (recipe) {
        return $http.post(buildURI('../api/entities/prepareAndSubmitRecipe'), recipe, {headers: {'Content-Type': 'text/plain'}});
      };

      return Falcon;

    }]);

})();
