/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
(function(exports) {
  "use strict";

  var USER_ID = 'falcon-dashboard';

  function onError(msg) {
    $('#alert-panel-body').html(msg);
    $('#alert-panel').alert();
    $('#alert-panel').show();
  }

  function ajax_impl(options) {
    // $.extend(options, add_user(options.url));
    return $.ajax(options);
  }

  function add_user(url) {
    var paramSeparator = (url.indexOf('?') != -1) ? '&' : '?';
    return url + paramSeparator + 'user.name=' + USER_ID;
  }

  function getJson_impl(url, success) {
    return ajax_impl({
      'dataType': 'json',
      'url': add_user(url),
      'success': success
    });
  }

  function getText_impl(url, success) {
    return ajax_impl({
      'dataType': 'text',
      'url': add_user(url),
      'success': success
    });
  }

  var falcon = {
    loadTemplate: function(tmpl_name, url, success) {
      $.get(url, function (data) {
        dust.loadSource(dust.compile(data, tmpl_name));
        success();
      }).fail(function() { onError('Cannot load the application.'); });
    },

    ajax: ajax_impl,
    getJson: getJson_impl,
    getText: getText_impl,
    ajaxFailureHandler: function(jqXHR, status, err) {
      if (jqXHR.status !== 0) {
        onError('Failed to load data. Error: ' + jqXHR.status + ' ' + err);
      }
    },

    /**
     * Calling the REST API recursively to get the dependency graph
     **/
    loadDependencyGraph: function(entity_type, entity_name, done_callback) {
      var nodes = {};
      var next_node_id = 0;

      var requests_in_fly = 0;

      function key(type, name) {
        return type + '/' + name;
      }

      function getOrCreateNode(type, name) {
        var k = key(type, name);
        if (nodes[k] !== undefined)
          return nodes[k];

        var n = {
          "id": next_node_id++,
          "type": type,
          "name": name,
          "dependency": []
        };
        nodes[k] = n;
        return n;
      }

      function loadEntry(node) {
        var type = node.type, name = node.name, k = key(type, name);
        getJson_impl(
          'api/entities/dependencies/' + type + '/' + name,
          function (data) {
            if (data.entity == null)
              return;

            if (!($.isArray(data.entity)))
              data.entity = new Array(data.entity);

            var l = data.entity.length;
            for (var i = 0; i < l; ++i) {
              var e = data.entity[i];
              /**
               * The REST API provides both the in and the out egeds
               * of the dependency graph. Here we add the edeges based
               * on the rules. (-> means the dependency edge)
               *
               * Feed->cluster, process->feed, process->cluster
               */
              var d = getOrCreateNode(e.type, e.name);
              var src = null, dst = null;
              if (d.type === "cluster") {
                src = node; dst = d;
              } else if (d.type === "process") {
                src = d; dst = node;
              } else {
                if (node.type === "cluster") {
                  src = d; dst = node;
                } else {
                  src = node; dst = d;
                }
              }
              console.log(src.name + '->' + dst.name);
              src.dependency.push(dst.id);
            }

            done_callback(nodes);
          })
      }

      function load() {
        var n = getOrCreateNode(entity_type, entity_name);
        loadEntry(n);
      }
      load();
    }
  };

  exports.falcon = falcon;
})(window);
