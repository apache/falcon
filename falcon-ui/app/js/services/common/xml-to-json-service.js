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
  
  var servicesModule = angular.module('app.services.x2js', []);

  servicesModule.factory('X2jsService', function() {
    var x2js = new X2JS(
      {arrayAccessFormPaths: [
        'feed.properties.property',
        'feed.locations.location',
        'feed.clusters.cluster',
        'feed.clusters.cluster.locations.location',
        'cluster.properties.property',
        'cluster.locations.location',
        'process.clusters.cluster',
        'process.inputs.input',
        'process.outputs.output'
      ]});

    return {
      xml_str2json: function(string) {
        return x2js.xml_str2json(string);
      },

      json2xml_str: function(jsonObj) {
        return x2js.json2xml_str(jsonObj);
      },

      prettifyXml: function (xml) {
        var formatted = '';
        var reg = /(>)(<)(\/*)/g;
        xml = xml.replace(reg, '$1\r\n$2$3');
        var pad = 0;
        jQuery.each(xml.split('\r\n'), function(index, node) {
          var indent = 0;
          if (node.match( /.+<\/\w[^>]*>$/ )) {
            indent = 0;
          } else if (node.match( /^<\/\w/ )) {
            if (pad !== 0) {
              pad -= 1;
            }
          } else if (node.match( /^<\w[^>]*[^\/]>.*$/ )) {
            indent = 1;
          } else {
            indent = 0;
          }

          var padding = '';
          for (var i = 0; i < pad; i++) {
            padding += '  ';
          }

          formatted += padding + node + '\r\n';
          pad += indent;
        });

        return formatted;
      }
    };

  });

})();