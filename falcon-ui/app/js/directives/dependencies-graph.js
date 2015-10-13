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

	var entitiesListModule = angular.module('app.directives.dependencies-graph', ['app.services' ]);

  entitiesListModule.controller('DependenciesGraphCtrl', ['$scope', 'Falcon', 'X2jsService', '$window', 'EncodeService', 'EntityModel',
                                      function($scope, Falcon, X2jsService, $window, encodeService, EntityModel) {



  }]);

  entitiesListModule.directive('dependenciesGraph', ["$timeout", 'Falcon', '$filter', '$state', 'X2jsService', 'EntityModel',
                                              function($timeout, Falcon, $filter, $state, X2jsService, EntityModel) {
    return {
      scope: {
        type: "=",
        name: "="
      },
      controller: 'DependenciesGraphCtrl',
      restrict: "EA",
      templateUrl: 'html/directives/dependenciesGraphDv.html',
      link: function (scope, element) {

        var loadDependencyGraph = function(entity_type, entity_name, done_callback) {
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

            Falcon.logRequest();
            Falcon.getEntityDependencies(type, name)
                .success(function (data) {
                  Falcon.logResponse('success', data, false, true);

                  if (data.entity == null)
                    return;

                  if (!($.isArray(data.entity)))
                    data.entity = new Array(data.entity);

                  var l = data.entity.length;
                  for (var i = 0; i < l; ++i) {
                    var e = data.entity[i];
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
                    //console.log(src.name + '->' + dst.name);
                    src.dependency.push(dst.id);
                  }

                  done_callback(nodes);
                })
                .error(function (err) {
                  Falcon.logResponse('error', err, false, true);
                });
          }

          function load() {
            var n = getOrCreateNode(entity_type, entity_name);
            loadEntry(n);
          }
          load();
        };

        var plotDependencyGraph = function(nodes, element) {
          var NODE_WIDTH  = 150;
          var NODE_HEIGHT = 50;
          var RECT_ROUND  = 10;
          var SEPARATION  = 40;
          var UNIVERSAL_SEP = 80;

          var svg = d3.select(element).append("svg");

          // Function to draw the lines of the edge
          var LINE_FUNCTION = d3.svg.line()
              .x(function(d) { return d.x; })
              .y(function(d) { return d.y; })
              .interpolate('basis');

          // Mappining from id to a node
          var node_by_id = {};

          var layout = null;

          /**
           * Calculate the intersection point between the point p and the edges of the rectangle rect
           **/
          function intersectRect(rect, p) {
            var cx = rect.x, cy = rect.y, dx = p.x - cx, dy = p.y - cy, w = rect.width / 2, h = rect.height / 2;

            if (dx == 0)
              return { "x": p.x, "y": rect.y + (dy > 0 ? h : -h) };

            var slope = dy / dx;

            var x0 = null, y0 = null;
            if (Math.abs(slope) < rect.height / rect.width) {
              // intersect with the left or right edges of the rect
              x0 = rect.x + (dx > 0 ? w : -w);
              y0 = cy + slope * (x0 - cx);
            } else {
              y0 = rect.y + (dy > 0 ? h : -h);
              x0 = cx + (y0 - cy) / slope;
            }

            return { "x": x0, "y": y0 };
          }

          function drawNode(u, value) {
            var root = svg.append('g').classed('node', true)
                .attr('transform', 'translate(' + -value.width/2 + ',' + -value.height/2 + ')');

            var node = node_by_id[u];




            var fo = root.append('foreignObject')
                .attr('x', value.x)
                .attr('y', value.y)
                .attr('width', value.width)
                .attr('height', value.height)
                .attr('class', 'foreignObject');


            var txt = fo.append('xhtml:div')
                .text(node.name)
                .classed('node-name', true)
                .classed('node-name-' + node.type, true);

            var rect = root.append('rect')
              .attr('width', value.width)
              .attr('height', value.height)
              .attr('x', value.x)
              .attr('y', value.y)
              .attr('rx', RECT_ROUND)
              .attr('ry', RECT_ROUND)

              .on('click', function () {

                Falcon.logRequest();
                Falcon.getEntityDefinition(node.type.toLowerCase(), node.name)
                  .success(function (data) {
                    Falcon.logResponse('success', data, false, true);
                    var entityModel = X2jsService.xml_str2json(data);
                    EntityModel.type = node.type.toLowerCase();
                    EntityModel.name = node.name;
                    EntityModel.model = entityModel;
                    $state.go('entityDetails');
                  })
                  .error(function (err) {
                    Falcon.logResponse('error', err, false, false);
                  });


              });

          }

          function drawEdge(e, u, v, value) {
            var root = svg.append('g').classed('edge', true);

            root.append('path')
                .attr('marker-end', 'url(#arrowhead)')
                .attr('d', function() {
                  var points = value.points;

                  var source = layout.node(u);
                  var target = layout.node(v);

                  var p0 = points.length === 0 ? target : points[0];
                  var p1 = points.length === 0 ? source : points[points.length - 1];

                  points.unshift(intersectRect(source, p0));
                  points.push(intersectRect(target, p1));

                  return LINE_FUNCTION(points);
                });
          }

          function postRender() {
            svg
                .append('svg:defs')
                .append('svg:marker')
                .attr('id', 'arrowhead')
                .attr('viewBox', '0 0 10 10')
                .attr('refX', 8)
                .attr('refY', 5)
                .attr('markerUnits', 'strokewidth')
                .attr('markerWidth', 8)
                .attr('markerHeight', 5)
                .attr('orient', 'auto')
                .attr('style', 'fill: #333')
                .append('svg:path')
                .attr('d', 'M 0 0 L 10 5 L 0 10 z');
          }

          function plot() {
            var g = new dagre.Digraph();

            for (var key in nodes) {
              var n = nodes[key];
              node_by_id[n.id] = n;
              g.addNode(n.id, { "width": NODE_WIDTH, "height": NODE_HEIGHT });
            }

            for (var key in nodes) {
              var n = nodes[key];
              for (var i = 0, l = n.dependency.length; i < l; ++i) {
                var d = n.dependency[i];
                g.addEdge(null, n.id, d);
              }
            }

            layout = dagre.layout()
                .universalSep(UNIVERSAL_SEP).rankSep(SEPARATION)
                .run(g);
            layout.eachEdge(drawEdge);
            layout.eachNode(drawNode);

            var graph = layout.graph();

            svg.attr("width", graph.width);
            svg.attr("height", graph.height);

            postRender();
          }
          plot();
        };

        var visualizeDependencyGraph = function(type, name) {
          loadDependencyGraph(type, name, function(nodes) {
            plotDependencyGraph(nodes, element[0]);
          });
        };

        //console.log(scope.type + " " + scope.name);
        visualizeDependencyGraph(scope.type, scope.name);

      }
    };
  }]);

})();