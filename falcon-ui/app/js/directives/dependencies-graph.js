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

  entitiesListModule.controller('DependenciesGraphCtrl', ['$scope', 'Falcon', 'X2jsService', '$window', 'EncodeService',
    function($scope, Falcon, X2jsService, $window, encodeService) {



    }]);

  entitiesListModule.directive('dependenciesGraph', ["$timeout", 'Falcon', '$filter', function($timeout, Falcon, $filter) {
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

          function getOrCreateNode(type, name, tag) {
            var k = key(type, name);
            if (nodes[k] !== undefined)
              return nodes[k];

            var n = {
              "guid": next_node_id++,
              "type": type,
              "name": name,
              "arrowDirections": tag,
              "children": []
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
                    var tag = e.hasOwnProperty('tags') && e.tags !== null ? e.tags.tag : "Input";
                    var d = getOrCreateNode(e.type, e.name, tag);
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
                    src.children.push(d);
                  }

                  done_callback(node);
                })
                .error(function (err) {
                  Falcon.logResponse('error', err, false, true);
                });
          }

          function load() {
            var n = getOrCreateNode(entity_type, entity_name, "Input");
            loadEntry(n);
          }
          load();
        };

        var plotDependencyGraph = function(nodes, container) {
          var element = d3.select(container.element),
              width = Math.max(container.width, 960),
              height = Math.max(container.height, 300);

          var margin = {
            top: 30,
            right: 30,
            bottom: 30,
            left: 80
          };
          width = width - margin.right - margin.left;
          height = height - margin.top - margin.bottom;
          // Function to draw the lines of the edge
          var i = 0;


          var tree = d3.layout.tree()
              .size([height, width]);

          var diagonal = d3.svg.diagonal()
              .projection(function(d) {
                return [d.y, d.x];
              });

          var svg = element.select('svg')
              .attr('width', width + margin.right + margin.left)
              .attr('height', height + margin.top + margin.bottom)
              .select('g')
              .attr('transform',
              'translate(' + margin.left + ',' + margin.right + ')');

          svg.append("svg:defs").append("svg:marker").attr("id", "output-arrow").attr("viewBox", "0 0 10 10")
              .attr("refX", 20).attr("refY", 5).attr("markerUnits", "strokeWidth").attr("markerWidth", 6)
              .attr("markerHeight", 9).attr("orient", "auto").append("svg:path").attr("d", "M 0 0 L 10 5 L 0 10 z");

          //marker for input type graph
          svg.append("svg:defs")
              .append("svg:marker")
              .attr("id", "input-arrow")
              .attr("viewBox", "0 0 10 10")
              .attr("refX", -7)
              .attr("refY", 5)
              .attr("markerUnits", "strokeWidth")
              .attr("markerWidth", 6)
              .attr("markerHeight", 9)
              .attr("orient", "auto")
              .append("svg:path")
              .attr("d", "M -2 5 L 8 0 L 8 10 z");

          var root = nodes;
          function update(source) {

            // Compute the new tree layout.
            var nodes1 = tree.nodes(source).reverse(),
                links = tree.links(nodes1);
            // Normalize for fixed-depth.
            nodes1.forEach(function(d) {
              d.y = d.depth * 180;
            });

            // Declare the nodes�
            var node = svg.selectAll('g.node')
                .data(nodes1, function(d) {

                  return d.id || (d.id = ++i);
                });
            // Enter the nodes.
            var nodeEnter = node.enter().append('g')
                .attr('class', 'node')
                .attr('transform', function(d) {
                  return 'translate(' + d.y + ',' + d.x + ')';
                });

            nodeEnter.append("image")
                .attr("xlink:href", function(d) {
                  //return d.icon;
                  return d.type === 'cluster' ? 'css/img/cloud.png' : 'css/img/feed.png';
                })
                .attr("x", "-18px")
                .attr("y", "-18px")
                .attr("width", "34px")
                .attr("height", "34px");

            nodeEnter.append('text')
                .attr('x', function(d) {
                  return d.children || d._children ?
                  (5) * -1 : +15;
                })
                .attr('dy', '-1.75em')
                .attr('text-anchor', function(d) {
                  return d.children || d._children ? 'middle' : 'middle';
                })
                .text(function(d) {
                  return d.name;
                })

                .style('fill-opacity', 1);

            // Declare the links�
            var link = svg.selectAll('path.link')
                .data(links, function(d) {
                  return d.target.id;
                });

            link.enter().insert('path', 'g')
                .attr('class', 'link')
              //.style('stroke', function(d) { return d.target.level; })
                .style('stroke', 'green')
                .attr('d', diagonal);
            link.attr("marker-start", function (d) {
              if(d.target.arrowDirections==="Input")
                return "url(#input-arrow)";
            }); //if input
            link.attr("marker-end", function (d) {
              if(d.target.arrowDirections==="Output")
                return "url(#output-arrow)";
            }); //if outPut

          }

          update(root);


        };

        var visualizeDependencyGraph = function(type, name) {
          loadDependencyGraph(type, name, function(nodes) {
            plotDependencyGraph(nodes, {element:element[0], height:element[0].offsetHeight,width:element[0].offsetWidth});
          });
        };

        //console.log(scope.type + " " + scope.name);
        visualizeDependencyGraph(scope.type, scope.name);

      }
    };
  }]);

})();