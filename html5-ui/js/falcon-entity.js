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
(function(falcon, dust) {
  var TYPE_MAP = {
    "feed": "Data set",
    "process": "Process",
    "cluster": "Cluster"
  };

  dust.loadSource(dust.compile($('#dependency-tmpl').html(), 'dependency'));
  dust.loadSource(dust.compile($('#instance-tmpl').html(), 'instance'));

  var entityType = (/type=(.+?)(&|$)/.exec(location.search)||[,null])[1];
  var entityId = (/id=(.+?)(&|$)/.exec(location.search)||[,null])[1];

  function url(type) {
    return 'api/' + type + '/' + entityType + '/' + entityId;
  }

  $('#breadcrumb-type').text(TYPE_MAP[entityType]);
  $('#entity-title').addClass('entity-link-' + entityType)
    .text(entityId + ' ')
    .append('<span class="label label-success">' + TYPE_MAP[entityType] + '</span>');

  function switchDependencyView(to_shown) {
    var orig = to_shown === 'list' ? 'graph' : 'list';
    $('#dep-view-' + orig).hide();
    $('#btn-dep-' + orig).addClass('btn-default').removeClass('btn-primary');
    $('#dep-view-' + to_shown).show();
    $('#btn-dep-' + to_shown).addClass('btn-primary').removeClass('btn-default');
  }

  function loadInstance(start, end) {
    falcon.getJson(url('instance/status') + '?start=' + start + '&end=' + end, function (data) {
      if (data.instances == null)
        return;

      if (!($.isArray(data.instances)))
        data.instances = new Array(data.instances);

      dust.render('instance', data, function(err, out) {
        $('#panel-instance > .panel-body').html(out);
        $('.lineage-href').click(function() {
          falcon.load_lineage_graph(entityId, $(this).attr('data-instance-name'));
        });
        $('.instance-hdfs-log').tooltip();
        $('#panel-instance').show();
      });
    }).fail(falcon.ajaxFailureHandler);
  }

  function loadDependency() {
    falcon.getJson(url('entities/dependencies'), function (data) {
      if (data.entity == null)
        return;

      if (!($.isArray(data.entity)))
        data.entity = new Array(data.entity);

      data.entity.sort(function(l,r) {
        var a = l.type, b = r.type;
        return a < b ? -1 : (a > b ? 1 : 0);
      });

      dust.render('dependency', data, function(err, out) {
        $('#dep-view-list').html(out);
        switchDependencyView('list');
        $('#panel-dependency').show();
      });
    }).fail(falcon.ajaxFailureHandler);
  }

  function load() {
    var isCluster = entityType === 'cluster';

    falcon.getText(url('entities/definition'), function (data) {
      $('#entity-def-textarea')
        .attr('rows', data.match(/\n/g).length + 1)
        .text(data);

      if (!isCluster) {
        var xml = $.parseXML(data);
        var e = $(xml).find('validity');
        if (e != null) {
          loadInstance(e.attr('start'), e.attr('end'));
        }
      }
    }).fail(falcon.ajaxFailureHandler);

    if (!isCluster) {
      loadDependency();
    }
  }

  /**
   * Plot dependency graph in SVG format
   **/
  function plotDependencyGraph(nodes, svg) {
    var NODE_WIDTH  = 150;
    var NODE_HEIGHT = 60;
    var RECT_ROUND  = 5;
    var SEPARATION  = 40;
    var UNIVERSAL_SEP = 80;

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

      var rect = root.append('rect')
        .attr('width', value.width)
        .attr('height', value.height)
        .attr('x', value.x)
        .attr('y', value.y)
        .attr('rx', RECT_ROUND)
        .attr('ry', RECT_ROUND);

      var fo = root.append('foreignObject')
        .attr('x', value.x)
        .attr('y', value.y)
        .attr('width', value.width)
        .attr('height', value.height);

      var txt = fo.append('xhtml:div')
        .text(node.name)
        .classed('node-name', true)
        .classed('node-name-' + node.type, true);
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

      $('#entity-dep-graph').attr('width', graph.width);
      $('#entity-dep-graph').attr('height', graph.height);
      postRender();
    }
    plot();
  }


  function visualizeDependencyGraph() {
    $('#dep-view-graph')
      .css('text-align', 'center')
      .html('<svg id="entity-dep-graph" height="200"><g></g></svg>');

    falcon.loadDependencyGraph(entityType, entityId, function(nodes) {
      plotDependencyGraph(nodes, d3.select('#entity-dep-graph'));
      switchDependencyView('graph');
    });
  }

  $('#btn-dep-list').click(function() {
    switchDependencyView('list');
  });
  $('#btn-dep-graph').click(visualizeDependencyGraph);
  load();
})(falcon, dust);
