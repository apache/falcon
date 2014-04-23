/*
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

(function(falcon) {
  "use strict";

  dust.loadSource(dust.compile($('#lineage-info-tmpl').html(), 'info'));
  var CIRCLE_RADIUS = 12, RANK_SEPARATION = 120, LABEL_WIDTH = 120, LABEL_HEIGHT = 80, LABEL_PADDING = 20;
  var PREFIX = '/api/graphs/lineage';

  var data = {
    queue : {},
    nodes : {},
    edges : {},
    active_node_id : null
  };

  function process_queue(done_cb) {
    var q = data.queue;
    if (q.length == 0) {
      done_cb();
      return;
    }

    function filter_node(n) {
      return n.type === 'process-instance' || n.type === 'feed-instance';
    }

    function is_nonterminal_label(e) {
      return e._label === 'input' || e._label === 'output';
    }

    function visit_neighbor(p) {
      var vid = p.id, depth = p.depth;
      if (depth == 0) {
        process_queue(done_cb);
        return;
      }
      falcon.getJson(PREFIX + '/vertices/' + vid + '/both', function(resp) {
        for (var i = 0; i < resp.results.length; ++i) {
          var n = resp.results[i];
          if (data.nodes[n._id] !== undefined || !filter_node(n)) {
            continue;
          }
          data.nodes[n._id] = n;
          q.push({'id': n._id, 'depth': depth - 1});
        }
      }).fail(falcon.ajaxFailureHandler)
        .always(function() {
          process_queue(done_cb);
        });
    }

    var v = data.queue.pop();
    falcon.getJson(PREFIX + '/vertices/' + v.id + '/bothE', function(resp) {
      var terminal_has_in_edge = false, terminal_has_out_edge = false;
      var node = data.nodes[v.id];
      for (var i = 0; i < resp.results.length; ++i) {
        var e = resp.results[i];
        if (is_nonterminal_label(e)) {
          terminal_has_in_edge = terminal_has_in_edge || e._inV == v.id;
          terminal_has_out_edge = terminal_has_out_edge || e._outV == v.id;
        }
        if (data.edges[e._id] !== undefined) {
          continue;
        }
        data.edges[e._id] = e;
      }
      node.is_terminal = !(terminal_has_in_edge && terminal_has_out_edge);
    }).fail(falcon.ajaxFailureHandler)
      .always(function () {
        visit_neighbor(v);
      });
  }

  function draw_graph() {
    var svg = d3.select('#lineage-graph').html('').append('svg:g');

    var LINE_FUNCTION = d3.svg.line()
      .x(function(d) { return d.x; })
      .y(function(d) { return d.y; })
      .interpolate('basis');

    function draw_node_functor(node_map) {
      function expand_node(d) {
        return function() {
          data.queue.push({'id': d._id, 'depth': 1});
          update_graph();
        };
      }

      function show_info_functor(d) {
        return function() {
          svg.selectAll('.lineage-node').classed('lineage-node-active', false);
          d3.select(this).classed('lineage-node-active', true);
          data.active_node_id = d._id;
          falcon.getJson(PREFIX + '/vertices/properties/' + d._id + '?relationships=true', function(resp) {
            dust.render('info', resp, function(err, out) {
              $('#lineage-info-panel').html(out);
            });
          }).fail(falcon.ajaxFailureHandler);
        };
      }

      return function(id, d) {
        var data = node_map[id];
        var r = svg.append('g')
          .attr('transform', 'translate(' + d.x + ',' + d.y + ')');

        var c = r.append('circle')
          .attr('data-node-id', id)
          .attr('class', 'lineage-node lineage-node-' + data.type)
          .attr('r', CIRCLE_RADIUS)
          .on('click', show_info_functor(data));

        if (!data.is_terminal) {
          c.on('dblclick', expand_node(data));
        } else {
          c.classed('lineage-node-terminal', true);
        }

        var name = node_map[id].name;
        r.append('title').text(name);

        var fo = r.append('foreignObject')
          .attr('transform', 'translate(' + (-LABEL_WIDTH / 2) + ', ' + LABEL_PADDING + ' )')
          .attr('width', LABEL_WIDTH)
          .attr('height', LABEL_HEIGHT);

        fo.append('xhtml:div').text(name)
          .attr('class', 'lineage-node-text');
      };
    }

    function draw_edge(layout) {
      return function(e, u, v, value) {
        var r = svg.append('g').attr('class', 'lineage-link');
        r.append('path')
          .attr('marker-end', 'url(#arrowhead)')
          .attr('d', function() {
            var points = value.points;

            var source = layout.node(u);
            var target = layout.node(v);

            var p = points.length === 0 ? source : points[points.length - 1];

            var r = CIRCLE_RADIUS;
            var sx = p.x, sy = p.y, tx = target.x, ty = target.y;
            var l = Math.sqrt((tx - sx) * (tx - sx) + (ty - sy) * (ty - sy));
            var dx = r / l * (tx - sx), dy = r / l * (ty - sy);

            points.unshift({'x': source.x, 'y': source.y});
            points.push({'x': target.x - dx, 'y': target.y - dy});
            return LINE_FUNCTION(points);
        });
      };
    }

    var node_map = {};
    var g = new dagre.Digraph();
    for (var k in data.nodes) {
      var n = data.nodes[k];
      g.addNode(n._id, { 'width': CIRCLE_RADIUS * 2 + LABEL_WIDTH, 'height': CIRCLE_RADIUS * 2 + LABEL_HEIGHT});
      node_map[n._id] = n;
    }

    for (var k in data.edges) {
      var e = data.edges[k];
      var src = node_map[e._inV], dst = node_map[e._outV];
      if (src !== undefined && dst !== undefined) {
        g.addEdge(null, e._outV, e._inV);
      }
    }

    var layout = dagre.layout().rankSep(RANK_SEPARATION).rankDir('LR').run(g);
    layout.eachEdge(draw_edge(layout));
    layout.eachNode(draw_node_functor(node_map));

    function post_render() {
      svg
        .append('svg:defs')
        .append('svg:marker')
        .attr('id', 'arrowhead')
        .attr('viewBox', '0 0 10 10')
        .attr('refX', 8)
        .attr('refY', 5)
        .attr('markerUnits', 'strokeWidth')
        .attr('markerWidth', 8)
        .attr('markerHeight', 5)
        .attr('orient', 'auto')
        .attr('style', 'fill: #ccc')
        .append('svg:path')
        .attr('d', 'M 0 0 L 10 5 L 0 10 z');
    }

    var bb = layout.graph();
    $('#lineage-graph').attr('width', bb.width);
    $('#lineage-graph').attr('height', bb.height);

    post_render();
  }

  function update_graph() {
    process_queue(function() {
      draw_graph();
      var id = data.active_node_id;
      if (id !== null) {
        d3.select('.node[data-node-id="' + id + '"]').classed('node-active', true);
      }
    });
  }

  falcon.load_lineage_graph = function(entityId, instance_name) {
    var node_name = entityId + '/' + instance_name;
    falcon.getJson(PREFIX + '/vertices?key=name&value=' + node_name, function(resp) {
      var n = resp.results[0];
      data.queue = [{'id': n._id, 'depth': 1}];
      data.nodes = {};
      data.nodes[n._id] = n;
      update_graph();
      $('#lineage-modal').modal('show');
    }).fail(falcon.ajaxFailureHandler);
  };
})(falcon);
