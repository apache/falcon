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
(function (falcon, dust) {
  var ENTRIES_PER_PAGE = 25;

  var source = $("#entity-list-tmpl").html();
  dust.loadSource(dust.compile(source, 'entity-list'));

  var entityType = (/type=(.+?)(&|$)/.exec(location.search)||[,'feed'])[1];

  function gotoPage(page) {
    var start = (page - 1) * ENTRIES_PER_PAGE, end = page * ENTRIES_PER_PAGE;

    var d = falcon.entities.slice(start, end);
    dust.render('entity-list', {"entity": d}, function(err, out) {
      var total = Math.ceil(falcon.entities.length / ENTRIES_PER_PAGE);
      $('#entity-paginator').data('activePage', page);
      generatePaginator(page, total);

      $('#entity-list-container').html(out);
    });
  }

  function generatePaginator(current, total) {
    var root = $('<ul class="pagination"></ul>'),
    prev = $('<li><a href="#">&laquo;</a></li>'),
    next = $('<li><a href="#">&raquo;</a></li>');

    root.append(prev);
    for (var i = 1; i <= total; ++i) {
      var l = $('<li><a href="#">' + i + '</a></li>')
        .attr('data-page', i);

      if (i == current)
        l.addClass('active');

      root.append(l);
    }

    root.append(next);

    if (current == 1) {
      prev.addClass('disabled');
    } else {
      prev.attr('data-page', current - 1);
    }

    if (current == total) {
      next.addClass('disabled');
    } else {
      next.attr('data-page', current + 1);
    }

    root.children()
      .click(function() {
        var n = $(this).attr('data-page');
        if (n !== undefined)
          gotoPage(n);

        return false;
      });
    $('#entity-paginator').html(root);
  }

  /**
   * Gets the latest entities from '/entities/list/process',
   * 'entities/list/feed', '/entities/list/cluster'
   **/
  function refreshEntities(type) {
    falcon.getJson('api/entities/list/' + type + '?fields=status', function (data) {
      if (data === null || data.entity == null)
        return;

      if (!($.isArray(data.entity)))
        data.entity = new Array(data.entity);

      $('#entity-paginator').data('activePage', 1);
      falcon.entities = data.entity;
      gotoPage(1);
    }).fail(falcon.ajaxFailureHandler);
  }

  function initialize() {
    $('.btn-entity-list').click(function() {
      $(this).parent().siblings().removeClass('active');
      $(this).parent().addClass('active');
      refreshEntities($(this).attr('data-entity-type'));
    });
    $('.btn-entity-list[data-entity-type="' + entityType + '"]').click();
  }

  initialize();
})(falcon, dust);
