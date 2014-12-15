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

  describe('X2jsService', function () {
    var x2jsService;

    beforeEach(module('app.services'));

    beforeEach(inject(function(X2jsService) {
      x2jsService = X2jsService;
    }));

    describe('prettifyXml', function() {
      it('Should prettify the xml', function() {
        var uglyXml = '<markup><child></child></markup>';

        expect(x2jsService.prettifyXml(uglyXml)).toNotBe(null);
      });
    });

    describe('prettifyXml', function() {
      it('Should prettify the xml', function() {
        var uglyXml = '<markup><child></child></markup>';

        expect(x2jsService.prettifyXml(uglyXml)).toNotBe(null);
      });
    });

    describe('Arrays configuration', function() {
      it('Should convert feed.properties.property as an array when only one element', function() {

        var xml =
          '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' +
          '<feed name="feedOne" description="feedOneDescription" xmlns="uri:falcon:feed:0.1">' +
            '<properties>' +
              '<property name="jobPriority" value="VERY_LOW"/>' +
            '</properties>' +
          '</feed>';

        var wrapper = x2jsService.xml_str2json(xml);

        expect(wrapper.feed.properties.property).toEqual(
          [{_name: 'jobPriority', _value: 'VERY_LOW'}]
        );

      });



      it('Should convert feed.locations.location as an array when only one element', function() {

        var xml =
          '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' +
          '<feed name="feedOne" description="feedOneDescription" xmlns="uri:falcon:feed:0.1">' +
            '<locations>' +
              '<location type="data" path="/path"/>' +
            '</locations>' +
          '</feed>';

        var wrapper = x2jsService.xml_str2json(xml);

        expect(wrapper.feed.locations.location).toEqual(
          [{_type: 'data', _path: '/path'}]
        );

      });

      it('Should convert feed.clusters.cluster as an array when only one element', function() {

        var xml =
          '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' +
          '<feed name="feedOne" description="feedOneDescription" xmlns="uri:falcon:feed:0.1">' +
            '<clusters>' +
              '<cluster name="ClusterName" type="source"/>' +
            '</clusters>' +
          '</feed>';

        var wrapper = x2jsService.xml_str2json(xml);

        expect(wrapper.feed.clusters.cluster).toEqual(
          [{_name: 'ClusterName', _type: 'source'}]
        );

      });

      it('Should convert feed.clusters.cluster.locations.location as an array when only one element', function() {

        var xml =
          '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' +
          '<feed name="feedOne" description="feedOneDescription" xmlns="uri:falcon:feed:0.1">' +
            '<clusters>' +
              '<cluster name="ClusterName" type="source">' +
                '<locations>' +
                  '<location type="data" path="/path" />' +
                '</locations>' +
              '</cluster>' +
            '</clusters>' +
          '</feed>';

        var wrapper = x2jsService.xml_str2json(xml);

        expect(wrapper.feed.clusters.cluster[0].locations.location).toEqual(
          [{_type: 'data', _path: '/path'}]
        );

      });

    });

  });
})();