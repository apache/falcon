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

  describe('EntitySerializer', function () {
    var serializer;
    beforeEach(module('app.services.entity.serializer', 'dateHelper'));


    beforeEach(inject(function(EntitySerializer, DateHelper) {
      serializer = EntitySerializer;
    }));

    describe('deserialize feed', function() {

      it('Should copy the general information', function() {

        var feedModel = {
          feed: {
            _xmlns: "uri:falcon:feed:0.1",
            _name: 'FeedName',
            _description: 'Feed Description'
          }
        };

        var feed = serializer.preDeserialize(feedModel, 'feed');

        expect(feed.name).toBe(feedModel.feed._name);
        expect(feed.description).toBe(feedModel.feed._description);
        expect(feed.xmlns).toBe(undefined);
      });

      it('Should copy tags', function() {

        var feedModel = {
          feed: {
            tags: 'owner=USMarketing,classification=Secure'
          }
        };

        var feed = serializer.preDeserialize(feedModel, 'feed');

        expect(feed.tags[0].key).toBe('owner');
        expect(feed.tags[0].value).toBe('USMarketing');
        expect(feed.tags[1].key).toBe('classification');
        expect(feed.tags[1].value).toBe('Secure');
      });

      it('Should copy groups', function() {

        var feedModel = {
          feed: {
            groups: 'churnAnalysisDataPipeline,Group2,Group3'
          }
        };

        var feed = serializer.preDeserialize(feedModel, 'feed');

        expect(feed.groups).toBe(feedModel.feed.groups);
      });

      it('Should copy ACL', function() {
        var feedModel = {
          feed: {
            ACL: {_owner: 'ambari-qa', _group: 'users', _permission: '0755' }
          }
        };

        var feed = serializer.preDeserialize(feedModel, 'feed');

        expect(feed.ACL.owner).toBe(feedModel.feed.ACL._owner);
        expect(feed.ACL.group).toBe(feedModel.feed.ACL._group);
        expect(feed.ACL.permission).toBe(feedModel.feed.ACL._permission);
      });

      it('Should copy Schema', function() {
        var feedModel = {
          feed: {
            schema: {_location: '/location', _provider: 'provider'}
          }
        };

        var feed = serializer.preDeserialize(feedModel, 'feed');

        expect(feed.schema.location).toBe(feedModel.feed.schema._location);
        expect(feed.schema.provider).toBe(feedModel.feed.schema._provider);
      });

      it('Should copy frequency', function() {
        var feedModel = {
          feed: {
            frequency: 'hours(20)'
          }
        };

        var feed = serializer.preDeserialize(feedModel, 'feed');

        expect(feed.frequency.unit).toBe('hours');
        expect(feed.frequency.quantity).toBe('20');
      });

      it('Should copy late arrival', function() {
        var feedModel = {
          feed: {
            "late-arrival": {"_cut-off": 'days(10)'}
          }
        };

        var feed = serializer.preDeserialize(feedModel, 'feed');

        expect(feed.lateArrival.active).toBe(true);
        expect(feed.lateArrival.cutOff.unit).toBe('days');
        expect(feed.lateArrival.cutOff.quantity).toBe('10');
      });

      it('Should not copy late arrival when is not present', function() {
        var feedModel = {
          feed: {}
        };

        var feed = serializer.preDeserialize(feedModel, 'feed');

        expect(feed.lateArrival.active).toBe(false);
        expect(feed.lateArrival.cutOff.unit).toBe('hours');
        expect(feed.lateArrival.cutOff.quantity).toBe(null);
      });

      it('Should copy availabilityFlag', function() {
        var feedModel = {
          feed: {
            availabilityFlag: 'Available'
          }
        };

        var feed = serializer.preDeserialize(feedModel, 'feed');

        expect(feed.availabilityFlag).toBe(feedModel.feed.availabilityFlag);
      });

      it('Should not copy availabilityFlag if not present in the xml', function() {
        var feedModel = {
          feed: {}
        };

        var feed = serializer.preDeserialize(feedModel, 'feed');

        expect(feed.availabilityFlag).toBe(null);
      });

      it('Should copy custom properties', function() {
        var feedModel = {
          feed: {
            properties: {property: [
              {_name: 'Prop1', _value: 'Value1'},
              {_name: 'Prop2', _value: 'Value2'}
            ]}
          }
        };

        var feed = serializer.preDeserialize(feedModel, 'feed');

        expect(feed.customProperties.length).toBe(3);
        expect(feed.customProperties[1].key).toBe('Prop1');
        expect(feed.customProperties[1].value).toBe('Value1');
        expect(feed.customProperties[2].key).toBe('Prop2');
        expect(feed.customProperties[2].value).toBe('Value2');
      });

      it('Should not copy falcon properties into the custom properties', function() {
        var feedModel = {
          feed: {
            properties: {property: [
              {_name: 'queueName', _value: 'QueueName'},
              {_name: 'Prop1', _value: 'Value1'}
            ]}
          }
        };

        var feed = serializer.preDeserialize(feedModel, 'feed');

        expect(feed.customProperties.length).toBe(2);
        expect(feed.customProperties[0].key).toBe(null);
        expect(feed.customProperties[0].value).toBe(null);
        expect(feed.customProperties[1].key).toBe('Prop1');
        expect(feed.customProperties[1].value).toBe('Value1');
      });

      it('Should copy queueName properties into properties', function() {
        var feedModel = {
          feed: {
            properties: {property: [
              {_name: 'queueName', _value: 'QueueName'},
              {_name: 'Prop1', _value: 'Value1'}
            ]}
          }
        };

        var feed = serializer.preDeserialize(feedModel, 'feed');

        expect(feed.properties.length).toBe(6);
        expect(feed.properties[0].key).toBe('queueName');
        expect(feed.properties[0].value).toBe('QueueName');
      });

      it('Should leave the default properties if no properties appear on the xml and copy the new ones', function() {
        var feedModel = {
          feed: {
            properties: {
              property: [
                {_name: 'jobPriority', _value: 'MEDIUM'}
              ]
            }
          }
        };


        var feed = serializer.preDeserialize(feedModel, 'feed');

        expect(feed.properties.length).toBe(6);
        expect(feed.properties[0].key).toBe('queueName');
        expect(feed.properties[0].value).toBe('');
        expect(feed.properties[1].key).toBe('jobPriority');
        expect(feed.properties[1].value).toBe('MEDIUM');
      });

      it('Should copy timeout as a Frequency Object', function() {
        var feedModel = {
          feed: {
            properties: {property: [
              {_name: 'queueName', _value: 'QueueName'},
              {_name: 'timeout', _value: 'days(4)'}
            ]}
          }
        };

        var feed = serializer.preDeserialize(feedModel, 'feed');

        expect(feed.properties.length).toBe(6);
        expect(feed.properties[2].key).toBe('timeout');
        expect(feed.properties[2].value.quantity).toBe('4');
        expect(feed.properties[2].value.unit).toBe('days');
      });

      it('Should copy file system locations', function() {
        var feedModel = {
          feed: {
            locations: {location: [
              {_type: 'data', _path: '/none1'},
              {_type: 'stats', _path: '/none2'}
            ]}
          }
        };

        var feed = serializer.preDeserialize(feedModel, 'feed');
        var locations = feed.storage.fileSystem.locations;

        expect(feed.storage.fileSystem.active).toBe(true);
        expect(locations.length).toBe(2);
        expect(locations[0].type).toBe('data');
        expect(locations[0].path).toBe('/none1');
        expect(locations[1].type).toBe('stats');
        expect(locations[1].path).toBe('/none2');
      });

      it('Should not copy file system locations if they are not defined and keep the defaults', function() {
        var feedModel = {
          feed: {
          }
        };

        var feed = serializer.preDeserialize(feedModel, 'feed');
        var locations = feed.storage.fileSystem.locations;

        expect(feed.storage.fileSystem.active).toBe(false);
        expect(locations.length).toBe(3);
        expect(locations[0].type).toBe('data');
        expect(locations[0].path).toBe('/');
        expect(locations[1].type).toBe('stats');
        expect(locations[1].path).toBe('/');
        expect(locations[2].type).toBe('meta');
        expect(locations[2].path).toBe('/');
      });

      it('Should set file system active flag as false if there are no locations are', function() {
        var feedModel = {
          feed: {}
        };

        var feed = serializer.preDeserialize(feedModel, 'feed');

        expect(feed.storage.fileSystem.active).toBe(false);
      });

      it('Should copy catalog uri', function() {
        var feedModel = {
          feed: {
            "table": {
              _uri : 'table:uri'
            }
          }
        };

        var feed = serializer.preDeserialize(feedModel, 'feed');

        expect(feed.storage.catalog.active).toBe(true);
        expect(feed.storage.catalog.catalogTable.uri).toBe('table:uri');
      });

      it('Should not copy catalog uri if not present', function() {
        var feedModel = {
          feed: {}
        };

        var feed = serializer.preDeserialize(feedModel, 'feed');

        expect(feed.storage.catalog.active).toBe(false);
        expect(feed.storage.catalog.catalogTable.uri).toBe(null);
      });

      it('Should copy cluster name and type', function() {
        var feedModel = {
          feed: {
            clusters: {
              cluster: [{
                _name: 'ClusterOne',
                _type: 'target'
              }]
            }
          }
        };

        var feed = serializer.preDeserialize(feedModel, 'feed');

        expect(feed.clusters.length).toBe(1);
        expect(feed.clusters[0].name).toBe('ClusterOne');
        expect(feed.clusters[0].type).toBe('target');
      });

      it('Should copy clusters and select the first source cluster', function() {
        var feedModel = {
          feed: {
            clusters: {
              cluster: [
                {_name: 'ClusterOne', _type: 'target'},
                {_name: 'ClusterTwo', _type: 'source'},
                {_name: 'ClusterThree', _type: 'target'},
                {_name: 'ClusterFour', _type: 'source'}
              ]
            }
          }
        };

        var feed = serializer.preDeserialize(feedModel, 'feed');

        expect(feed.clusters[0].selected).toBe(false);
        expect(feed.clusters[1].selected).toBe(true);
        expect(feed.clusters[2].selected).toBe(false);
        expect(feed.clusters[3].selected).toBe(false);

      });

      xit('Should copy validity', function() {
        var feedModel = {
          feed: {
            clusters: {cluster: [{_name: 'ClusterOne', _type: 'target',
              validity: {
                _start: '2014-02-28T01:20Z',
                _end: '2016-03-31T04:30Z'
              }
            }]}
          }
        };

        var feed = serializer.preDeserialize(feedModel, 'feed');

        expect(feed.clusters[0].validity.start.date).toEqual(new Date(2014, 2, 28,0,0));
        expect(feed.clusters[0].validity.start.time).toEqual(newUtcTime(1, 20));
        expect(feed.clusters[0].validity.end.date).toEqual(newUtcDate(2016, 3, 31));
        expect(feed.clusters[0].validity.end.time).toEqual(newUtcTime(4, 30));

      });

      it('Should copy retention', function() {
        var feedModel = {
          feed: {
            clusters: {cluster: [{_name: 'ClusterOne', _type: 'target',
              retention: {
                _limit: 'weeks(4)',
                _action: 'delete'
              }
            }]}
          }
        };

        var feed = serializer.preDeserialize(feedModel, 'feed');

        expect(feed.clusters[0].retention.quantity).toBe('4');
        expect(feed.clusters[0].retention.unit).toBe('weeks');
        expect(feed.clusters[0].retention.action).toBe('delete');
      });

      it('Should copy clusters locations', function() {
        var feedModel = {
          feed: {
            clusters: {cluster: [{_name: 'ClusterOne', _type: 'target',
              locations: {
                location: [
                  {_type: 'stats', _path: '/path1'},
                  {_type: 'data', _path: '/path2'},
                  {_type: 'tmp', _path: '/path3'}
                ]}
            }]}
          }
        };

        var feed = serializer.preDeserialize(feedModel, 'feed');
        var locations = feed.clusters[0].storage.fileSystem.locations;

        expect(feed.clusters[0].storage.fileSystem.active).toBe(true);
        expect(locations.length).toBe(3);
        expect(locations[0].type).toBe('stats');
        expect(locations[0].path).toBe('/path1');
        expect(locations[1].type).toBe('data');
        expect(locations[1].path).toBe('/path2');
        expect(locations[2].type).toBe('tmp');
        expect(locations[2].path).toBe('/path3');
      });

      it('filesystem should be inactive if there are no locations', function() {
        var feedModel = {
          feed: {
            clusters: {cluster: [{_name: 'ClusterOne', _type: 'target'}]}
          }
        };

        var feed = serializer.preDeserialize(feedModel, 'feed');
        var locations = feed.clusters[0].storage.fileSystem.locations;

        expect(feed.clusters[0].storage.fileSystem.active).toBe(false);
        expect(locations.length).toBe(3);
      });

      it('Should copy catalog uri', function() {
        var feedModel = {
          feed: {
            clusters: {cluster: [{_name: 'ClusterOne', _type: 'target',
              "table": {
                _uri : 'table:uri'
              }
            }]}
          }
        };


        var feed = serializer.preDeserialize(feedModel, 'feed');
        var catalogStorage = feed.clusters[0].storage.catalog;

        expect(catalogStorage.active).toBe(true);
        expect(catalogStorage.catalogTable.uri).toBe('table:uri');
      });

      it('Should set catalog storage as inactive when not present in the xml', function() {
        var feedModel = {
          feed: {
            clusters: {cluster: [{_name: 'ClusterOne', _type: 'target'}]}
          }
        };


        var feed = serializer.preDeserialize(feedModel, 'feed');
        var catalogStorage = feed.clusters[0].storage.catalog;

        expect(catalogStorage.active).toBe(false);
        expect(catalogStorage.catalogTable.uri).toBe(null);
      });

      it('Should copy timezone', function() {
        var feedModel = {
          feed: {
            timezone: 'GMT+03:50'
          }
        };

        var feed = serializer.preDeserialize(feedModel, 'feed');

        expect(feed.timezone).toBe('GMT+03:50');
      });

    });

    describe('serialize feed into xml', function() {

      it('Should transform the basic properties', function () {
        var feed = {
          name: 'FeedName',
          description: 'Feed Description',
          groups: 'a,b,c'
        };

        var xml = serializer.serialize(feed, 'feed');

        expect(xml).toBe(
            "<feed xmlns='uri:falcon:feed:0.1' name='FeedName' description='Feed Description'>" +
            "<groups>a,b,c</groups>" +
            "</feed>"
        );

      });

      it('Should transform tags properly', function () {
        var feed = {name: 'FeedName',
          tags: [{key: 'key1', value: 'value1'}, {key: 'key2', value: 'value2'}, {key: null, value: 'value3'}]
        };

        var xml = serializer.serialize(feed, 'feed');

        expect(xml).toBe(
            "<feed xmlns='uri:falcon:feed:0.1' name='FeedName'>" +
            "<tags>key1=value1,key2=value2</tags>" +
            "</feed>"
        );

      });

      //it('Should transform ACL properly', function () {
      //  var feed = {name: 'FeedName',
      //    ACL: {owner: 'ambari-qa', group: 'users', permission: '0755'}
      //  };
      //
      //  var xml = serializer.serialize(feed, 'feed');
      //
      //  expect(xml).toBe(
      //      "<feed xmlns='uri:falcon:feed:0.1' name='FeedName'>" +
      //      "<ACL owner='ambari-qa' group='users' permission='0755'/>" +
      //      "</feed>"
      //  );
      //
      //});

      it('Should add an ACL element even though the properties are empty', function () {
        var feed = {name: 'FeedName',
          ACL: {owner: null, group: null, permission: null}
        };

        var xml = serializer.serialize(feed, 'feed');

        expect(xml).toBe(
            "<feed xmlns='uri:falcon:feed:0.1' name='FeedName'>" +
            "<ACL/>" +
            "</feed>"
        );

      });

      it('Should transform schema properly', function () {
        var feed = {name: 'FeedName',
          schema: {location: '/location', provider: 'none'}
        };

        var xml = serializer.serialize(feed, 'feed');

        expect(xml).toBe(
            "<feed xmlns='uri:falcon:feed:0.1' name='FeedName'>" +
            "<schema location='/location' provider='none'/>" +
            "</feed>"
        );

      });

      it('Should add the schema element even though the properties are empty', function () {
        var feed = {name: 'FeedName',
          schema: {location: null, provider: null}
        };

        var xml = serializer.serialize(feed, 'feed');

        expect(xml).toBe(
            "<feed xmlns='uri:falcon:feed:0.1' name='FeedName'>" +
            "<schema/>" +
            "</feed>"
        );

      });

      it('Should transform frequency properly', function () {
        var feed = {name: 'FeedName',
          frequency: {quantity: 4, unit: 'weeks'}
        };

        var xml = serializer.serialize(feed, 'feed');

        expect(xml).toBe(
            "<feed xmlns='uri:falcon:feed:0.1' name='FeedName'>" +
            "<frequency>weeks(4)</frequency>" +
            "</feed>"
        );

      });

      it('Should transform late arrival properly when defined', function () {
        var feed = {name: 'FeedName',
          lateArrival: {active: true, cutOff: {quantity: 22, unit: 'hours'}}
        };

        var xml = serializer.serialize(feed, 'feed');

        expect(xml).toBe(
            "<feed xmlns='uri:falcon:feed:0.1' name='FeedName'>" +
            "<late-arrival cut-off='hours(22)'/>" +
            "</feed>"
        );

      });

      it('Should not transform late arrival properly when quantity is not defined', function () {
        var feed = {name: 'FeedName',
          lateArrival: {active: false, cutOff: {quantity: null, unit: 'hours'}}
        };

        var xml = serializer.serialize(feed, 'feed');

        expect(xml).toBe(
          "<feed xmlns='uri:falcon:feed:0.1' name='FeedName'/>"
        );

      });

      it('Should transform availability flag', function () {
        var feed = {name: 'FeedName',
          availabilityFlag: 'Available'
        };

        var xml = serializer.serialize(feed, 'feed');

        expect(xml).toBe(
            "<feed xmlns='uri:falcon:feed:0.1' name='FeedName'>" +
            "<availabilityFlag>Available</availabilityFlag>" +
            "</feed>"
        );

      });

      it('Should transform timezone', function () {
        var feed = {name: 'FeedName',
          timezone: 'GMT+1:00'
        };

        var xml = serializer.serialize(feed, 'feed');

        expect(xml).toBe(
            "<feed xmlns='uri:falcon:feed:0.1' name='FeedName'>" +
            "<timezone>GMT+1:00</timezone>" +
            "</feed>"
        );

      });

      //it('Should transform queueName, jobPriority and timeout and custom properties', function () {
      //  var feed = {name: 'FeedName',
      //    properties: [
      //      {key: 'queueName', value: 'Queue'},
      //      {key: 'jobPriority', value: 'HIGH'},
      //      {key: 'timeout', value: {quantity: 7, unit: 'weeks'}}
      //    ],
      //    customProperties: [
      //      {key: 'custom1', value: 'value1'},
      //      {key: 'custom2', value: 'value2'}
      //    ]
      //  };
      //
      //  var xml = serializer.serialize(feed, 'feed');
      //
      //  expect(xml).toBe(
      //      "<feed xmlns='uri:falcon:feed:0.1' name='FeedName'>" +
      //      "<properties>" +
      //      "<property name='queueName' value='Queue'></property>" +
      //      "<property name='jobPriority' value='HIGH'></property>" +
      //      "<property name='timeout' value='weeks(7)'></property>" +
      //      "<property name='custom1' value='value1'></property>" +
      //      "<property name='custom2' value='value2'></property>" +
      //      "</properties>" +
      //      "</feed>"
      //  );
      //
      //});

      it('Should transform not add queueName nor timeout if they were not defined', function () {
        var feed = {name: 'FeedName',
          properties: [
            {key: 'queueName', value: null},
            {key: 'jobPriority', value: 'HIGH'},
            {key: 'timeout', value: {quantity: null, unit: 'weeks'}}
          ]
        };

        var xml = serializer.serialize(feed, 'feed');

        expect(xml).toBe(
            "<feed xmlns='uri:falcon:feed:0.1' name='FeedName'>" +
            "<properties>" +
            "<property name='jobPriority' value='HIGH'></property>" +
            "</properties>" +
            "</feed>"
        );

      });

      it('Should transform locations properly if file system storage is active', function () {
        var feed = {name: 'FeedName',
          storage: {
            fileSystem: {
              active: true,
              locations: [
                {type: 'data', path: '/none1'},
                {type: 'stats', path: '/none2'},
                {type: 'meta', path: '/none3'}
              ]
            }
          }
        };

        var xml = serializer.serialize(feed, 'feed');

        expect(xml).toBe(
            "<feed xmlns='uri:falcon:feed:0.1' name='FeedName'>" +
            "<locations>" +
            "<location type='data' path='/none1'></location>" +
            "<location type='stats' path='/none2'></location>" +
            "<location type='meta' path='/none3'></location>" +
            "</locations>" +
            "</feed>"
        );

      });

      it('Should not transform locations properly if file system storage is not active', function () {
        var feed = {name: 'FeedName',
          storage: {
            fileSystem: {
              active: false,
              locations: [
                {type: 'data', path: '/none1'},
                {type: 'stats', path: '/none2'},
                {type: 'meta', path: '/none3'}
              ]
            }
          }
        };

        var xml = serializer.serialize(feed, 'feed');

        expect(xml).toBe(
          "<feed xmlns='uri:falcon:feed:0.1' name='FeedName'/>"
        );

      });

      it('Should transform catalog properly if catalog storage is active', function () {
        var feed = {name: 'FeedName',
          storage: {
            catalog: {
              active: true,
              catalogTable: {uri: '/none'}
            }
          }
        };

        var xml = serializer.serialize(feed, 'feed');

        expect(xml).toBe(
            "<feed xmlns='uri:falcon:feed:0.1' name='FeedName'>" +
            "<table uri='/none'/>" +
            "</feed>"
        );

      });

      it('Should not transform catalog if catalog storage is not active', function () {
        var feed = {name: 'FeedName',
          storage: {
            catalog: {
              active: false,
              catalogTable: {uri: '/none'}
            }
          }
        };

        var xml = serializer.serialize(feed, 'feed');

        expect(xml).toBe(
          "<feed xmlns='uri:falcon:feed:0.1' name='FeedName'/>"
        );

      });

      xit('Should transform clusters', function () {
        var feed = {name: 'FeedName',
          storage: {
            fileSystem: {active: true, locations: [
              {type: 'data', path: '/masterpath'}
            ]},
            catalog: {active: true, catalogTable: {uri: '/masteruri'}}
          },
          clusters: [
            {
              name: 'primaryCluster',
              type: 'source',
              validity: {start: {date: newUtcDate(2014, 2, 28), time: newUtcTime(0,0)}, end: {date: newUtcDate(2016, 4, 1), time: newUtcTime(0,0)}},
              retention: {quantity: 2, unit: 'hours', action: 'delete'},
              storage: {
                fileSystem: {
                  active: true,
                  locations: [
                    {type: 'data', path: '/none1'},
                    {type: 'stats', path: '/none2'},
                    {type: 'meta', path: '/none3'}
                  ]
                },
                catalog: {
                  active: false,
                  catalogTable: {uri: '/primaryuri'}
                }
              }
            },
            {
              name: 'secondaryCluster',
              type: 'target',
              validity: {start: {date: newUtcDate(2015, 2, 28), time: newUtcTime(0,0)}, end: {date: newUtcDate(2017, 4, 1), time: newUtcTime(0,0)}},
              retention: {quantity: 5, unit: 'weeks', action: 'archive'},
              storage: {
                fileSystem: {
                  active: true,
                  locations: [
                    {type: 'data', path: '/none4'},
                    {type: 'stats', path: '/none5'},
                    {type: 'meta', path: '/none6'}
                  ]
                },
                catalog: {
                  active: true,
                  catalogTable: {uri: '/secondaryuri'}
                }
              }
            }
          ]
        };

        var xml = serializer.serialize(feed, 'feed');

        expect(xml).toBe(
            "<feed xmlns='uri:falcon:feed:0.1' name='FeedName'>" +
            "<clusters>" +
            "<cluster name='primaryCluster' type='source'>" +
            "<validity start='2014-02-28T00:00Z' end='2016-04-01T00:00Z'/>" +
            "<retention limit='hours(2)' action='delete'/>" +
            "<locations>" +
            "<location type='data' path='/none1'></location>" +
            "<location type='stats' path='/none2'></location>" +
            "<location type='meta' path='/none3'></location>" +
            "</locations>" +
            "<table uri='/primaryuri'/>" +
            "</cluster>" +
            "<cluster name='secondaryCluster' type='target'>" +
            "<validity start='2015-02-28T00:00Z' end='2017-04-01T00:00Z'/>" +
            "<retention limit='weeks(5)' action='archive'/>" +
            "<locations>" +
            "<location type='data' path='/none4'></location>" +
            "<location type='stats' path='/none5'></location>" +
            "<location type='meta' path='/none6'></location>" +
            "</locations>" +
            "<table uri='/secondaryuri'/>" +
            "</cluster>" +
            "</clusters>" +
            "<locations>" +
            "<location type='data' path='/masterpath'></location>" +
            "</locations>" +
            "<table uri='/masteruri'/>" +
            "</feed>"
        );

      });

    });

    describe('deserialize process', function() {

      it('Should copy the general information', function() {

        var processModel = {
          process: {
            _xmlns: "uri:falcon:process:0.1",
            _name: 'ProcessName'
          }
        };

        var process = serializer.preDeserialize(processModel, 'process');

        expect(process.name).toBe(processModel.process._name);
        expect(process.xmlns).toBe(undefined);
      });

      it('Should copy tags', function() {

        var processModel = {
          process: {
            tags: 'owner=USMarketing,classification=Secure'
          }
        };

        var process = serializer.preDeserialize(processModel, 'process');

        expect(process.tags[0].key).toBe('owner');
        expect(process.tags[0].value).toBe('USMarketing');
        expect(process.tags[1].key).toBe('classification');
        expect(process.tags[1].value).toBe('Secure');
      });

      it('Should copy workflow', function() {

        var processModel = {
          process: {_xmlns: "uri:falcon:process:0.1", _name: 'ProcessName',
            workflow: {
              _name: 'emailCleanseWorkflow',
              _version: '5.0',
              _engine: 'pig',
              _path: '/user/ambari-qa/falcon/demo/apps/pig/id.pig'
            }
          }
        };

        var process = serializer.preDeserialize(processModel, 'process');

        expect(process.workflow.name).toBe(processModel.process.workflow._name);
        expect(process.workflow.version).toBe(processModel.process.workflow._version);
        expect(process.workflow.engine).toBe(processModel.process.workflow._engine);
        expect(process.workflow.path).toBe(processModel.process.workflow._path);

      });

      it('Should copy timezone', function() {

        var processModel = {
          process: {_xmlns: "uri:falcon:process:0.1", _name: 'ProcessName',
            timezone: 'GMT+1:00'
          }
        };

        var process = serializer.preDeserialize(processModel, 'process');

        expect(process.timezone).toBe(processModel.process.timezone);
      });

      it('Should copy frequency', function() {
        var processModel = {process: {
            frequency: 'hours(20)'
        }};

        var process = serializer.preDeserialize(processModel, 'process');

        expect(process.frequency.unit).toBe('hours');
        expect(process.frequency.quantity).toBe('20');

      });

      it('Should copy parallel', function() {
        var processModel = { process: {
          parallel: 3
        }};

        var process = serializer.preDeserialize(processModel, 'process');

        expect(process.parallel).toBe(processModel.process.parallel);

      });

      it('Should copy order', function() {
        var processModel = { process: {
          order: 'LIFO'
        }};

        var process = serializer.preDeserialize(processModel, 'process');

        expect(process.order).toBe(processModel.process.order);

      });

      it('Should copy retry', function() {
        var processModel = { process: {
          retry: {
            _policy: 'periodic', _attempts: '3', _delay: 'minutes(15)'
          }
        }};

        var process = serializer.preDeserialize(processModel, 'process');

        expect(process.retry.policy).toBe(processModel.process.retry._policy);
        expect(process.retry.attempts).toBe(processModel.process.retry._attempts);
        expect(process.retry.delay.quantity).toBe('15');
        expect(process.retry.delay.unit).toBe('minutes');

      });

      xit('Should copy clusters', function() {
        var processModel = {
          process: {
            clusters: {cluster: [{_name: 'ClusterOne',
              validity: {
                _start: '2014-02-28T01:20Z',
                _end: '2016-03-31T04:30Z'
              }
            }]}
          }
        };

        var process = serializer.preDeserialize(processModel, 'process');

        expect(process.clusters[0].validity.start.date).toEqual(newUtcDate(2014, 2, 28));
        expect(process.clusters[0].validity.start.time).toEqual(newUtcTime(1, 20));
        expect(process.clusters[0].validity.end.date).toEqual(newUtcDate(2016, 3, 31));
        expect(process.clusters[0].validity.end.time).toEqual(newUtcTime(4, 30));

      });

      it('Should copy inputs', function() {
        var processModel = {
          process: {
            inputs: {input: [
              {_name: 'input', _feed: 'rawEmailFeed', _start: 'now(0,0)', _end: 'now(0,0)' }
            ]}
          }
        };

        var process = serializer.preDeserialize(processModel, 'process');

        expect(process.inputs[0].name).toEqual('input');
        expect(process.inputs[0].feed).toEqual('rawEmailFeed');
        expect(process.inputs[0].start).toEqual('now(0,0)');
        expect(process.inputs[0].end).toEqual('now(0,0)');
      });

      it('Should copy outputs', function() {
        var processModel = {
          process: {
            outputs: {output: [
              {_name: 'output', _feed: 'cleansedEmailFeed', _instance: 'now(0,0)' }
            ]}
          }
        };

        var process = serializer.preDeserialize(processModel, 'process');

        expect(process.outputs[0].name).toEqual('output');
        expect(process.outputs[0].feed).toEqual('cleansedEmailFeed');
        expect(process.outputs[0].outputInstance).toEqual('now(0,0)');
      });

    });

    describe('serialize process into xml', function() {

      it('Should transform the basic properties', function () {
        var process = {
          name: 'ProcessName'
        };

        var xml = serializer.serialize(process, 'process');

        expect(xml).toBe(
            "<process xmlns='uri:falcon:process:0.1' name='ProcessName'/>"
        );
      });

      it('Should transform tags properly', function () {
        var process = {name: 'ProcessName',
          tags: [{key: 'key1', value: 'value1'}, {key: 'key2', value: 'value2'}, {key: null, value: 'value3'}]
        };

        var xml = serializer.serialize(process, 'process');

        expect(xml).toBe(
            "<process xmlns='uri:falcon:process:0.1' name='ProcessName'>" +
              "<tags>key1=value1,key2=value2</tags>" +
            "</process>"
        );
      });

      it('Should transform workflow properly', function () {
        var process = {name: 'ProcessName',
          workflow: {
           name: 'emailCleanseWorkflow',
           engine: 'pig',
           version: '5.0',
           path: '/user/ambari-qa/falcon/demo/apps/pig/id.pig'
          }
        };

        var xml = serializer.serialize(process, 'process');

        expect(xml).toBe(
            "<process xmlns='uri:falcon:process:0.1' name='ProcessName'>" +
              "<workflow name='emailCleanseWorkflow' version='5.0' engine='pig' path='/user/ambari-qa/falcon/demo/apps/pig/id.pig'/>" +
            "</process>"
        );
      });

      it('Should transform timezone', function () {
        var process = {name: 'ProcessName',
          timezone: 'GMT+1:00'
        };

        var xml = serializer.serialize(process, 'process');

        expect(xml).toBe(
            "<process xmlns='uri:falcon:process:0.1' name='ProcessName'>" +
              "<timezone>GMT+1:00</timezone>" +
            "</process>"
        );

      });

      it('Should transform frequency properly', function () {
        var process = {name: 'ProcessName',
          frequency: {quantity: 4, unit: 'weeks'}
        };

        var xml = serializer.serialize(process, 'process');

        expect(xml).toBe(
            "<process xmlns='uri:falcon:process:0.1' name='ProcessName'>" +
              "<frequency>weeks(4)</frequency>" +
            "</process>"
        );

      });

      it('Should transform parallel properly', function () {
        var process = {name: 'ProcessName',
          parallel: 11
        };

        var xml = serializer.serialize(process, 'process');

        expect(xml).toBe(
            "<process xmlns='uri:falcon:process:0.1' name='ProcessName'>" +
              "<parallel>11</parallel>" +
            "</process>"
        );

      });

      it('Should transform order properly', function () {
        var process = {name: 'ProcessName',
          order: 'LIFO'
        };

        var xml = serializer.serialize(process, 'process');

        expect(xml).toBe(
            "<process xmlns='uri:falcon:process:0.1' name='ProcessName'>" +
              "<order>LIFO</order>" +
            "</process>"
        );

      });

      it('Should transform retry properly', function () {
        var process = {name: 'ProcessName',
          retry: {
            policy: 'periodic',
            delay: {quantity: 15, unit: 'minutes'},
            attempts: 3
          }
        };

        var xml = serializer.serialize(process, 'process');

        expect(xml).toBe(
            "<process xmlns='uri:falcon:process:0.1' name='ProcessName'>" +
              "<retry policy='periodic' delay='minutes(15)' attempts='3'/>" +
            "</process>"
        );
      });

      xit('Should transform clusters', function () {

        var process = {name: 'ProcessName',
          clusters: [
            {
              name: 'primaryCluster',
              validity: {start: {date: newUtcDate(2014, 2, 28), time: newUtcTime(0,0)}, end: {date: newUtcDate(2016, 4, 1), time: newUtcTime(0,0)}}
            },
            {
              name: 'secondaryCluster',
              validity: {start: {date: newUtcDate(2015, 2, 28), time: newUtcTime(0,0)}, end: {date: newUtcDate(2017, 4, 1), time: newUtcTime(0,0)}}
            }
          ]
        };

        var xml = serializer.serialize(process, 'process');

        expect(xml).toBe(
            "<process xmlns='uri:falcon:process:0.1' name='ProcessName'>" +
              "<clusters>" +
                "<cluster name='primaryCluster'>" +
                  "<validity start='2014-02-28T00:00Z' end='2016-04-01T00:00Z'/>" +
                "</cluster>" +
                "<cluster name='secondaryCluster'>" +
                  "<validity start='2015-02-28T00:00Z' end='2017-04-01T00:00Z'/>" +
                "</cluster>" +
              "</clusters>" +
            "</process>"
        );

      });

      it('Should transform inputs', function () {

        var process = {name: 'ProcessName',
          inputs: [
            { name: 'input', feed: 'rawEmailFeed', start: 'now(0,0)', end: 'now(0,0)' }
          ]
        };

        var xml = serializer.serialize(process, 'process');

        expect(xml).toBe(
            "<process xmlns='uri:falcon:process:0.1' name='ProcessName'>" +
              "<inputs>" +
                "<input name='input' feed='rawEmailFeed' start='now(0,0)' end='now(0,0)'>" +
                "</input>" +
              "</inputs>" +
            "</process>"
        );

      });

      it('Should transform outputs', function () {

        var process = {name: 'ProcessName',
          outputs: [
            { name: 'output', feed: 'cleansedEmailFeed', outputInstance: 'now(0,0)'}
          ]
        };

        var xml = serializer.serialize(process, 'process');

        expect(xml).toBe(
            "<process xmlns='uri:falcon:process:0.1' name='ProcessName'>" +
              "<outputs>" +
                "<output name='output' feed='cleansedEmailFeed' instance='now(0,0)'>" +
                "</output>" +
              "</outputs>" +
            "</process>"
        );

      });

    });

    function newUtcDate(year, month, day) {
      return new Date(Date.UTC(year, month, day))
    }

    function newUtcTime(hours, minutes) {
      return new Date(Date.UTC(1900, 1, 1, hours, minutes, 0));
    }

  });
})();