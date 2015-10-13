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
  var module = angular.module('app.services.entity.serializer',
    ['app.services.json.transformer',
      'app.services',
      'app.services.entity.factory',
      'app.services.entity.model']);

  module.factory('EntitySerializer',
    ['EntityFactory', 'JsonTransformerFactory', 'X2jsService', 'DateHelper', 'EntityModel',
    function(EntityFactory, JsonTransformerFactory, X2jsService, DateHelper, EntityModel) {

      var feedTz, processTz; //>> this is due the weird way feed and process forms are made// only way i found to pass tz
                             //>> was creating this variables and duplicating import timeanddateostring functions


      function timeAndDateToStringFeed(input) {
        return DateHelper.createISO(input.date, input.time, feedTz);
      }
      function importDateFeed (input) {
        return DateHelper.importDate(input, feedTz);
      }
      function timeAndDateToStringProcess(input) {
        //return DateHelper.createISO(input.date, input.time, processTz);
        return DateHelper.createISOString(input.date, input.time);
      }
      function importDateProcess (input) {
        return DateHelper.importDate(input, processTz);
      }


    return {
      preSerialize: function(feed, type) {
        if(type === 'feed') {
          if(feed.properties) {
            feed.allproperties = feed.properties.concat(feed.customProperties);
          }
          return preSerializeFeed(feed, JsonTransformerFactory);
        } else if(type === 'process') {
          return preSerializeProcess(feed, JsonTransformerFactory);
        }
      },

      serialize: function(feed, type) {
        return X2jsService.json2xml_str(this.preSerialize(feed, type));
      },

      preDeserialize: function(entityModel, type) {
        if(type === 'feed') {
          return preDeserializeFeed(entityModel, JsonTransformerFactory);
        } else if(type === 'process') {
          return preDeserializeProcess(entityModel, JsonTransformerFactory);
        }
      },

      deserialize: function(xml, type) {
        var entityModel = X2jsService.xml_str2json(xml);
        return this.preDeserialize(entityModel, type);
      }

    };

      function keyValuePairs(input) {
        return input.filter(emptyKey).map(entryToString).join(',');
      }

      function emptyKey (input) {
        return input.key;
      }

      function emptyValue (input) {
        return input && input.value;
      }

      function emptyFrequency (input) {
        return input.value.unit ? input.value.quantity : input.value;
      }

      function entryToString(input) {
        return input.key + '=' + input.value;
      }

      function frequencyToString(input) {
        return input.quantity ? input.unit + '(' + input.quantity + ')' : null;
      }

      function pad(n) {
        return String("00" + n).slice(-2);
      }



      function emptyElement() {return {};}

      function EntityModel(type) {
        this[type] = {_xmlns: 'uri:falcon:' + type + ':0.1'};
      }

      function preSerializeFeed (feed, transformerFactory) {
        var propertyTransform = transformerFactory
          .transform('key', '_name')
          .transform('value', '_value', function(value) {
            return value.quantity ? frequencyToString(value) : value;
          });

        var locationTransform = transformerFactory
          .transform('type', '_type')
          .transform('path', '_path');

        var clusterTransform = transformerFactory
          .transform('name', '_name')
          .transform('type', '_type')
          .transform('validity.start', 'validity._start', (function () { feedTz = feed.timezone; return timeAndDateToStringFeed; }()))
          .transform('validity.end', 'validity._end', timeAndDateToStringFeed)
          .transform('retention', 'retention._limit', frequencyToString)
          .transform('retention.action', 'retention._action')
          .transform('storage.fileSystem', 'locations.location', function(fileSystem) {
            return feed.storage.fileSystem.active ? transformfileSystem(fileSystem) : null;
          })
          .transform('storage.catalog', 'table', function(catalog) {
            return feed.storage.catalog.active ? transformCatalog(catalog) : null;
          });

        var transform = transformerFactory
          .transform('name', 'feed._name')
          .transform('description', 'feed._description')
          .transform('tags', 'feed.tags', keyValuePairs)
          .transform('groups', 'feed.groups')
          .transform('availabilityFlag', 'feed.availabilityFlag')
          .transform('frequency', 'feed.frequency', frequencyToString)
          .transform('timezone', 'feed.timezone')
          .transform('lateArrival.cutOff', 'feed.late-arrival._cut-off', frequencyToString)
          .transform('clusters', 'feed.clusters.cluster', function(clusters) {
            return clusters.map(function(cluster) {
              return clusterTransform.apply(cluster, {});
            });
          })
          .transform('storage.fileSystem', 'feed.locations.location', function(fileSystem) {
            return fileSystem.active ? transformfileSystem(fileSystem) : null;
          })
          .transform('storage.catalog', 'feed.table', function(catalog) {
            return catalog.active ? transformCatalog(catalog) : null;
          })
          .transform('ACL', 'feed.ACL', emptyElement)
          .transform('ACL.owner', 'feed.ACL._owner')
          .transform('ACL.group', 'feed.ACL._group')
          .transform('ACL.permission', 'feed.ACL._permission')
          .transform('schema', 'feed.schema', emptyElement)
          .transform('schema.location', 'feed.schema._location')
          .transform('schema.provider', 'feed.schema._provider')
          .transform('allproperties', 'feed.properties.property', function(properties) {
            return properties.filter(emptyValue).filter(emptyFrequency).map(function(property) {
              return propertyTransform.apply(property, {});
            });
          });

        function transformfileSystem (fileSystem) {
          return fileSystem.locations.map(function(location) {
            return locationTransform.apply(location, {});
          });
        }

        function transformCatalog(catalog) {
          return {_uri : catalog.catalogTable.uri};
        }

        return transform.apply(feed, new EntityModel('feed'));

      }

      function preSerializeProcess (process, transformerFactory) {

        var clusterTransform = transformerFactory
          .transform('name', '_name')
          .transform('validity.start', 'validity._start', (function () { processTz = process.timezone; return timeAndDateToStringProcess; }()))
          .transform('validity.end', 'validity._end', timeAndDateToStringProcess);

        var inputTransform = transformerFactory
          .transform('name', '_name')
          .transform('feed', '_feed')
          .transform('start', '_start')
          .transform('end', '_end');

        var outputTransform = transformerFactory
          .transform('name', '_name')
          .transform('feed', '_feed')
          .transform('outputInstance', '_instance');

        var transform = transformerFactory
          .transform('name', 'process._name')
          .transform('tags', 'process.tags', keyValuePairs)



          .transform('clusters', 'process.clusters.cluster', function(clusters) {
            return clusters.map(function(cluster) {
              return clusterTransform.apply(cluster, {});
            });
          })
          .transform('parallel', 'process.parallel')
          .transform('order', 'process.order')
          .transform('frequency', 'process.frequency', frequencyToString)
          .transform('timezone', 'process.timezone')
          .transform('inputs', 'process.inputs.input', function(inputs) {
            if(inputs.length === 0) {
              return null;
            }
            return inputs.map(function(input) {
              return inputTransform.apply(input, {});
            });
          })
          .transform('outputs', 'process.outputs.output', function(outputs) {
            if(outputs.length === 0) {
              return null;
            }
            return outputs.map(function(output) {
              return outputTransform.apply(output, {});
            });
          })
          .transform('workflow.name', 'process.workflow._name')
          .transform('workflow.version', 'process.workflow._version')
          .transform('workflow.engine', 'process.workflow._engine')
          .transform('workflow.path', 'process.workflow._path')
          .transform('retry.policy', 'process.retry._policy')
          .transform('retry.delay', 'process.retry._delay', frequencyToString)
          .transform('retry.attempts', 'process.retry._attempts')
          .transform('ACL', 'process.ACL', emptyElement)
          .transform('ACL.owner', 'process.ACL._owner')
          .transform('ACL.group', 'process.ACL._group')
          .transform('ACL.permission', 'process.ACL._permission');


        return transform.apply(process, new EntityModel('process'));

      }

      function preDeserializeFeed(feedModel, transformerFactory) {

        var feed = EntityFactory.newFeed();
        feed.storage.fileSystem.active = false;

        var clusterTransform = transformerFactory
            .transform('_name', 'name')
            .transform('_type', 'type')
            .transform('validity._start', 'validity.start.date', (function () { feedTz = feedModel.feed.timezone; return importDateFeed; }()))
            .transform('validity._start', 'validity.start.time', importDateFeed)
            .transform('validity._end', 'validity.end.date', importDateFeed)
            .transform('validity._end', 'validity.end.time', importDateFeed)
            .transform('retention._limit', 'retention', parseFrequency)
            .transform('retention._action', 'retention.action')
            .transform('locations', 'storage.fileSystem.active', parseBoolean)
            .transform('locations.location', 'storage.fileSystem.locations', parseLocations)
            .transform('table', 'storage.catalog.active', parseBoolean)
            .transform('table._uri', 'storage.catalog.catalogTable.uri')
          ;

        var transform = transformerFactory
            .transform('_name', 'name')
            .transform('_description', 'description')
            .transform('tags', 'tags', parseKeyValuePairs)
            .transform('groups','groups')
            .transform('ACL._owner','ACL.owner')
            .transform('ACL._group','ACL.group')
            .transform('ACL._permission','ACL.permission')
            .transform('schema._location','schema.location')
            .transform('schema._provider','schema.provider')
            .transform('frequency','frequency', parseFrequency)
            .transform('late-arrival','lateArrival.active', parseBoolean)
            .transform('late-arrival._cut-off','lateArrival.cutOff', parseFrequency)
            .transform('availabilityFlag', 'availabilityFlag')
            .transform('properties.property', 'customProperties', parseProperties(isCustomProperty, EntityFactory.newFeedCustomProperties()))
            .transform('properties.property', 'properties', parseProperties(isFalconProperty, EntityFactory.newFeedProperties()))
            .transform('locations', 'storage.fileSystem.active', parseBoolean)
            .transform('locations.location', 'storage.fileSystem.locations', parseLocations)
            .transform('table', 'storage.catalog.active', parseBoolean)
            .transform('table._uri', 'storage.catalog.catalogTable.uri')
            .transform('clusters.cluster', 'clusters', parseClusters(clusterTransform))
            .transform('timezone', 'timezone');

        return transform.apply(angular.copy(feedModel.feed), feed);
      }

      function preDeserializeProcess(processModel, transformerFactory) {

        var process = EntityFactory.newProcess();

        var clusterTransform = transformerFactory
            .transform('_name', 'name')
            .transform('validity._start', 'validity.start.date', (function () { processTz = processModel.process.timezone; return importDateProcess; }()))
            .transform('validity._start', 'validity.start.time', importDateProcess)
            .transform('validity._end', 'validity.end.date', importDateProcess)
            .transform('validity._end', 'validity.end.time', importDateProcess);

        var inputTransform = transformerFactory
          .transform('_name', 'name')
          .transform('_feed', 'feed')
          .transform('_start', 'start')
          .transform('_end', 'end');

        var outputTransform = transformerFactory
          .transform('_name', 'name')
          .transform('_feed', 'feed')
          .transform('_instance', 'outputInstance');

        var transform = transformerFactory
          .transform('_name', 'name')
          .transform('tags', 'tags', parseKeyValuePairs)
          .transform('workflow._name', 'workflow.name')
          .transform('workflow._version', 'workflow.version')
          .transform('workflow._engine', 'workflow.engine')
          .transform('workflow._path', 'workflow.path')
          .transform('timezone', 'timezone')
          .transform('frequency','frequency', parseFrequency)
          .transform('parallel','parallel')
          .transform('order','order')
          .transform('retry._policy','retry.policy')
          .transform('retry._attempts','retry.attempts')
          .transform('retry._delay','retry.delay', parseFrequency)
          .transform('clusters.cluster', 'clusters', parseClusters(clusterTransform))
          .transform('inputs.input', 'inputs', parseInputs(inputTransform))
          .transform('outputs.output', 'outputs', parseOutputs(outputTransform))
          .transform('ACL._owner','ACL.owner')
          .transform('ACL._group','ACL.group')
          .transform('ACL._permission','ACL.permission');


        function parseClusters(transform) {
          return function(clusters) {
            var result = clusters.map(parseCluster(transform));
            return  result;
          };
        }

        return transform.apply(angular.copy(processModel.process), process);
      }

      function parseDate(input) {
        var dateComponent = (input.split('T')[0]).split('-');
        return newUtcDate(dateComponent[0], dateComponent[1], dateComponent[2]);
      }

      function parseTime(input) {
        var timeComponent = (input.split('T')[1].split('Z')[0]).split(':');
        return newUtcTime(timeComponent[0], timeComponent[1]);
      }

      function parseClusters(transform) {
        return function(clusters) {
          var result = clusters.map(parseCluster(transform));
          selectFirstSourceCluster(result);
          return  result;
        };
      }

      function parseInputs(transform) {
        return function(inputs) {
          return inputs.map(parseInput(transform));
        };
      }

      function parseInput(transform) {
        return function(input) {
          return transform.apply(input, EntityFactory.newInput());
        };
      }

      function parseOutputs(transform) {
        return function(outputs) {
          return outputs.map(parseOutput(transform));
        };
      }

      function parseOutput(transform) {
        return function(output) {
          return transform.apply(output, EntityFactory.newOutput());
        };
      }

      function parseCluster(transform) {
        return function(input) {
          var cluster = EntityFactory.newCluster('target', false);
          cluster.storage.fileSystem.active = false;
          return  transform.apply(input, cluster);
        };
      }

      function selectFirstSourceCluster(clusters) {
        for(var i = 0, n = clusters.length; i < n; i++) {
          if(clusters[i].type === 'source') {
            clusters[i].selected = true;
            return;
          }
        }
      }

      function parseKeyValue(keyValue) {
        var parsedPair = keyValue.split('=');
        return EntityFactory.newEntry(parsedPair[0], parsedPair[1]);
      }

      function parseKeyValuePairs(tagsString) {
        return tagsString.split(',').map(parseKeyValue);
      }

      function parseFrequency(frequencyString) {
        var parsedFrequency = frequencyString.split('(');
        return EntityFactory.newFrequency(parsedFrequency[1].split(')')[0], parsedFrequency[0]);
      }

      function parseBoolean(input) {
        return !!input;
      }

      function parseProperties(filterCallback, defaults) {
        return function(properties) {
          var result = filter(properties, filterCallback).map(parseProperty);
          return merge(defaults, result);
        };
      }


      function merge(defaults, newValues) {
        var result = angular.copy(defaults);
        var defaultMap = indexBy(result, 'key');

        newValues.forEach(function(newValue) {
          if(defaultMap[newValue.key]) {
            defaultMap[newValue.key].value = newValue.value;
          } else {
            result.push(newValue);
          }
        });


        return result;
      }


      function filter(array, callback) {
        var out = [];
        for(var i = 0, n = array.length; i < n; i++) {
          if(callback(array[i])) {
            out.push(array[i]);
          }
        }
        return out;
      }


      function parseProperty(property) {
        var value = property._name !== 'timeout' ? property._value : parseFrequency(property._value);
        return EntityFactory.newEntry(property._name, value);
      }

      function parseLocations(locations) {
        return locations.map(parseLocation);
      }

      function parseLocation(location) {
        return EntityFactory.newLocation(location._type, location._path);
      }

      function indexBy(array, property) {
        var map = {};

        array.forEach(function(element) {
          map[element[property]] = element;
        });

        return map;
      }

      function newUtcDate(year, month, day) {
        return new Date(year, (month-1), day);
      }

      function newUtcTime(hours, minutes) {
        return new Date(1900, 1, 1, hours, minutes, 0);
      }

  }]);


  var falconProperties = {
    queueName: true,
    jobPriority: true,
    timeout: true,
    parallel: true,
    maxMaps: true,
    mapBandwidthKB: true
  };


  function isCustomProperty(property) {
    return !falconProperties[property._name];
  }

  function isFalconProperty(property) {
    return falconProperties[property._name];
  }


})();