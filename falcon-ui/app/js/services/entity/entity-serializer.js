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
        return DateHelper.createISO(input.date, input.time, processTz);
      }
      function importDateProcess (input) {
        return DateHelper.importDate(input, processTz);
      }


    return {
      preSerialize: function(entity, type) {
        if(type === 'feed') {
          if(entity.properties) {
            entity.allproperties = entity.properties.concat(entity.customProperties);
          }
          return preSerializeFeed(entity, JsonTransformerFactory);
        } else if(type === 'process') {
          return preSerializeProcess(entity, JsonTransformerFactory);
        } else if(type === 'datasource') {
          if(entity.properties) {
            entity.allProperties = entity.properties.concat(entity.customProperties);
          }
          return preSerializeDatasource(entity, JsonTransformerFactory);
        }
      },

      serialize: function(entity, type) {
        return X2jsService.json2xml_str(this.preSerialize(entity, type));
      },

      preDeserialize: function(entityModel, type) {
        if(type === 'feed') {
          return preDeserializeFeed(entityModel, JsonTransformerFactory);
        } else if(type === 'process') {
          return preDeserializeProcess(entityModel, JsonTransformerFactory);
        } else if(type === 'cluster') {
          return preDeserializeCluster(entityModel, JsonTransformerFactory);
        }  else if(type === 'datasource') {
          return preDeserializeDatasource(entityModel, JsonTransformerFactory);
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

      function emptyProperty (property) {
        return property && property.name && property.name !== ''
          && property.value && property.value !== '';
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

        var partitionTransform = transformerFactory
          .transform('name', '_name');

        var clusterTransform = transformerFactory
          .transform('cluster.name', '_name')
          .transform('cluster.type', '_type')
          .transform('cluster.partition', '_partition')
          .transform('cluster.validity.start', 'validity._start', (function () { feedTz = feed.timezone; return timeAndDateToStringFeed; }()))
          .transform('cluster.validity.end', 'validity._end', timeAndDateToStringFeed)
          .transform('cluster.retention', 'retention._limit', frequencyToString)
          .transform('cluster.retention.action', 'retention._action')
          .transform('cluster','import',function(cluster){
            if(feed.dataTransferType === 'import' && feed.import){
              return dataSourceTransformImport.apply(feed.import, {});
            }
          })
          .transform('cluster','export',function(cluster){
            if(feed.dataTransferType === 'export' && feed.export){
              return dataSourceTransformExport.apply(feed.export, {});
            }
          })
          .transform('cluster', 'locations.location', function(cluster) {
            if ((cluster.type === 'source' && feed.sourceClusterLocationType === 'hdfs')
                || (cluster.type === 'target' && feed.targetClusterLocationType === 'hdfs')
                || (feed.dataTransferType === 'import' && feed.targetClusterLocationType === 'hdfs')
                || (feed.dataTransferType === 'export' && feed.sourceClusterLocationType === 'hdfs')
              ) {
                return transformfileSystem(cluster.storage.fileSystem);
            }
            return null;
          })
          .transform('cluster', 'table', function(cluster) {
            if ((cluster.type === 'source' && feed.sourceClusterLocationType === 'hive')
                || (cluster.type === 'target' && feed.targetClusterLocationType === 'hive')
                || (feed.dataTransferType === 'import' && feed.targetClusterLocationType === 'hive')
                || (feed.dataTransferType === 'export' && feed.sourceClusterLocationType === 'hive')
              ) {
                return transformCatalog(cluster.storage.catalog);
            }
            return null;
          })
          ;

        var fieldTransform = transformerFactory
            .transform('field', 'field');

        var dataSourceTransformImport = transformerFactory
            .transform('source.name','source._name')
            .transform('source.tableName','source._tableName')
            .transform('source.extract.type','source.extract._type')
            .transform('source.extract.mergepolicy','source.extract.mergepolicy')
            .transform('source','source.fields.includes.field',function(source){
              if (source.columnsType === 'include' && source.fields.includes && source.fields.includes.length > 0) {
                return source.fields.includes.map(function (field) {
                  return fieldTransform.apply(field, field);
                });
              }
              return null;
            })
            .transform('source','source.fields.excludes.field',function(source){
              if(source.columnsType === 'exclude' && source.fields.excludes && source.fields.excludes.length > 0){
                return source.fields.excludes.map(function (field) {
                  return fieldTransform.apply(field, field);
                });
              }
              return null;
            })
            ;

          var dataSourceTransformExport = transformerFactory
            .transform('target.name','target._name')
            .transform('target.tableName','target._tableName')
            .transform('target.load.type','target.load._type')
            .transform('target','target.fields.includes.field',function(target){
              if(target.columnsType === 'include' && target.fields.includes && target.fields.includes.length > 0){
                return target.fields.includes.map(function (field) {
                  return fieldTransform.apply(field, field);
                });
              }
              return null;
            })
            .transform('target','target.fields.excludes.field',function(target){
              if(target.columnsType === 'exclude' && target.fields.excludes && target.fields.excludes.length > 0){
                return target.fields.excludes.map(function (field) {
                  return fieldTransform.apply(field, field);
                });
              }
              return null;
            })
            ;

        var transform = transformerFactory
          .transform('name', 'feed._name')
          .transform('description', 'feed._description')
          .transform('tags', 'feed.tags', keyValuePairs)
          .transform('partitions', 'feed.partitions.partition', function(partitions) {
            return partitions.length==0 ? null : partitions.map(function(partition) {
              return partitionTransform.apply(partition, {});
            });
          })
          .transform('groups', 'feed.groups')
          .transform('availabilityFlag', 'feed.availabilityFlag')
          .transform('frequency', 'feed.frequency', frequencyToString)
          .transform('timezone', 'feed.timezone')
          .transform('lateArrival.cutOff', 'feed.late-arrival._cut-off', frequencyToString)
          .transform('clusters', 'feed.clusters.cluster', function(clusters) {
            clusters = clusters.filter(function (cluster) {
              return cluster.name;
            });
            if (!feed.enableFeedReplication) {
              clusters = clusters.filter(function (cluster) {
                return cluster.type == 'source';
              });
            }
            return clusters.map(function(cluster) {
              return clusterTransform.apply({'cluster':cluster}, {});
            });
          })
          .transform('storage.fileSystem', 'feed.locations.location', function(fileSystem) {
            if (feed.dataTransferType === 'hdfs'
              || (feed.dataTransferType === 'import' && feed.targetClusterLocationType === 'hdfs')
              || (feed.dataTransferType === 'export' && feed.sourceClusterLocationType === 'hdfs')
              || (feed.dataTransferType === 'hive' && feed.sourceClusterLocationType === 'hdfs')) {
                return transformfileSystem(fileSystem)
            }
            return null;
          })
          .transform('storage.catalog', 'feed.table', function(catalog) {
            if (feed.dataTransferType === 'hive'
              || (feed.dataTransferType === 'import' && feed.targetClusterLocationType === 'hive')
              || (feed.dataTransferType === 'export' && feed.sourceClusterLocationType === 'hive')
              || (feed.dataTransferType === 'hdfs' && feed.sourceClusterLocationType === 'hive')) {
                return transformCatalog(catalog);
            }
            return null;
          })
          .transform('ACL', 'feed.ACL', emptyElement)
          .transform('ACL.owner', 'feed.ACL._owner')
          .transform('ACL.group', 'feed.ACL._group')
          .transform('ACL.permission', 'feed.ACL._permission')
          .transform('schema', 'feed.schema', emptyElement)
          .transform('schema.location', 'feed.schema._location')
          .transform('schema.provider', 'feed.schema._provider')
          .transform('allproperties', 'feed.properties.property', function(properties) {
            properties = properties.filter(emptyValue);
            return properties.length==0 ? null : properties.map(function(property) {
              return propertyTransform.apply(property, {});
            });
          })
          //.transform('retentionFrequency', 'feed.lifecycle.retention-stage.frequency', frequencyToString)
          ;

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

        var propertyTransform = transformerFactory
          .transform('name', '_name')
          .transform('value', '_value');

        var sparkTransform = transformerFactory
          .transform('spark', 'master', function(spark) {
            if (spark.master === 'yarn') {
              return spark.master + '-' + spark.mode;
            } else {
              return spark.master;
            }
          })
          .transform('spark', 'mode', function(spark) {
            if (spark.master && spark.master.indexOf('yarn') != '-1') {
              return spark.mode;
            } else {
              return null;
            }
          })
          .transform('spark.name', 'name')
          .transform('spark.class', 'class')
          .transform('spark.jar', 'jar')
          .transform('spark.sparkOptions', 'spark-opts')
          .transform('spark.arg', 'arg');

        var workflowNameCheck = function(workflow) {
          if (workflow.engine === 'spark') {
            return null;
          } else {
            return workflow.name;
          }
        }

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
          .transform('properties', 'process.properties.property', function(properties) {
            properties = properties.filter(emptyProperty);
            if(properties.length === 0) {
              return null;
            } else {
              return properties.map(function(property) {
                return propertyTransform.apply(property, {});
              });
            }
          })
          .transform('workflow', 'process.workflow._name', workflowNameCheck)
          .transform('workflow.version', 'process.workflow._version')
          .transform('workflow.engine', 'process.workflow._engine')
          .transform('workflow.path', 'process.workflow._path')
          .transform('workflow', 'process.spark-attributes', function(workflow) {
            if (workflow.engine === 'spark') {
              return sparkTransform.apply(workflow, {});
            }
          })
          .transform('retry.policy', 'process.retry._policy')
          .transform('retry.delay', 'process.retry._delay', frequencyToString)
          .transform('retry.attempts', 'process.retry._attempts')
          .transform('ACL', 'process.ACL', emptyElement)
          .transform('ACL.owner', 'process.ACL._owner')
          .transform('ACL.group', 'process.ACL._group')
          .transform('ACL.permission', 'process.ACL._permission');


        return transform.apply(process, new EntityModel('process'));

      }

      function preSerializeDatasource (datasource, transformerFactory) {
        var credentialPasswordTextTransform = transformerFactory
          .transform('type', '_type')
          .transform('userName', 'userName')
          .transform('passwordText', 'passwordText', function(value) {
            return value;
          });

        var credentialPasswordFileTransform = transformerFactory
          .transform('type', '_type')
          .transform('userName', 'userName')
          .transform('passwordFile', 'passwordFile');

        var credentialPasswordAliasTransform = transformerFactory
          .transform('type', '_type')
          .transform('userName', 'userName')
          .transform('passwordAlias', 'passwordAlias.alias')
          .transform('providerPath', 'passwordAlias.providerPath');

        var credentialTransform = function(credential)  {
          if (credential.type == 'password-text') {
            return credentialPasswordTextTransform.apply(credential, {});
          } if (credential.type == 'password-file') {
            return credentialPasswordFileTransform.apply(credential, {});
          } if (credential.type == 'password-alias') {
            return credentialPasswordAliasTransform.apply(credential, {});
          } else {
            return null;
          }
        };

        var interfaceTransform = transformerFactory
          .transform('type', '_type')
          .transform('endpoint', '_endpoint')
          .transform('credential.type', 'credential._type')
          .transform('credential', 'credential', credentialTransform);

        var propertyTransform = transformerFactory
          .transform('name', '_name')
          .transform('value', '_value', function(value) {
            return value;
          });
        var driverJarsTransform = function(driverJars){
          var filtered = driverJars.filter(function(jar){
            return jar.value && jar.value.trim().length > 0;
          });
          if(filtered.length > 0){
            return filtered.map(function(jar){
              return jar.value;
            });
          }
        };
        var transform = transformerFactory
          .transform('colo', 'datasource._colo')
          .transform('description', 'datasource._description')
          .transform('type', 'datasource._type')
          .transform('name', 'datasource._name')
          .transform('tags', 'datasource.tags', keyValuePairs)
          .transform('interfaces', 'datasource.interfaces.interface', function(datasourceInterfaces) {
            return datasourceInterfaces.interfaces.length==0 ? null
              : datasourceInterfaces.interfaces.map(function(datasourceInterface) {
                  return interfaceTransform.apply(datasourceInterface, {});
                });
          })
          .transform('interfaces.credential', 'datasource.interfaces.credential', credentialTransform)
          .transform('driver.clazz', 'datasource.driver.clazz')
          .transform('driver.jar', 'datasource.driver.jar',driverJarsTransform)
          .transform('allProperties', 'datasource.properties.property', function(properties) {
            properties = properties.filter(emptyValue).filter(emptyFrequency);
            return properties.length==0 ? null : properties.map(function(property) {
              return propertyTransform.apply(property, {});
            });
          })
          .transform('ACL', 'datasource.ACL', emptyElement)
          .transform('ACL.owner', 'datasource.ACL._owner')
          .transform('ACL.group', 'datasource.ACL._group')
          .transform('ACL.permission', 'datasource.ACL._permission')

        return transform.apply(datasource, new EntityModel('datasource'));

      }

      function preDeserializeCluster(clusterModel, transformerFactory) {

        var cluster = EntityFactory.newClusterEntity();

        var transform = transformerFactory
            .transform('_name', 'name')
            .transform('_colo','colo')
            .transform('_description', 'description')
            .transform('tags', 'tags', parseKeyValuePairs)
            .transform('ACL._owner','ACL.owner')
            .transform('ACL._group','ACL.group')
            .transform('ACL._permission','ACL.permission')
            .transform('locations.location', 'locations', parseClusterLocations)
            .transform('properties.property', 'properties', parseClusterProperties)
            .transform('interfaces.interface', 'interfaces', parseClusterInterfaces);

        return transform.apply(angular.copy(clusterModel.cluster), cluster);
      }

      function preDeserializeFeed(feedModel, transformerFactory) {

        var feed = EntityFactory.newFeed();

        var clusterTransform = transformerFactory
            .transform('_name', 'name')
            .transform('_type', 'type')
            .transform('_partition', 'partition')
            .transform('validity._start', 'validity.start.date', (function () { feedTz = feedModel.feed.timezone; return importDateFeed; }()))
            .transform('validity._start', 'validity.start.time', importDateFeed)
            .transform('validity._end', 'validity.end.date', importDateFeed)
            .transform('validity._end', 'validity.end.time', importDateFeed)
            .transform('retention._limit', 'retention', parseFrequency)
            .transform('retention._action', 'retention.action')
            .transform('locations.location', 'storage.fileSystem.locations', parseLocations)
            .transform('table._uri', 'storage.catalog.catalogTable.uri')
          ;
          var fieldTransform = transformerFactory
              .transform('field', 'field');
          var dataSourceTransformImport = transformerFactory
              .transform('_name','name')
              .transform('_tableName','tableName')
              .transform('extract._type','extract.type')
              .transform('extract.mergepolicy','extract.mergepolicy')
              .transform('fields.excludes.field','excludesCSV', parseFields)
              .transform('fields.includes.field','includesCSV', parseFields)
              ;

          var dataSourceTransformExport = transformerFactory
              .transform('_name','name')
              .transform('_tableName','tableName')
              .transform('load._type','load.type')
              .transform('fields.includes.field','includesCSV', parseFields)
              .transform('fields.excludes.field','excludesCSV', parseFields)
              ;

        var transform = transformerFactory
            .transform('_name', 'name')
            .transform('_description', 'description')
            .transform('tags', 'tags', parseKeyValuePairs)
            .transform('groups','groups')
            .transform('availabilityFlag', 'availabilityFlag')
            .transform('ACL._owner','ACL.owner')
            .transform('ACL._group','ACL.group')
            .transform('ACL._permission','ACL.permission')
            .transform('schema._location','schema.location')
            .transform('schema._provider','schema.provider')
            .transform('frequency','frequency', parseFrequency)
            .transform('late-arrival','lateArrival.active', parseBoolean)
            .transform('late-arrival._cut-off','lateArrival.cutOff', parseFrequency)
            .transform('properties.property', 'customProperties', parseProperties(isCustomProperty, EntityFactory.newFeedCustomProperties()))
            .transform('properties.property', 'properties', parseProperties(isFalconProperty, EntityFactory.newFeedProperties()))
            .transform('locations.location', 'storage.fileSystem.locations', parseLocations)
            .transform('table._uri', 'storage.catalog.catalogTable.uri')
            .transform('partitions.partition', 'partitions', parsePartitions)
            .transform('clusters.cluster', 'clusters', parseClusters(feed,clusterTransform))
            .transform('clusters.cluster', 'datasources', parseFeedDatasources)
            .transform('timezone', 'timezone')
            ;

            function parseFeedDatasources(clusters) {
              if (clusters.length > 0) {
                var cluster = clusters[0];
                if (cluster.import) {
                  feed.dataTransferType = 'import';
                  feed.import = { 'source' : dataSourceTransformImport.apply(cluster.import.source, {}) };
                  if(cluster._type ==='source' && feed.storage.catalog.catalogTable.uri !== null){
                    feed.targetClusterLocationType = 'hive';
                  } else {
                    feed.targetClusterLocationType = 'hdfs';
                  }
                } else if (clusters[0].export) {
                  feed.dataTransferType = 'export';
                  feed.export = { 'target' : dataSourceTransformExport.apply(clusters[0].export.target, {}) };
                  if(cluster._type ==='source' && feed.storage.catalog.catalogTable.uri !== null){
                    feed.targetClusterLocationType = 'hive';
                  } else {
                    feed.targetClusterLocationType = 'hdfs';
                  }
                }
              }
              return null;
            }

            function parseFields(fields) {
              return $.isArray(fields) ? fields.join() : fields;
            }


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

        var propertyTransform = transformerFactory
          .transform('_name', 'name')
          .transform('_value', 'value');

        var sparkTransform = transformerFactory
          .transform('master', 'master')
          .transform('mode', 'mode')
          .transform('name', 'name')
          .transform('class', 'class')
          .transform('jar', 'jar')
          .transform('spark-opts', 'sparkOptions')
          .transform('arg', 'arg');

        var transform = transformerFactory
          .transform('_name', 'name')
          .transform('tags', 'tags', parseKeyValuePairs)
          .transform('workflow._name', 'workflow.name')
          .transform('workflow._version', 'workflow.version')
          .transform('workflow._engine', 'workflow.engine')
          .transform('workflow._path', 'workflow.path')
          .transform('spark-attributes', 'workflow.spark', parseSparkProperties(sparkTransform))
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
          .transform('properties.property', 'properties', parseProperties)
          .transform('ACL._owner','ACL.owner')
          .transform('ACL._group','ACL.group')
          .transform('ACL._permission','ACL.permission');


        function parseClusters(transform) {
          return function(clusters) {
            var result = clusters.map(parseCluster(transform));
            return  result;
          };
        }

        function parseProperties(properties) {
          return $.isArray(properties) ? properties.map(parseProperty) : [parseProperty(properties)];
        }

        function parseProperty(property) {
          return EntityFactory.newProperty(property._name, property._value);
        }

        function parseSparkProperties(transform) {
          return function(sparkAttributes) {
            if (sparkAttributes.master && sparkAttributes.master.indexOf('yarn') !== '-1') {
              sparkAttributes.master = 'yarn';
            }
            return sparkTransform.apply(sparkAttributes, EntityFactory.newSparkAttributes());
          };
        }

        return transform.apply(angular.copy(processModel.process), process);
      }

      function preDeserializeDatasource(datasourceModel, transformerFactory) {
          var datasource = EntityFactory.newDatasource();

          function parseProperty(property) {
            return EntityFactory.newProperty(property._name, property._value);
          }

          function parseProperties(filterCallback) {
            return function(properties) {
              var result = filter(properties, filterCallback).map(parseProperty);
              return result;
            };
          }

          function parseInterface(datasourceinterface) {
            var interfaceTransform = transformerFactory
              .transform('_type', 'type')
              .transform('_endpoint', 'endpoint')
              .transform('credential._type', 'credential.type')
              .transform('credential.userName', 'credential.userName')
              .transform('credential.passwordText', 'credential.passwordText')
              .transform('credential.passwordFile', 'credential.passwordFile')
              .transform('credential.passwordAlias.alias', 'credential.passwordAlias')
              .transform('credential.passwordAlias.providerPath', 'credential.providerPath');

              return interfaceTransform.apply(datasourceinterface, EntityFactory.newDatasourceInterface());
          }

          function parseInterfaces(interfaces) {
              return $.isArray(interfaces) ? interfaces.map(parseInterface) : [parseInterface(interfaces)];
          }
          function parseDriverJar(jar){
            return {value : jar};
          }
          function parseDriverJars(jars) {
            return $.isArray(jars) ? jars.map(parseDriverJar) : [parseDriverJar(jars)];
          }
          var transform = transformerFactory
              .transform('_name', 'name')
              .transform('_description', 'description')
              .transform('tags', 'tags', parseKeyValuePairs)
              .transform('_colo','colo')
              .transform('_type','type')
              .transform('ACL._owner','ACL.owner')
              .transform('ACL._group','ACL.group')
              .transform('ACL._permission','ACL.permission')
              .transform('driver.clazz','driver.clazz')
              .transform('driver.jar','driver.jar',parseDriverJars)
              .transform('interfaces.interface', 'interfaces.interfaces', parseInterfaces)
              .transform('properties.property', 'properties', parseProperties(isDatasourceProperty))
              .transform('properties.property', 'customProperties', parseProperties(isCustomDatasourceProperty));

          return transform.apply(angular.copy(datasourceModel.datasource), datasource);

      }

      function parseDate(input) {
        var dateComponent = (input.split('T')[0]).split('-');
        return newUtcDate(dateComponent[0], dateComponent[1], dateComponent[2]);
      }

      function parseTime(input) {
        var timeComponent = (input.split('T')[1].split('Z')[0]).split(':');
        return newUtcTime(timeComponent[0], timeComponent[1]);
      }

      function parseClusters(feed, transform) {
        return function(clusters) {
          if (clusters.length > 0 && clusters[0] === "") {
            return null;
          }
          var result = clusters.map(parseCluster(transform));
          result.forEach(function(cluster){
            if (cluster.type ==='target') {
              feed.enableFeedReplication = true;
            }
            if(cluster.type ==='source' && cluster.storage.catalog){
              feed.sourceClusterLocationType = 'hive';
            }
            if(cluster.type ==='target' && cluster.storage.catalog){
              feed.targetClusterLocationType = 'hive';
            }
            if(cluster.type ==='source' && cluster.storage.fileSystem){
              feed.sourceClusterLocationType = 'hdfs';
            }
            if(cluster.type ==='target' && cluster.storage.fileSystem){
              feed.targetClusterLocationType = 'hdfs';
            }
          });
          if (feed.sourceClusterLocationType === 'hive'
            && (!feed.targetClusterLocationType || feed.targetClusterLocationType === 'hive')) {
            feed.dataTransferType = 'hive';
          } else if (feed.sourceClusterLocationType === 'hdfs'
            && (!feed.targetClusterLocationType || feed.targetClusterLocationType === 'hdfs')) {
            feed.dataTransferType = 'hdfs';
          }
          if (!feed.targetClusterLocationType && (feed.dataTransferType === 'hdfs' || feed.dataTransferType === 'hive')) {
            feed.targetClusterLocationType = feed.dataTransferType;
          }
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
          //cluster.storage.fileSystem.active = false;
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

      function parseClusterLocations(locations) {
        return $.isArray(locations) ? locations.map(parseClusterLocation) : [parseClusterLocation(locations)];
      }

      function parseClusterLocation(location) {
        return EntityFactory.newClusterLocation(location._name, location._path);
      }

      function parseClusterProperties(properties) {
        return $.isArray(properties) ? properties.map(parseClusterProperty) : [parseClusterProperty(properties)];
      }

      function parseClusterProperty(property) {
        return EntityFactory.newEntry(property._name, property._value);
      }

      function parseClusterInterfaces(interfaces) {
        return $.isArray(interfaces) ? interfaces.map(parseClusterInterface) : [parseClusterInterface(interfaces)];
      }

      function parseClusterInterface(clusterInterface) {
        return EntityFactory.newClusterInterface(
          clusterInterface._type, clusterInterface._endpoint, clusterInterface._version);
      }

      function parsePartitions(partitions) {
        return $.isArray(partitions) ? partitions.map(parsePartititon) : [parsePartititon(partitions)];
      }

      function parsePartititon(partition) {
        return EntityFactory.newPartition(partition._name);
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

  var datasourceProperties = {
    parameterFile: true,
    verboseMode: true,
    directMode: true
  };

  function isCustomDatasourceProperty(property) {
    return !datasourceProperties[property._name];
  }

  function isDatasourceProperty(property) {
    return datasourceProperties[property._name];
  }

})();
