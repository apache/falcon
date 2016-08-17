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
  var module = angular.module('app.services.extension.serializer',
    ['app.services',
      'app.services.entity.factory',
      'app.services.entity.model']);

  module.factory('ExtensionSerializer', ['EntityFactory', 'DateHelper', 'EntityModel',
    function(EntityFactory, DateHelper, EntityModel) {

      var convertTags = function (tagsArray) {
        var result = [];
        tagsArray.forEach(function(element) {
          if(element.key && element.value) {
            result.push(element.key + "=" + element.value);
          }
        });
        result = result.join(",");
        return result;
      };

      var convertObjectToString = function (obj) {
        var str = '';
        for (var key in obj) {
          if (obj.hasOwnProperty(key)) {
              str += key + '=' + obj[key] + '\n';
          }
        }
        return str;
      };

      var serializeExtensionCommonProperties = function(extension) {
        var extensionProps = {};
        extensionProps.jobName = extension.name;
        extensionProps.jobValidityStart = DateHelper.createISO(extension.validity.start.date,
          extension.validity.start.time, extension.validity.timezone);
        extensionProps.jobValidityEnd = DateHelper.createISO(extension.validity.end.date,
          extension.validity.end.time, extension.validity.timezone);
        extensionProps.jobFrequency = extension.frequency.unit + '(' + extension.frequency.quantity + ')';
        extensionProps.jobTimezone = extension.validity.timezone;
        extensionProps.jobTags = convertTags(extension.tags);
        extensionProps.jobRetryPolicy = extension.retry.policy;
        extensionProps.jobRetryDelay = extension.retry.delay.unit + '(' + extension.retry.delay.quantity + ')';
        extensionProps.jobRetryAttempts = extension.retry.attempts;
        extensionProps.jobAclOwner = extension.ACL.owner;
        extensionProps.jobAclGroup = extension.ACL.group;
        extensionProps.jobAclPermission = extension.ACL.permission;
        extensionProps.tdeEncryptionEnabled = extensionProps.tdeEncryptionEnabled;

        extensionProps.sourceCluster = extension.source.cluster;
        extensionProps.targetCluster = extension.target.cluster;
        if (extension.runOn === 'source') {
          extensionProps.jobClusterName = extension.source.cluster;
        } else if (extension.runOn === 'target') {
          extensionProps.jobClusterName = extension.target.cluster;
        }
        if (extension.alerts.length > 0) {
          extensionProps.jobNotificationType = 'email';
          extensionProps.jobNotificationReceivers = extension.alerts.join();
        }
        return extensionProps;
      };

      var serializeSnapshotExtensionProperties = function(snapshot) {
        var snapshotProps = serializeExtensionCommonProperties(snapshot);
        snapshotProps.sourceSnapshotDir = snapshot.source.directoryPath.trim();
        snapshotProps.targetSnapshotDir = snapshot.target.directoryPath.trim();
        snapshotProps.sourceSnapshotRetentionAgeLimit = snapshot.source.deleteFrequency.unit
          + '(' + snapshot.source.deleteFrequency.quantity + ')';
        snapshotProps.targetSnapshotRetentionAgeLimit = snapshot.target.deleteFrequency.unit
          + '(' + snapshot.target.deleteFrequency.quantity + ')';
        snapshotProps.sourceSnapshotRetentionNumber = snapshot.source.retentionNumber;
        snapshotProps.targetSnapshotRetentionNumber = snapshot.target.retentionNumber;
        if (snapshot.allocation) {
          if (snapshot.allocation.distcpMaxMaps) {
            snapshotProps.distcpMaxMaps = snapshot.allocation.distcpMaxMaps;
          }
          if (snapshot.allocation.distcpMapBandwidth) {
            snapshotProps.distcpMapBandwidth = snapshot.allocation.distcpMapBandwidth;
          }
        }

        return snapshotProps;
      };

      var serializeHDFSExtensionProperties = function(hdfsExtension, secureMode) {
        var hdfsExtensionProps = serializeExtensionCommonProperties(hdfsExtension);
        hdfsExtensionProps.sourceDir = hdfsExtension.source.path.trim();
        hdfsExtensionProps.targetDir = hdfsExtension.target.path.trim();
        if (hdfsExtension.allocation.hdfs) {
          if (hdfsExtension.allocation.hdfs.distcpMaxMaps) {
            hdfsExtensionProps.distcpMaxMaps = hdfsExtension.allocation.hdfs.distcpMaxMaps;
          }
          if (hdfsExtension.allocation.hdfs.distcpMapBandwidth) {
            hdfsExtensionProps.distcpMapBandwidth = hdfsExtension.allocation.hdfs.distcpMapBandwidth;
          }
        }
        return hdfsExtensionProps;
      };

      var serializeHiveExtensionProperties = function(hiveEntension, secureMode) {
        var hiveEntensionProps = serializeExtensionCommonProperties(hiveEntension);
        if (hiveEntension.allocation.hive) {
          if (hiveEntension.allocation.hive.distcpMaxMaps) {
            hiveEntensionProps.distcpMaxMaps = hiveEntension.allocation.hive.distcpMaxMaps;
          }
          if (hiveEntension.allocation.hive.distcpMapBandwidth) {
            hiveEntensionProps.distcpMapBandwidth = hiveEntension.allocation.hive.distcpMapBandwidth;
          }
          if (hiveEntension.allocation.hive.maxEvents) {
            hiveEntensionProps.maxEvents = hiveEntension.allocation.hive.maxEvents;
          }
          if (hiveEntension.allocation.hive.replicationMaxMaps) {
            hiveEntensionProps.replicationMaxMaps = hiveEntension.allocation.hive.replicationMaxMaps;
          }
        }

        hiveEntensionProps.sourceDatabases = hiveEntension.source.hiveDatabases;
        hiveEntensionProps.sourceTables = (hiveEntension.source.hiveDatabaseType === "databases")
          ? "*" : hiveEntension.source.hiveTables;

        hiveEntensionProps.sourceStagingPath = hiveEntension.hiveOptions.source.stagingPath
        hiveEntensionProps.sourceHiveServer2Uri = hiveEntension.hiveOptions.source.hiveServerToEndpoint;
        hiveEntensionProps.targetStagingPath = hiveEntension.hiveOptions.target.stagingPath
        hiveEntensionProps.targetHiveServer2Uri = hiveEntension.hiveOptions.target.hiveServerToEndpoint;

        // Secure mode
        if (secureMode) {
          hiveEntensionProps.sourceMetastoreUri = hiveEntension.source.hiveMetastoreUri;
          hiveEntensionProps.sourceHiveMetastoreKerberosPrincipal = hiveEntension.source.hiveMetastoreKerberosPrincipal;
          hiveEntensionProps.sourceHive2KerberosPrincipal = hiveEntension.source.hive2KerberosPrincipal;
          hiveEntensionProps.targetMetastoreUri = hiveEntension.target.hiveMetastoreUri;
          hiveEntensionProps.targetHiveMetastoreKerberosPrincipal = hiveEntension.target.hiveMetastoreKerberosPrincipal;
          hiveEntensionProps.targetHive2KerberosPrincipal =  hiveEntension.target.hive2KerberosPrincipal;
        }

        return hiveEntensionProps;
      };

      var serializeBasicExtensionModel = function(model, extensionObj) {

        extensionObj.name = model.process._name;

        extensionObj.retry.policy = model.process.retry._policy;
        extensionObj.retry.attempts = model.process.retry._attempts;
        extensionObj.retry.delay.quantity = (function () {
          return parseInt(model.process.retry._delay.split('(')[1]);
        }());
        extensionObj.retry.delay.unit = (function () {
          return model.process.retry._delay.split('(')[0];
        }());

        extensionObj.frequency.quantity = (function () {
          return parseInt(model.process.frequency.split('(')[1]);
        }());
        extensionObj.frequency.unit = (function () {
          return model.process.frequency.split('(')[0];
        }());

        // extensionObj.ACL.owner = model.process.ACL._owner;
        // extensionObj.ACL.group = model.process.ACL._group;
        // extensionObj.ACL.permissions = model.process.ACL._permission;

        extensionObj.validity.timezone = model.process.timezone;
        extensionObj.validity.start.date = DateHelper.importDate(
          model.process.clusters.cluster[0].validity._start, model.process.timezone);
        extensionObj.validity.start.time = DateHelper.importDate(
          model.process.clusters.cluster[0].validity._start, model.process.timezone);
        extensionObj.validity.end.date = DateHelper.importDate(
          model.process.clusters.cluster[0].validity._end, model.process.timezone);
        extensionObj.validity.end.time = DateHelper.importDate(
          model.process.clusters.cluster[0].validity._end, model.process.timezone);

        extensionObj.tags = (function () {
          var array = [];
          if(model.process && model.process.tags){
            model.process.tags.split(',').forEach(function (fieldToSplit) {
              var splittedString = fieldToSplit.split('=');
              if (splittedString[0] != '_falcon_extension_name' && splittedString[0] != '_falcon_extension_job') {
                array.push({key: splittedString[0], value: splittedString[1]});
              }
            });
          }
          return array;
        }());

        if(model.process && model.process.tags){
          if (model.process.tags.indexOf('_falcon_extension_name=HDFS-MIRRORING') !== -1) {
            extensionObj.type = 'HDFS';
          } else if (model.process.tags.indexOf('_falcon_extension_name=HDFS-SNAPSHOT-MIRRORING') !== -1) {
            extensionObj.type = 'snapshot';
          } else if (model.process.tags.indexOf('_falcon_extension_name=HIVE-MIRRORING') !== -1) {
            extensionObj.type = 'HIVE';
          }
        }

        if (model.process.notification._to) {
          extensionObj.alerts = (function () {
            if (model.process.notification._to !== "NA") {
              return model.process.notification._to.split(',');
            } else {
              return [];
            }
          }());
        }

        model.process.properties.property.forEach(function (item) {
            if (item._name === 'targetCluster') {
              extensionObj.target.cluster = item._value;
            }
            if (item._name === 'sourceCluster') {
              extensionObj.source.cluster = item._value;
            }
          });

          return extensionObj;
      };

      var serializeSnapshotExtensionModel = function(model, snapshotObj) {
        snapshotObj = serializeBasicExtensionModel(model, snapshotObj);

        model.process.properties.property.forEach(function (item) {
            if (item._name === 'distcpMaxMaps') {
              snapshotObj.allocation.distcpMaxMaps = item._value;
            }
            if (item._name === 'distcpMapBandwidth') {
              snapshotObj.allocation.distcpMapBandwidth = item._value;
            }
            if (item._name === 'tdeEncryptionEnabled') {
              if (item._value === 'true') {
                snapshotObj.tdeEncryptionEnabled = true;
              } else {
                snapshotObj.tdeEncryptionEnabled = false;
              }
            }
            if (item._name === 'sourceSnapshotDir') {
              snapshotObj.source.directoryPath = item._value;
            }
            if (item._name === 'targetSnapshotDir') {
              snapshotObj.target.directoryPath = item._value;
            }
            if (item._name === 'sourceSnapshotRetentionNumber') {
              snapshotObj.source.retentionNumber = item._value;
            }
            if (item._name === 'targetSnapshotRetentionNumber') {
              snapshotObj.target.retentionNumber = item._value;
            }
            if (item._name === 'sourceSnapshotRetentionAgeLimit') {
              snapshotObj.source.deleteFrequency.quantity = (function () {
                return parseInt(item._value.split('(')[1]);
              }());
              snapshotObj.source.deleteFrequency.unit = (function () {
                return item._value.split('(')[0];
              }());
            }
            if (item._name === 'targetSnapshotRetentionAgeLimit') {
              snapshotObj.target.deleteFrequency.quantity = (function () {
                return parseInt(item._value.split('(')[1]);
              }());
              snapshotObj.target.deleteFrequency.unit = (function () {
                return item._value.split('(')[0];
              }());
            }
          });

          return snapshotObj;
      };

      var serializeHDFSExtensionModel = function(model, hdfsExtensionObj) {
        hdfsExtensionObj = serializeBasicExtensionModel(model, hdfsExtensionObj);
        model.process.properties.property.forEach(function (item) {
            if (item._name === 'distcpMaxMaps') {
              hdfsExtensionObj.allocation.hdfs.distcpMaxMaps = item._value;
            }
            if (item._name === 'distcpMapBandwidth') {
              hdfsExtensionObj.allocation.hdfs.distcpMapBandwidth = item._value;
            }
            // if (item._name === 'tdeEncryptionEnabled') {
            //   if (item._value === 'true') {
            //     hdfsExtensionObj.tdeEncryptionEnabled = true;
            //   } else {
            //     hdfsExtensionObj.tdeEncryptionEnabled = false;
            //   }
            // }
            if (item._name === 'sourceDir') {
              hdfsExtensionObj.source.path = item._value;
            }
            if (item._name === 'targetDir') {
              hdfsExtensionObj.target.path = item._value;
            }
          });

          return hdfsExtensionObj;
      };

      var serializeHiveExtensionModel = function(model, hiveExtensionObj, secureMode) {
        hiveExtensionObj = serializeBasicExtensionModel(model, hiveExtensionObj);
        model.process.properties.property.forEach(function (item) {
            if (item._name === 'distcpMaxMaps') {
              hiveExtensionObj.allocation.hive.distcpMaxMaps = item._value;
            }
            if (item._name === 'distcpMapBandwidth') {
              hiveExtensionObj.allocation.hive.distcpMapBandwidth = item._value;
            }
            if (item._name === 'tdeEncryptionEnabled') {
              if (item._value === 'true') {
                hiveExtensionObj.tdeEncryptionEnabled = true;
              } else {
                hiveExtensionObj.tdeEncryptionEnabled = false;
              }
            }
            if (item._name === 'replicationMaxMaps') {
              hiveExtensionObj.allocation.hive.replicationMaxMaps = item._value;
            }
            if (item._name === 'maxEvents') {
              hiveExtensionObj.allocation.hive.maxEvents = item._value;
            }
            if (item._name === 'sourceTables') {
              if (item._value === "*") {
                hiveExtensionObj.source.hiveDatabaseType = "databases";
              } else {
                hiveExtensionObj.source.hiveDatabaseType = "tables";
                hiveExtensionObj.source.hiveTables = item._value;
              }
            }
            if (item._name === 'sourceDatabases') {
                hiveExtensionObj.source.hiveDatabases = item._value;
            }

            if (item._name === 'sourceStagingPath') {
              hiveExtensionObj.hiveOptions.source.stagingPath = item._value;
            }
            if (item._name === 'targetStagingPath') {
              hiveExtensionObj.hiveOptions.target.stagingPath = item._value;
            }

            if (item._name === 'sourceHiveServer2Uri') {
              hiveExtensionObj.hiveOptions.source.hiveServerToEndpoint = item._value;
            }
            if (item._name === 'targetHiveServer2Uri') {
              hiveExtensionObj.hiveOptions.target.hiveServerToEndpoint = item._value;
            }

            // Secure Mode Properties
            if (secureMode) {
              if(item._name === 'sourceMetastoreUri') {
                hiveExtensionObj.source.hiveMetastoreUri = item._value;
              }
              if (item._name === 'sourceHiveMetastoreKerberosPrincipal') {
                hiveExtensionObj.source.hiveMetastoreKerberosPrincipal = item._value;
              }
              if (item._name === 'sourceHive2KerberosPrincipal') {
                hiveExtensionObj.source.hive2KerberosPrincipal = item._value;
              }
              if(item._name === 'targetMetastoreUri') {
                hiveExtensionObj.target.hiveMetastoreUri = item._value;
              }
              if (item._name === 'targetHiveMetastoreKerberosPrincipal') {
                hiveExtensionObj.target.hiveMetastoreKerberosPrincipal = item._value;
              }
              if (item._name === 'targetHive2KerberosPrincipal') {
                hiveExtensionObj.target.hive2KerberosPrincipal = item._value;
              }
            }
          });

          return hiveExtensionObj;
      };

      var serializeExtensionProperties = function(extension, extensionType, secureMode) {
        if (extensionType === 'snapshot') {
          return serializeSnapshotExtensionProperties(extension);
        } else if (extensionType === 'HDFS-MIRROR') {
          return serializeHDFSExtensionProperties(extension, secureMode);
        }  else if (extensionType === 'HIVE-MIRROR') {
          return serializeHiveExtensionProperties(extension, secureMode);
        }
      };

      var serializeExtensionModel = function(extensionModel, extensionType, secureMode) {
        if (extensionType === 'snapshot') {
          return serializeSnapshotExtensionModel(extensionModel, EntityFactory.newEntity('snapshot'));
        } else if (extensionType === 'hdfs-mirror') {
          return serializeHDFSExtensionModel(extensionModel, EntityModel.datasetModel.UIModel);
        } else if (extensionType === 'hive-mirror') {
          return serializeHiveExtensionModel(extensionModel, EntityModel.datasetModel.UIModel, secureMode);
        }
      };

      return {
        serializeExtensionProperties: serializeExtensionProperties,
        serializeExtensionModel: serializeExtensionModel,
        convertObjectToString: convertObjectToString,
        convertTags: convertTags
      };

  }]);
})();
