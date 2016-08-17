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

  var module = angular.module('app.services.tooltip', []);

  module.factory('TooltipMessages', ['$window', function ($window) {
    var messages = {
        'cluster.name': 'Unique. No special characters. Maximum 40 Characters',
        'cluster.colo': 'Data center or co-location of this cluster',
        'cluster.interfaces.readonly': 'URI for read operations<br>Eg. hdfs://localhost:50070 | webhdfs://localhost:50070 | hftp://localhost:50070',
        'cluster.interfaces.write': 'URI for write operations<br>Eg. hdfs://localhost:8020',
        'cluster.interfaces.execute': 'URI for executing jobs<br>Eg. localhost:8050',
        'cluster.interfaces.workflow': 'URI to access the workflow manager<br>Eg. http://localhost:11000/oozie/',
        'cluster.interfaces.messaging': 'URI for Falcon message broker<br>Eg. tcp://localhost:61616?daemon=true',
        'cluster.interfaces.registry': 'URI to the Hive Host Thrift port<br>Eg. thrift://localhost:9083',
        'cluster.interfaces.spark': 'URI for the Spark Master',
        'cluster.locations.staging': 'Default HDFS directory for staging on this cluster. The directory should have 777 permisssion and owned by falcon.',
        'cluster.locations.temp': 'Default HDFS directory for temporary storage on this cluster',
        'cluster.locations.working': 'Default HDFS directory for working storage on this cluster.The directory should have 755 permisssion and owned by falcon.',

        'feed.name': 'Unique. No special characters. Maximum 40 Characters',
        'feed.groups': 'Comma seperated list of Feed Groups this Feed is part of',
        'feed.schema.location': 'Location of the file containing the layout for the feed',
        'feed.schema.provider': 'Data interchange protocol of the Feed:  Avro, Hive, RDBMS, etc',
        'feed.properties.frequency': 'Frequency of feed generation.',
        'feed.properties.lateArrival': 'Specify how long Feed processing should wait for the required feed to become available.',
        'feed.properties.availabilityIndicator': 'If one exists, provide the name of file whose existance indicates the feed is available for use',
        'feed.properties.timezone': 'Timezone associate with the feed, if different from the cluster default timezone.',
        'feed.location.storageType': 'Select Catalog for Hive tables, File System for HDFS',
        'feed.storage.tableUri': 'Eg. catalog:default:sample_07#ds=${YEAR}-${MONTH}-${DAY}-${HOUR}',

        'process.name': 'Unique. No special characters.',
        'process.workflow.path': 'Must specify a valid HDFS script of the engine type (Pig, Hive, Oozie, Spark)',
        'process.properties.timezone': 'Timezone associate with the feed, if different from the cluster default timezone.',
        'process.properties.retryPolicy': 'Workflow failure handling policy'
          + '<div class="pt5px"><b>Periodic:</b><br>Try X times after  N Min/Hours/</div>'
          + '<div class="pt5px"><b>Exponential Backup:</b><br>Try X times after N to the x1, N to x2, N to x3, etc.</div>'
          + '<div class="pt5px"><b>None:</b><br>Do not retry</div>',
        'process.properties.order': 'Order for instance pickup'
          + '<div class="pt5px"><b>FIFO:</b><br>Oldest to Latest</div>'
          + '<div class="pt5px"><b>LIFO:</b><br>Latest to Oldest</div>'
          + '<div class="pt5px"><b>LASTONLY:</b><br>Latest only</div>',
        'process.cluster': 'Cluster(s) this process should execute on',

        'process.workflow.spark.name': 'Name of the spark application',
        'process.workflow.spark.application': 'Path of jar or python file',
        'process.workflow.spark.class': "Spark application's main class",
        'process.workflow.spark.master': 'Run in Yarn or directly on spark',
        'process.workflow.spark.mode': 'Run locally or or remote on the Application Master',

        'datasource.driver.jar': 'HDFS location containing driver jar and should have access to the falcon user',
        'datasource.credential.passwordFile': 'HDFS location containing password file without newline at the end and should be accessible by the user who creates the datasource entity',

        'dataset.name': 'Unique job name',
        'dataset.jobClusterName': 'Cluster where job should run',
        'dataset.jobValidityStart': 'Job validity start time',
        'dataset.jobValidityEnd': 'Job validity end time',
        'dataset.jobFrequency': 'Job frequency. Valid frequency types are minutes, hours, days, months',
        'dataset.jobTimezone': 'Time zone for the job',
        'dataset.jobTags': 'list of comma separated tags. Key Value Pairs, separated by comma. Eg. consumer=consumer@xyz.com, _department_type=forecasting',
        'dataset.jobRetryPolicy': 'Job retry policy',
        'dataset.jobRetryDelay': 'Job retry delay',
        'dataset.jobRetryAttempts': 'Job retry attempts',
        'dataset.jobRetryOnTimeout': 'Job retry on timeout',
        'dataset.jobAclOwner': 'ACL owner',
        'dataset.jobAclGroup': 'ACL group',
        'dataset.jobAclPermission': 'ACL permission',
        'dataset.sourceDir': 'Multiple hdfs comma separated source directories',
        'dataset.sourceCluster': 'Source cluster',
        'dataset.targetDir': 'Target hdfs directory',
        'dataset.targetCluster': 'Source cluster',
        'dataset.distcpMaxMaps': 'Maximum number of mappers for DistCP',
        'dataset.distcpMapBandwidth': 'Bandwidth in MB for each mapper in DistCP',
        'dataset.maxEvents': 'To ceil the max events processed each time the job runs. Set it to max value depending on your bandwidth limit. Setting it to -1 will process all the events but can hog up the bandwidth. Use it judiciously!',
        'dataset.replicationMaxMaps': 'Maximum number of mappers to use for hive replication',
        'dataset.jobNotificationReceivers': 'Email Notification for Falcon instance completion.',
        'dataset.tdeEncryptionEnabled': 'Set this flag to true if TDE encryption is enabled on source and target',
        'dataset.sourceStagingPath': 'Staging path on source',
        'dataset.targetStagingPath': 'Staging path on target',
        'dataset.sourceHiveServer2Uri': 'Hive2 server end point. Eg. hive2://localhost:10000',
        'dataset.targetHiveServer2Uri': 'Hive2 server end point. Eg.hive2://localhost:10000',
        'dataset.sourceDatabases': 'For DB level replication specify multiple comma separated databases to replicate',
        'dataset.sourceTables': 'For table level replication specify multiple comma separated tables to replicate',
        'dataset.sourceHive2KerberosPrincipal': 'Kerberos principal required to access hive servers. Eg. hive/_HOST@EXAMPLE.COM',
        'dataset.targetHive2KerberosPrincipal': 'Kerberos principal required to access hive servers. Eg. hive/_HOST@EXAMPLE.COM',

        'dataset.snapshot.sourceSnapshotDir' : 'Snapshot-able source directory which should be replicated',
        'dataset.snapshot.sourceSnapshotRetentionAgeLimit' : 'Snapshots on source older than this age limit will be eligible for deletion.',
        'dataset.snapshot.sourceSnapshotRetentionNumber' : 'These many latest snapshots on source will be retained, the rest of them eligible for deletion.',
        'dataset.snapshot.targetSnapshotDir' : 'Snapshot-able target directory to which source should be replicated',
        'dataset.snapshot.targetSnapshotRetentionAgeLimit' : 'Snapshots on target older than this age limit will be eligible for deletion.',
        'dataset.snapshot.targetSnapshotRetentionNumber' : 'These many latest snapshots on target will be retained, the rest of them eligible for deletion.'
    };

    return {
      messages: messages
    };

  }]);
}());
