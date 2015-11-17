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

package org.apache.falcon.hive;

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.hive.util.EventSourcerUtils;
import org.apache.falcon.hive.util.HiveDRUtils;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.api.repl.ReplicationTask;
import org.apache.hive.hcatalog.api.repl.StagingDirectoryProvider;
import org.apache.hive.hcatalog.common.HCatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Sources meta store change events from Hive.
 */
public class MetaStoreEventSourcer implements EventSourcer {

    private static final Logger LOG = LoggerFactory.getLogger(MetaStoreEventSourcer.class);

    private final HCatClient sourceMetastoreClient;
    private final Partitioner partitioner;
    private final EventSourcerUtils eventSourcerUtils;
    private final ReplicationEventMetadata eventMetadata;
    private Map<String, Long> lastEventsIdMap = null;
    private long lastCounter;

    /* TODO handle cases when no events. files will be empty and lists will be empty */
    public MetaStoreEventSourcer(HCatClient sourceMetastoreClient, Partitioner partitioner,
                                 EventSourcerUtils eventSourcerUtils,
                                 Map<String, Long> lastEventsIdMap) throws Exception {
        this.sourceMetastoreClient = sourceMetastoreClient;
        this.eventMetadata = new ReplicationEventMetadata();
        this.partitioner = partitioner;
        this.eventSourcerUtils = eventSourcerUtils;
        this.lastEventsIdMap = lastEventsIdMap;
    }

    public String sourceEvents(HiveDROptions inputOptions) throws Exception {
        HiveDRUtils.ReplicationType replicationType = HiveDRUtils.getReplicationType(inputOptions.getSourceTables());
        LOG.info("Sourcing replication events for type : {}", replicationType);
        if (replicationType == HiveDRUtils.ReplicationType.DB) {
            List<String> dbNames = inputOptions.getSourceDatabases();
            for (String db : dbNames) {
                ++lastCounter;
                sourceEventsForDb(inputOptions, db);
            }
        } else {
            List<String> tableNames = inputOptions.getSourceTables();
            String db = inputOptions.getSourceDatabases().get(0);
            for (String tableName : tableNames) {
                ++lastCounter;
                sourceEventsForTable(inputOptions, db, tableName);
            }
        }

        if (eventMetadata.getEventFileMetadata() == null || eventMetadata.getEventFileMetadata().isEmpty()) {
            LOG.info("No events for tables for the request db: {} , Tables : {}", inputOptions.getSourceDatabases(),
                    inputOptions.getSourceTables());
            eventSourcerUtils.cleanUpEventInputDir();
            return null;
        } else {
            return eventSourcerUtils.persistToMetaFile(eventMetadata, inputOptions.getJobName());
        }
    }

    private void sourceEventsForDb(HiveDROptions inputOptions, String dbName) throws Exception {
        Iterator<ReplicationTask> replicationTaskIter = sourceReplicationEvents(getLastSavedEventId(dbName, null),
                inputOptions.getMaxEvents(), dbName, null);
        if (replicationTaskIter == null || !replicationTaskIter.hasNext()) {
            LOG.info("No events for db: {}", dbName);
            return;
        }
        processEvents(dbName, null, inputOptions, replicationTaskIter);
    }

    private void sourceEventsForTable(HiveDROptions inputOptions, String dbName, String tableName)
        throws Exception {
        Iterator<ReplicationTask> replicationTaskIter = sourceReplicationEvents(getLastSavedEventId(dbName, tableName),
                inputOptions.getMaxEvents(), dbName, tableName
        );
        if (replicationTaskIter == null || !replicationTaskIter.hasNext()) {
            LOG.info("No events for db.table: {}.{}", dbName, tableName);
            return;
        }
        processEvents(dbName, tableName, inputOptions, replicationTaskIter);
    }

    private void processEvents(String dbName, String tableName, HiveDROptions inputOptions,
                               Iterator<ReplicationTask> replicationTaskIter) throws Exception {
        if (partitioner.isPartitioningRequired(inputOptions)) {
            ReplicationEventMetadata dbEventMetadata = partitioner.partition(inputOptions, dbName, replicationTaskIter);

            if (dbEventMetadata == null || dbEventMetadata.getEventFileMetadata() == null
                    || dbEventMetadata.getEventFileMetadata().isEmpty()) {
                LOG.info("No events for db: {} , Table : {}", dbName, tableName);
            } else {
                EventSourcerUtils.updateEventMetadata(eventMetadata, dbEventMetadata);
            }
        } else {
            processTableReplicationEvents(replicationTaskIter, dbName, tableName,
                    inputOptions.getSourceStagingPath(), inputOptions.getTargetStagingPath());
        }
    }

    private long getLastSavedEventId(final String dbName, final String tableName) throws Exception {
        String key = dbName;
        if (StringUtils.isNotEmpty(tableName)) {
            key = dbName + "." + tableName;
        }
        long eventId = lastEventsIdMap.get(key);
        LOG.info("LastSavedEventId eventId for {} : {}", key, eventId);
        return eventId;
    }

    private Iterator<ReplicationTask> sourceReplicationEvents(long lastEventId, int maxEvents, String dbName,
                                                              String tableName) throws Exception {
        try {
            return sourceMetastoreClient.getReplicationTasks(lastEventId, maxEvents, dbName, tableName);
        } catch (HCatException e) {
            throw new Exception("Exception getting replication events " + e.getMessage(), e);
        }
    }


    protected void processTableReplicationEvents(Iterator<ReplicationTask> taskIter, String dbName,
                                               String tableName, String srcStagingDirProvider,
                                               String dstStagingDirProvider) throws Exception {
        String srcFilename = null;
        String tgtFilename = null;
        OutputStream srcOutputStream = null;
        OutputStream tgtOutputStream = null;

        while (taskIter.hasNext()) {
            ReplicationTask task = taskIter.next();
            if (task.needsStagingDirs()) {
                task.withSrcStagingDirProvider(new StagingDirectoryProvider.TrivialImpl(srcStagingDirProvider,
                        HiveDRUtils.SEPARATOR));
                task.withDstStagingDirProvider(new StagingDirectoryProvider.TrivialImpl(dstStagingDirProvider,
                        HiveDRUtils.SEPARATOR));
            }

            if (task.isActionable()) {
                Iterable<? extends org.apache.hive.hcatalog.api.repl.Command> srcCmds = task.getSrcWhCommands();
                if (srcCmds != null) {
                    if (StringUtils.isEmpty(srcFilename)) {
                        srcFilename = eventSourcerUtils.getSrcFileName(String.valueOf(lastCounter)).toString();
                        srcOutputStream = eventSourcerUtils.getFileOutputStream(srcFilename);
                    }
                    eventSourcerUtils.persistReplicationEvents(srcOutputStream, srcCmds);
                }


                Iterable<? extends org.apache.hive.hcatalog.api.repl.Command> dstCmds = task.getDstWhCommands();
                if (dstCmds != null) {
                    if (StringUtils.isEmpty(tgtFilename)) {
                        tgtFilename = eventSourcerUtils.getTargetFileName(String.valueOf(lastCounter)).toString();
                        tgtOutputStream = eventSourcerUtils.getFileOutputStream(tgtFilename);
                    }
                    eventSourcerUtils.persistReplicationEvents(tgtOutputStream, dstCmds);
                }

            } else {
                LOG.error("Task is not actionable with event Id : {}", task.getEvent().getEventId());
            }
        }
        // Close the stream
        eventSourcerUtils.closeOutputStream(srcOutputStream);
        eventSourcerUtils.closeOutputStream(tgtOutputStream);
        EventSourcerUtils.updateEventMetadata(eventMetadata, dbName, tableName, srcFilename, tgtFilename);
    }

    public String persistToMetaFile(String jobName) throws Exception {
        return eventSourcerUtils.persistToMetaFile(eventMetadata, jobName);
    }

    public void cleanUp() throws Exception {
        if (sourceMetastoreClient != null) {
            sourceMetastoreClient.close();
        }
    }
}
