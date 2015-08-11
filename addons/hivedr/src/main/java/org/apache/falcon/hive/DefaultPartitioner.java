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

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.hive.util.DRStatusStore;
import org.apache.falcon.hive.util.EventSourcerUtils;
import org.apache.falcon.hive.util.HiveDRUtils;
import org.apache.falcon.hive.util.ReplicationStatus;
import org.apache.hive.hcatalog.api.repl.Command;
import org.apache.hive.hcatalog.api.repl.ReplicationTask;
import org.apache.hive.hcatalog.api.repl.StagingDirectoryProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.hive.hcatalog.api.HCatNotificationEvent.Scope;

/**
 * Partitioner for partitioning events for a given DB.
 */
public class DefaultPartitioner implements Partitioner {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultPartitioner.class);
    private EventFilter eventFilter;
    private final DRStatusStore drStore;
    private final EventSourcerUtils eventSourcerUtils;

    private enum CMDTYPE {
        SRC_CMD_TYPE,
        TGT_CMD_TYPE
    }

    public DefaultPartitioner(DRStatusStore drStore, EventSourcerUtils eventSourcerUtils) {
        this.drStore = drStore;
        this.eventSourcerUtils = eventSourcerUtils;
    }

    private class EventFilter {
        private final Map<String, Long> eventFilterMap;

        public EventFilter(String sourceMetastoreUri, String targetMetastoreUri, String jobName,
                           String database) throws Exception {
            eventFilterMap = new HashMap<>();
            Iterator<ReplicationStatus> replStatusIter = drStore.getTableReplicationStatusesInDb(sourceMetastoreUri,
                    targetMetastoreUri, jobName, database);
            while (replStatusIter.hasNext()) {
                ReplicationStatus replStatus = replStatusIter.next();
                eventFilterMap.put(replStatus.getTable(), replStatus.getEventId());
            }
        }
    }

    public ReplicationEventMetadata partition(final HiveDROptions drOptions, final String databaseName,
                                              final Iterator<ReplicationTask> taskIter) throws Exception {
        long lastCounter = 0;
        String dbName = databaseName.toLowerCase();
        // init filtering before partitioning
        this.eventFilter = new EventFilter(drOptions.getSourceMetastoreUri(), drOptions.getTargetMetastoreUri(),
                drOptions.getJobName(), dbName);
        String srcStagingDirProvider = drOptions.getSourceStagingPath();
        String dstStagingDirProvider = drOptions.getTargetStagingPath();

        List<Command> dbSrcEventList = Lists.newArrayList();
        List<Command> dbTgtEventList = Lists.newArrayList();

        Map<String, List<String>> eventMetaFileMap =
                new HashMap<>();
        Map<String, List<OutputStream>> outputStreamMap =
                new HashMap<>();


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
                Scope eventScope = task.getEvent().getEventScope();
                String tableName = task.getEvent().getTableName();
                if (StringUtils.isNotEmpty(tableName)) {
                    tableName = tableName.toLowerCase();
                }

                boolean firstEventForTable = (eventScope == Scope.TABLE)
                        && isFirstEventForTable(eventMetaFileMap, tableName);
                if (firstEventForTable && (task.getSrcWhCommands() != null || task.getDstWhCommands() != null)) {
                    ++lastCounter;
                }
                Iterable<? extends org.apache.hive.hcatalog.api.repl.Command> srcCmds = task.getSrcWhCommands();
                if (srcCmds != null) {
                    if (eventScope == Scope.DB) {
                        processDBScopeCommands(dbSrcEventList, srcCmds, outputStreamMap, CMDTYPE.SRC_CMD_TYPE);
                    } else if (eventScope == Scope.TABLE) {
                        OutputStream srcOut;
                        if (firstEventForTable) {
                            srcFilename = eventSourcerUtils.getSrcFileName(String.valueOf(lastCounter)).toString();
                            srcOutputStream = eventSourcerUtils.getFileOutputStream(srcFilename);
                            srcOut = srcOutputStream;
                        } else {
                            srcOut = outputStreamMap.get(tableName).get(0);
                        }
                        processTableScopeCommands(srcCmds, eventMetaFileMap, tableName, dbSrcEventList, srcOut);
                    } else {
                        throw new Exception("Event scope is not DB or Table");
                    }
                }


                Iterable<? extends org.apache.hive.hcatalog.api.repl.Command> dstCmds = task.getDstWhCommands();
                if (dstCmds != null) {
                    if (eventScope == Scope.DB) {
                        processDBScopeCommands(dbTgtEventList, dstCmds, outputStreamMap, CMDTYPE.TGT_CMD_TYPE);
                    } else if (eventScope == Scope.TABLE) {
                        OutputStream tgtOut;
                        if (firstEventForTable) {
                            tgtFilename = eventSourcerUtils.getTargetFileName(String.valueOf(lastCounter)).toString();
                            tgtOutputStream = eventSourcerUtils.getFileOutputStream(tgtFilename);
                            tgtOut = tgtOutputStream;
                        } else {
                            tgtOut = outputStreamMap.get(tableName).get(1);
                        }
                        processTableScopeCommands(dstCmds, eventMetaFileMap, tableName, dbTgtEventList, tgtOut);
                    } else {
                        throw new Exception("Event scope is not DB or Table");
                    }
                }

                // If first table event, update the state data at the end
                if (firstEventForTable) {
                    updateStateDataIfFirstTableEvent(tableName, srcFilename, tgtFilename, srcOutputStream,
                            tgtOutputStream, eventMetaFileMap, outputStreamMap);
                }
            } else {
                LOG.error("Task is not actionable with event Id : {}", task.getEvent().getEventId());
            }
        }

        ReplicationEventMetadata eventMetadata = new ReplicationEventMetadata();
        // If there were only DB events for this run
        if (eventMetaFileMap.isEmpty()) {
            ++lastCounter;
            if (!dbSrcEventList.isEmpty()) {
                srcFilename = eventSourcerUtils.getSrcFileName(String.valueOf(lastCounter)).toString();
                srcOutputStream = eventSourcerUtils.getFileOutputStream(srcFilename);
                eventSourcerUtils.persistReplicationEvents(srcOutputStream, dbSrcEventList);
            }

            if (!dbTgtEventList.isEmpty()) {
                tgtFilename = eventSourcerUtils.getTargetFileName(String.valueOf(lastCounter)).toString();
                tgtOutputStream = eventSourcerUtils.getFileOutputStream(tgtFilename);
                eventSourcerUtils.persistReplicationEvents(tgtOutputStream, dbTgtEventList);
            }

            // Close the stream
            eventSourcerUtils.closeOutputStream(srcOutputStream);
            eventSourcerUtils.closeOutputStream(tgtOutputStream);
            EventSourcerUtils.updateEventMetadata(eventMetadata, dbName, null, srcFilename, tgtFilename);
        } else {
            closeAllStreams(outputStreamMap);
            for (Map.Entry<String, List<String>> entry : eventMetaFileMap.entrySet()) {
                String srcFile = null;
                String tgtFile = null;
                if (entry.getValue() != null) {
                    srcFile = entry.getValue().get(0);
                    tgtFile = entry.getValue().get(1);
                }
                EventSourcerUtils.updateEventMetadata(eventMetadata, dbName, entry.getKey(), srcFile, tgtFile);
            }
        }

        return eventMetadata;
    }

    private void updateStateDataIfFirstTableEvent(final String tableName, final String srcFilename,
                                                  final String tgtFilename,
                                                  final OutputStream srcOutputStream,
                                                  final OutputStream tgtOutputStream,
                                                  Map<String, List<String>> eventMetaFileMap,
                                                  Map<String, List<OutputStream>> outputStreamMap) {
        List<String> files = Arrays.asList(srcFilename, tgtFilename);
        eventMetaFileMap.put(tableName, files);

        List<OutputStream> streams = Arrays.asList(srcOutputStream, tgtOutputStream);
        outputStreamMap.put(tableName, streams);
    }

    private void closeAllStreams(final Map<String, List<OutputStream>> outputStreamMap) throws Exception {
        if (outputStreamMap == null || outputStreamMap.isEmpty()) {
            return;
        }

        for (Map.Entry<String, List<OutputStream>> entry : outputStreamMap.entrySet()) {
            List<OutputStream> streams = entry.getValue();

            for (OutputStream out : streams) {
                if (out != null) {
                    eventSourcerUtils.closeOutputStream(out);
                }
            }
        }
    }

    private void processDBScopeCommands(final List<Command> dbEventList, final Iterable<? extends org.apache.hive
            .hcatalog.api.repl.Command> cmds, final Map<String, List<OutputStream>> outputStreamMap, CMDTYPE cmdType
    ) throws Exception {
        addCmdsToDBEventList(dbEventList, cmds);

        /* add DB event to all tables */
        if (!outputStreamMap.isEmpty()) {
            addDbEventToAllTablesEventFile(cmds, outputStreamMap, cmdType);
        }
    }

    private void processTableScopeCommands(final Iterable<? extends org.apache.hive.hcatalog.api.repl.Command> cmds,
                                           final Map<String,
                                                   List<String>> eventMetaFileMap, String tableName,
                                           final List<Command> dbEventList, final OutputStream out) throws Exception {
        // First event for this table
        // Before adding this event, add all the DB events
        if (isFirstEventForTable(eventMetaFileMap, tableName)) {
            addDbEventsToTableEventFile(out, dbEventList, tableName);
        }
        addTableEventToFile(out, cmds, tableName);
    }

    private boolean isFirstEventForTable(final Map<String,
            List<String>> eventMetaFileMap, final String tableName) {
        List<String> files = eventMetaFileMap.get(tableName);
        return (files == null || files.isEmpty());
    }

    private void addCmdsToDBEventList(List<Command> dbEventList, final java.lang.Iterable
            <? extends org.apache.hive.hcatalog.api.repl.Command> cmds) {
        for (Command cmd : cmds) {
            dbEventList.add(cmd);
        }
    }

    private void addDbEventToAllTablesEventFile(
            final java.lang.Iterable<? extends org.apache.hive.hcatalog.api.repl.Command> cmds,
            final Map<String, List<OutputStream>> outputStreamMap, final CMDTYPE cmdType) throws Exception {
        for (Map.Entry<String, List<OutputStream>> entry : outputStreamMap.entrySet()) {
            String tableName = entry.getKey();
            List<OutputStream> streams = entry.getValue();
            OutputStream out;
            if (CMDTYPE.SRC_CMD_TYPE == cmdType) {
                out = streams.get(0);
            } else {
                out = streams.get(1);
            }
            addTableEventToFile(out, cmds, tableName);
        }
    }

    private void addDbEventsToTableEventFile(final OutputStream out, final List<Command> dbEventList,
                                             final String tableName) throws Exception {
        /* First event for the table, add db events before adding this event */
        addTableEventToFile(out, dbEventList, tableName);
    }

    private void addTableEventToFile(final OutputStream out,
                                     final java.lang.Iterable<? extends org.apache.hive.hcatalog.api.repl.Command> cmds,
                                     final String tableName) throws Exception {
        Long eventId = eventFilter.eventFilterMap.get(tableName);
        /* If not already processed, add it */
        for (Command cmd : cmds) {
            persistEvent(out, eventId, cmd);
        }
    }

    private void persistEvent(final OutputStream out, final Long eventId, final Command cmd) throws Exception {
        if (out == null) {
            LOG.debug("persistEvent : out is null");
            return;
        }
        if (eventId == null || cmd.getEventId() > eventId) {
            eventSourcerUtils.persistReplicationEvents(out, cmd);
        }
    }

    public boolean isPartitioningRequired(final HiveDROptions options) {
        return (HiveDRUtils.getReplicationType(options.getSourceTables()) == HiveDRUtils.ReplicationType.DB);
    }
}
