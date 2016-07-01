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

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.hive.exception.HiveReplicationException;
import org.apache.falcon.hive.util.DRStatusStore;
import org.apache.falcon.hive.util.DelimiterUtils;
import org.apache.falcon.hive.util.FileUtils;
import org.apache.falcon.hive.util.HiveDRUtils;
import org.apache.falcon.hive.util.HiveMetastoreUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.api.HCatTable;
import org.apache.hive.hcatalog.api.repl.ReplicationUtils;
import org.apache.hive.hcatalog.common.HCatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Sources meta store change events from Hive.
 */
public class LastReplicatedEvents {
    private static final Logger LOG = LoggerFactory.getLogger(LastReplicatedEvents.class);
    private final HCatClient targetMetastoreClient;
    private final DRStatusStore drStore;
    private final FileSystem jobFS;
    private Path eventsInputDirPath;

    /* TODO handle cases when no events. files will be empty and lists will be empty */
    public LastReplicatedEvents(Configuration conf, String targetMetastoreUri,
                                String targetMetastoreKerberosPrincipal,
                                String targetHive2KerberosPrincipal,
                                DRStatusStore drStore, HiveDROptions inputOptions) throws Exception {
        targetMetastoreClient = HiveMetastoreUtils.initializeHiveMetaStoreClient(targetMetastoreUri,
                targetMetastoreKerberosPrincipal, targetHive2KerberosPrincipal);
        jobFS = FileSystem.get(conf);
        this.drStore = drStore;
        init(inputOptions.getJobName());
    }

    private void init(final String jobName) throws Exception {
        // Create base dir to store events on cluster where job is running
        Path dir = new Path(FileUtils.DEFAULT_EVENT_STORE_PATH);
        // Validate base path
        FileUtils.validatePath(jobFS, new Path(DRStatusStore.BASE_DEFAULT_STORE_PATH));

        if (!jobFS.exists(dir)) {
            if (!FileSystem.mkdirs(jobFS, dir, FileUtils.DEFAULT_DIR_PERMISSION)) {
                throw new IOException("Creating directory failed: " + dir);
            }
        }

        eventsInputDirPath = new Path(FileUtils.DEFAULT_EVENT_STORE_PATH, jobName);

        if (!jobFS.exists(eventsInputDirPath)) {
            if (!jobFS.mkdirs(eventsInputDirPath)) {
                throw new Exception("Creating directory failed: " + eventsInputDirPath);
            }
        }
    }

    public String getLastEvents(HiveDROptions inputOptions) throws Exception {
        HiveDRUtils.ReplicationType replicationType = HiveDRUtils.getReplicationType(inputOptions.getSourceTables());
        LOG.info("Obtaining last events for replicationType : {}", replicationType);
        HashMap<String, Long> lastEvents = new HashMap<String, Long>();
        if (replicationType == HiveDRUtils.ReplicationType.DB) {
            List<String> dbNames = inputOptions.getSourceDatabases();
            for (String db : dbNames) {
                lastEvents.put(db, getLastSavedEventId(inputOptions, db, null));
            }
        } else {
            List<String> tableNames = inputOptions.getSourceTables();
            String db = inputOptions.getSourceDatabases().get(0);
            for (String tableName : tableNames) {
                lastEvents.put(db + "." + tableName, getLastSavedEventId(inputOptions, db, tableName));
            }
        }

        return persistLastEventsToFile(lastEvents, inputOptions.getJobName());
    }

    private long getLastSavedEventId(HiveDROptions inputOptions, final String dbName,
                                     final String tableName) throws Exception {
        HiveDRUtils.ReplicationType replicationType = HiveDRUtils.getReplicationType(inputOptions.getSourceTables());
        String jobName = inputOptions.getJobName();
        String sourceMetastoreUri = inputOptions.getSourceMetastoreUri();
        String targetMetastoreUri = inputOptions.getTargetMetastoreUri();

        long eventId = 0;
        if (HiveDRUtils.ReplicationType.DB == replicationType) {
            eventId = drStore.getReplicationStatus(sourceMetastoreUri, targetMetastoreUri,
                    jobName, dbName).getEventId();
        } else if (HiveDRUtils.ReplicationType.TABLE == replicationType) {
            eventId = drStore.getReplicationStatus(sourceMetastoreUri, targetMetastoreUri,
                    jobName, dbName, tableName).getEventId();
        }

        if (eventId == -1) {
            if (HiveDRUtils.ReplicationType.DB == replicationType) {
                /*
                 * API to get last repl ID for a DB is very expensive, so Hive does not want to make it public.
                 * HiveDrTool finds last repl id for DB by finding min last repl id of all tables.
                 */
                eventId = getLastReplicationIdForDatabase(dbName);
            } else {
                HCatTable table = targetMetastoreClient.getTable(dbName, tableName);
                eventId = ReplicationUtils.getLastReplicationId(table);
            }
        }

        if ((StringUtils.isEmpty(tableName))) {
            LOG.info("Last replicated eventId for DB : {} is {}", dbName, eventId);
        } else {
            LOG.info("Last replicated eventId for DB : {} Table : {} is {}", dbName, tableName, eventId);
        }

        return eventId;
    }

    private long getLastReplicationIdForDatabase(String databaseName) throws HiveReplicationException {
        /*
         * This is a very expensive method and should only be called during first dbReplication instance.
         */
        long eventId = Long.MAX_VALUE;
        try {
            List<String> tableList = targetMetastoreClient.listTableNamesByPattern(databaseName, "*");
            for (String tableName : tableList) {
                long temp = ReplicationUtils.getLastReplicationId(
                        targetMetastoreClient.getTable(databaseName, tableName));
                if (temp < eventId) {
                    eventId = temp;
                }
            }
            return (eventId == Long.MAX_VALUE) ? 0 : eventId;
        } catch (HCatException e) {
            throw new HiveReplicationException("Unable to find last replication id for database "
                    + databaseName, e);
        }
    }

    public String persistLastEventsToFile(final HashMap<String, Long> lastEvents,
                                          final String identifier) throws IOException {
        if (lastEvents.size()!=0) {
            Path eventsFile = new Path(eventsInputDirPath.toString(), identifier+".id");
            OutputStream out = null;

            try {
                out = FileSystem.create(jobFS, eventsFile, FileUtils.FS_PERMISSION_700);
                for (Map.Entry<String, Long> entry : lastEvents.entrySet()) {
                    out.write(entry.getKey().getBytes());
                    out.write(DelimiterUtils.TAB_DELIM.getBytes());
                    out.write(String.valueOf(entry.getValue()).getBytes());
                    out.write(DelimiterUtils.NEWLINE_DELIM.getBytes());
                }
                out.flush();
            } finally {
                IOUtils.closeQuietly(out);
            }
            return jobFS.makeQualified(eventsFile).toString();
        } else {
            return null;
        }
    }

    public void cleanUp() throws Exception {
        if (targetMetastoreClient != null) {
            targetMetastoreClient.close();
        }
    }
}
