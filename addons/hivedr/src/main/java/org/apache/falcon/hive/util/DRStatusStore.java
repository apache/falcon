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

package org.apache.falcon.hive.util;

import org.apache.falcon.hive.exception.HiveReplicationException;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

import java.util.Iterator;
import java.util.List;

/**
 * Abstract class for Data Replication Status Store.
 */
public abstract class DRStatusStore {

    public static final String BASE_DEFAULT_STORE_PATH = "/apps/falcon/extensions/mirroring";
    public static final FsPermission DEFAULT_STORE_PERMISSION =
            new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.NONE);

    private static String storeGroup = "users";


    /**
     * Update replication status of a table(s)/db after replication job jobName completes.
     * @param jobName Name of the replication job.
     * @param statusList List of replication statuses of db/tables replicated by jobName.
     */
    public abstract void updateReplicationStatus(String jobName, List<ReplicationStatus> statusList)
        throws HiveReplicationException;

    /**
     * Get Replication status for a database.
     * @param source Replication source uri.
     * @param target Replication target uri.
     * @param jobName Name of the replication job.
     * @param database Name of the target database.
     * @return ReplicationStatus
     * destination commands for each table
     */
    public abstract ReplicationStatus getReplicationStatus(String source, String target,
                                                           String jobName, String database)
        throws HiveReplicationException;

    /**
     * Get Replication status for a table.
     * @param source Replication source uri.
     * @param target Replication target uri.
     * @param jobName Name of the replication job.
     * @param database Name of the target database.
     * @param table Name of the target table.
     * @return ReplicationStatus
     * destination commands for each table
     */
    public abstract ReplicationStatus getReplicationStatus(String source, String target,
                                                           String jobName, String database,
                                                           String table) throws HiveReplicationException;

    /**
     * Get Replication status of all tables in a database.
     * @param source Replication source uri.
     * @param target Replication target uri.
     * @param jobName Name of the replication job.
     * @param database Name of the target database.
     * @return Iterator
     * destination commands for each table
     */
    public abstract Iterator<ReplicationStatus> getTableReplicationStatusesInDb(String source, String target,
                                                                                String jobName, String database)
        throws HiveReplicationException;


    /**
     * Delete a replication job.
     * @param jobName Name of the replication job.
     * @param database Name of the target database.
     * destination commands for each table
     */
    public abstract void deleteReplicationStatus(String jobName, String database) throws HiveReplicationException;

    public static String getStoreGroup() {
        return storeGroup;
    }

    public static void setStoreGroup(String group) {
        storeGroup = group;
    }
}
