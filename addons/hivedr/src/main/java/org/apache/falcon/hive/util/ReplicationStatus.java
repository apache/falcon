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

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.hive.exception.HiveReplicationException;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 * Object to store replication status of a DB or a table.
 */
public class ReplicationStatus {

    public static final int INDENT_FACTOR = 4;
    private static final String SOURCE = "sourceUri";
    private static final String TARGET = "targetUri";
    private static final String JOB_NAME = "jobName";
    private static final String DATABASE = "database";
    private static final String TABLE = "table";
    private static final String EVENT_ID = "eventId";
    private static final String STATUS_KEY = "status";
    private static final String STATUS_LOG = "statusLog";

    /**
     * Replication Status enum.
     */
    public static enum Status {
        INIT,
        SUCCESS,
        FAILURE
    }

    private String sourceUri;
    private String targetUri;
    private String jobName;
    private String database;
    private String table;
    private Status status = Status.SUCCESS;
    private long eventId = -1;
    private String log;

    //SUSPEND CHECKSTYLE CHECK ParameterNumberCheck
    public ReplicationStatus(String sourceUri, String targetUri, String jobName,
                             String database, String table,
                             ReplicationStatus.Status status, long eventId) throws HiveReplicationException {
        init(sourceUri, targetUri, jobName, database, table, status, eventId, null);
    }

    private void init(String source, String target, String job,
                      String dbName, String tableName, ReplicationStatus.Status replStatus,
                      long eventNum, String logStr) throws HiveReplicationException {
        setSourceUri(source);
        setTargetUri(target);
        setJobName(job);
        setDatabase(dbName);
        setTable(tableName);
        setStatus(replStatus);
        setEventId(eventNum);
        setLog(logStr);
    }
    //RESUME CHECKSTYLE CHECK ParameterNumberCheck

    public ReplicationStatus(String jsonString) throws HiveReplicationException {
        try {
            JSONObject object = new JSONObject(jsonString);
            Status objectStatus;
            try {
                objectStatus = ReplicationStatus.Status.valueOf(object.getString(STATUS_KEY).toUpperCase());
            } catch (IllegalArgumentException e1) {
                throw new HiveReplicationException("Unable to deserialize jsonString to ReplicationStatus."
                        + " Invalid status " + object.getString(STATUS_KEY), e1);
            }

            init(object.getString(SOURCE), object.getString(TARGET), object.getString(JOB_NAME),
                    object.getString(DATABASE), object.has(TABLE) ? object.getString(TABLE) : null,
                    objectStatus, object.has(EVENT_ID) ? object.getLong(EVENT_ID) : -1,
                    object.has(STATUS_LOG) ? object.getString(STATUS_LOG) : null);
        } catch (JSONException e) {
            throw new HiveReplicationException("Unable to deserialize jsonString to ReplicationStatus ", e);
        }

    }

    public String toJsonString() throws HiveReplicationException {
        try {
            return toJsonObject().toString(INDENT_FACTOR);
        } catch (JSONException e) {
            throw new HiveReplicationException("Unable to serialize ReplicationStatus ", e);
        }
    }

    public JSONObject toJsonObject() throws HiveReplicationException {
        JSONObject jsonObject = new JSONObject();
        try {
            jsonObject.put(SOURCE, this.sourceUri);
            jsonObject.put(TARGET, this.targetUri);
            jsonObject.put(JOB_NAME, this.jobName);
            jsonObject.put(DATABASE, this.database);
            if (StringUtils.isNotEmpty(this.table)) {
                jsonObject.put(TABLE, this.table);
            }
            jsonObject.put(STATUS_KEY, this.status.name());
            if (this.eventId > -1) {
                jsonObject.put(EVENT_ID, this.eventId);
            } else {
                jsonObject.put(EVENT_ID, -1);
            }
            if (StringUtils.isNotEmpty(this.log)) {
                jsonObject.put(STATUS_LOG, this.log);
            }
            return jsonObject;
        } catch (JSONException e) {
            throw new HiveReplicationException("Unable to serialize ReplicationStatus ", e);
        }
    }

    public String getSourceUri() {
        return this.sourceUri;
    }

    public void setSourceUri(String source) throws HiveReplicationException {
        validateString(SOURCE, source);
        this.sourceUri = source;
    }

    public String getTargetUri() {
        return this.targetUri;
    }

    public void setTargetUri(String target) throws HiveReplicationException {
        validateString(TARGET, target);
        this.targetUri = target;
    }

    public String getJobName() {
        return this.jobName;
    }

    public void setJobName(String jobName) throws HiveReplicationException {
        validateString(JOB_NAME, jobName);
        this.jobName = jobName;
    }

    public String getDatabase() {
        return this.database;
    }

    public void setDatabase(String database) throws HiveReplicationException {
        validateString(DATABASE, database);
        this.database = database.toLowerCase();
    }

    public String getTable() {
        return this.table;
    }

    public void setTable(String table) {
        this.table = (table == null) ? null : table.toLowerCase();
    }

    public Status getStatus() {
        return this.status;
    }

    public void setStatus(Status status) throws HiveReplicationException {
        if (status != null) {
            this.status = status;
        } else {
            throw new HiveReplicationException("Failed to set ReplicationStatus. Input \""
                    + STATUS_KEY + "\" cannot be empty");
        }
    }

    public long getEventId() {
        return this.eventId;
    }

    public void setEventId(long eventId) throws HiveReplicationException {
        if (eventId > -1) {
            this.eventId = eventId;
        }
    }

    public String getLog() {
        return this.log;
    }

    public void setLog(String log) {
        this.log = log;
    }

    private void validateString(String inputName, String input) throws HiveReplicationException {
        if (StringUtils.isEmpty(input)) {
            throw new HiveReplicationException("Failed to set ReplicationStatus. Input \""
                    + inputName + "\" cannot be empty");
        }
    }

    public String toString() {
        return sourceUri + "\t" + targetUri + "\t" + jobName + "\t"
                + database + "\t"+ table + "\t" + status + "\t"+ eventId;
    }

}
