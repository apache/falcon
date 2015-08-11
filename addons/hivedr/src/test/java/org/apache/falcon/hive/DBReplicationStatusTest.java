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

import org.apache.falcon.hive.exception.HiveReplicationException;
import org.apache.falcon.hive.util.DBReplicationStatus;
import org.apache.falcon.hive.util.ReplicationStatus;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for DBReplicationStatus.
 */
@Test
public class DBReplicationStatusTest {

    private Map<String, ReplicationStatus> tableStatuses = new HashMap<String, ReplicationStatus>();
    private ReplicationStatus dbReplicationStatus;
    private ReplicationStatus tableStatus1;

    public DBReplicationStatusTest() {
    }

    @BeforeClass
    public void prepare() throws Exception {
        dbReplicationStatus = new ReplicationStatus("source", "target", "jobname",
                "Default1", null, ReplicationStatus.Status.FAILURE, 20L);
        tableStatus1 = new ReplicationStatus("source", "target", "jobname",
                "default1", "Table1", ReplicationStatus.Status.SUCCESS, 20L);
        tableStatuses.put("Table1", tableStatus1);

    }

    public void dBReplicationStatusSerializeTest() throws Exception {
        DBReplicationStatus replicationStatus = new DBReplicationStatus(dbReplicationStatus, tableStatuses);

        String expected = "{\n" + "    \"db_status\": {\n"
                + "        \"sourceUri\": \"source\",\n" + "        \"targetUri\": \"target\",\n"
                + "        \"jobName\": \"jobname\",\n" + "        \"database\": \"default1\",\n"
                + "        \"status\": \"FAILURE\",\n" + "        \"eventId\": 20\n" + "    },\n"
                + "    \"table_status\": {\"table1\": {\n" + "        \"sourceUri\": \"source\",\n"
                + "        \"targetUri\": \"target\",\n" + "        \"jobName\": \"jobname\",\n"
                + "        \"database\": \"default1\",\n" + "        \"table\": \"table1\",\n"
                + "        \"status\": \"SUCCESS\",\n" + "        \"eventId\": 20\n" + "    }}\n" + "}";
        String actual = replicationStatus.toJsonString();
        Assert.assertEquals(actual, expected);
    }

    public void dBReplicationStatusDeserializeTest() throws Exception {

        String jsonString = "{\"db_status\":{\"sourceUri\":\"source\","
                + "\"targetUri\":\"target\",\"jobName\":\"jobname\",\"database\":\"default1\",\"status\":\"SUCCESS\","
                + "\"eventId\":20},\"table_status\":{\"Table1\":{\"sourceUri\":\"source\",\"targetUri\":\"target\","
                + "\"jobName\":\"jobname\",\"database\":\"default1\",\"table\":\"Table1\",\"status\":\"SUCCESS\","
                + "\"eventId\":20},\"table3\":{\"sourceUri\":\"source\",\"targetUri\":\"target\","
                + "\"jobName\":\"jobname\", \"database\":\"Default1\",\"table\":\"table3\",\"status\":\"FAILURE\","
                + "\"eventId\":10}, \"table2\":{\"sourceUri\":\"source\",\"targetUri\":\"target\","
                + "\"jobName\":\"jobname\", \"database\":\"default1\",\"table\":\"table2\",\"status\":\"INIT\"}}}";

        DBReplicationStatus dbStatus = new DBReplicationStatus(jsonString);
        Assert.assertEquals(dbStatus.getDatabaseStatus().getDatabase(), "default1");
        Assert.assertEquals(dbStatus.getDatabaseStatus().getJobName(), "jobname");
        Assert.assertEquals(dbStatus.getDatabaseStatus().getEventId(), 20);

        Assert.assertEquals(dbStatus.getTableStatuses().get("table1").getEventId(), 20);
        Assert.assertEquals(dbStatus.getTableStatuses().get("table1").getStatus(), ReplicationStatus.Status.SUCCESS);
        Assert.assertEquals(dbStatus.getTableStatuses().get("table2").getEventId(), -1);
        Assert.assertEquals(dbStatus.getTableStatuses().get("table2").getStatus(), ReplicationStatus.Status.INIT);
        Assert.assertEquals(dbStatus.getTableStatuses().get("table3").getEventId(), 10);
        Assert.assertEquals(dbStatus.getTableStatuses().get("table3").getStatus(), ReplicationStatus.Status.FAILURE);


    }

    public void wrongDBForTableTest() throws Exception {

        ReplicationStatus newDbStatus = new ReplicationStatus("source", "target", "jobname",
                "wrongDb", null, ReplicationStatus.Status.FAILURE, 20L);
        new DBReplicationStatus(newDbStatus);

        try {
            new DBReplicationStatus(newDbStatus, tableStatuses);
            Assert.fail();
        } catch (HiveReplicationException e) {
            Assert.assertEquals(e.getMessage(),
                    "Cannot set status for table default1.table1, It does not belong to DB wrongdb");
        }

        String jsonString = "{\n" + "    \"db_status\": {\n"
                + "        \"sourceUri\": \"source\",\n" + "        \"targetUri\": \"target\",\n"
                + "        \"jobName\": \"jobname\",\n" + "        \"database\": \"wrongdb\",\n"
                + "        \"status\": \"FAILURE\",\n" + "        \"eventId\": 20\n" + "    },\n"
                + "    \"table_status\": {\"table1\": {\n" + "        \"sourceUri\": \"source\",\n"
                + "        \"targetUri\": \"target\",\n" + "        \"jobName\": \"jobname\",\n"
                + "        \"database\": \"default1\",\n" + "        \"table\": \"table1\",\n"
                + "        \"status\": \"SUCCESS\",\n" + "        \"eventId\": 20\n" + "    }}\n" + "}";

        try {
            new DBReplicationStatus(jsonString);
            Assert.fail();
        } catch (HiveReplicationException e) {
            Assert.assertEquals(e.getMessage(),
                    "Unable to create DBReplicationStatus from JsonString. Cannot set status for "
                            + "table default1.table1, It does not belong to DB wrongdb");
        }
    }

    public void updateTableStatusTest() throws Exception {
        DBReplicationStatus replicationStatus = new DBReplicationStatus(dbReplicationStatus, tableStatuses);
        replicationStatus.updateTableStatus(tableStatus1);

        // wrong DB test
        try {
            replicationStatus.updateTableStatus(new ReplicationStatus("source", "target", "jobname",
                    "wrongDB", "table2", ReplicationStatus.Status.INIT, -1L));
            Assert.fail();
        } catch (HiveReplicationException e) {
            Assert.assertEquals(e.getMessage(),
                    "Cannot update Table Status. TableDB wrongdb does not match current DB default1");
        }

        // wrong status test
        try {
            replicationStatus.updateTableStatus(dbReplicationStatus);
            Assert.fail();
        } catch (HiveReplicationException e) {
            Assert.assertEquals(e.getMessage(),
                    "Cannot update Table Status. Table name is empty.");
        }

    }

    public void updateDBStatusTest() throws Exception {
        DBReplicationStatus replicationStatus = new DBReplicationStatus(dbReplicationStatus, tableStatuses);
        replicationStatus.updateDbStatus(dbReplicationStatus);

        // wrong DB test
        try {
            replicationStatus.updateDbStatus(new ReplicationStatus("source", "target", "jobname",
                    "wrongDB", null, ReplicationStatus.Status.INIT, -1L));
            Assert.fail();
        } catch (HiveReplicationException e) {
            Assert.assertEquals(e.getMessage(),
                    "Cannot update Database Status. StatusDB wrongdb does not match current DB default1");
        }

        // wrong status test
        try {
            replicationStatus.updateDbStatus(tableStatus1);
            Assert.fail();
        } catch (HiveReplicationException e) {
            Assert.assertEquals(e.getMessage(),
                    "Cannot update DB Status. This is table level status.");
        }
    }

    public void updateDbStatusFromTableStatusesTest() throws Exception {

        ReplicationStatus dbStatus = new ReplicationStatus("source", "target", "jobname",
                "default1", null, ReplicationStatus.Status.SUCCESS, 20L);
        ReplicationStatus table1 = new ReplicationStatus("source", "target", "jobname",
                "default1", "table1", ReplicationStatus.Status.SUCCESS, 20L);
        ReplicationStatus table2 = new ReplicationStatus("source", "target", "jobname",
                "Default1", "table2", ReplicationStatus.Status.INIT, -1L);
        ReplicationStatus table3 = new ReplicationStatus("source", "target", "jobname",
                "default1", "Table3", ReplicationStatus.Status.FAILURE, 15L);
        ReplicationStatus table4 = new ReplicationStatus("source", "target", "jobname",
                "Default1", "Table4", ReplicationStatus.Status.FAILURE, 18L);
        Map<String, ReplicationStatus> tables = new HashMap<String, ReplicationStatus>();

        tables.put("table1", table1);
        tables.put("table2", table2);
        tables.put("table3", table3);
        tables.put("table4", table4);

        // If there is a failue, last eventId should be lowest eventId of failed tables
        DBReplicationStatus status = new DBReplicationStatus(dbStatus, tables);
        Assert.assertEquals(status.getDatabaseStatus().getEventId(), 20);
        Assert.assertEquals(status.getDatabaseStatus().getStatus(), ReplicationStatus.Status.SUCCESS);
        status.updateDbStatusFromTableStatuses();
        Assert.assertEquals(status.getDatabaseStatus().getEventId(), 15);
        Assert.assertEquals(status.getDatabaseStatus().getStatus(), ReplicationStatus.Status.FAILURE);

        // If all tables succeed, last eventId should be highest eventId of success tables
        table3 = new ReplicationStatus("source", "target", "jobname",
                "default1", "table3", ReplicationStatus.Status.SUCCESS, 25L);
        table4 = new ReplicationStatus("source", "target", "jobname",
                "default1", "table4", ReplicationStatus.Status.SUCCESS, 22L);
        tables.put("Table3", table3);
        tables.put("Table4", table4);
        status = new DBReplicationStatus(dbStatus, tables);
        status.updateDbStatusFromTableStatuses();
        Assert.assertEquals(status.getDatabaseStatus().getEventId(), 25);
        Assert.assertEquals(status.getDatabaseStatus().getStatus(), ReplicationStatus.Status.SUCCESS);

        // Init tables should not change DB status.
        Map<String, ReplicationStatus> initOnlyTables = new HashMap<String, ReplicationStatus>();
        initOnlyTables.put("table2", table2);
        dbStatus = new ReplicationStatus("source", "target", "jobname",
                "default1", null, ReplicationStatus.Status.SUCCESS, 20L);
        status = new DBReplicationStatus(dbStatus, initOnlyTables);
        Assert.assertEquals(status.getDatabaseStatus().getEventId(), 20);
        Assert.assertEquals(status.getDatabaseStatus().getStatus(), ReplicationStatus.Status.SUCCESS);
        status.updateDbStatusFromTableStatuses();
        Assert.assertEquals(status.getDatabaseStatus().getEventId(), 20);
        Assert.assertEquals(status.getDatabaseStatus().getStatus(), ReplicationStatus.Status.SUCCESS);


    }

}
