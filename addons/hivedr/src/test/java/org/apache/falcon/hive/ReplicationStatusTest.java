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
import org.apache.falcon.hive.util.ReplicationStatus;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Unit tests for ReplicationStatus.
 */
@Test
public class ReplicationStatusTest {

    private ReplicationStatus dbStatus, tableStatus;

    public ReplicationStatusTest() {}


    @BeforeClass
    public void prepare() throws Exception {
        dbStatus = new ReplicationStatus("source", "target", "jobname",
                "default1", null, ReplicationStatus.Status.INIT, 0L);
        tableStatus = new ReplicationStatus("source", "target", "jobname",
                "testDb", "Table1", ReplicationStatus.Status.SUCCESS, 0L);
    }

    public void replicationStatusSerializeTest() throws Exception {
        String expected = "{\n    \"sourceUri\": \"source\",\n"
                + "    \"targetUri\": \"target\",\n    \"jobName\": \"jobname\",\n"
                + "    \"database\": \"testdb\",\n    \"table\": \"table1\",\n"
                + "    \"status\": \"SUCCESS\",\n    \"eventId\": 0\n}";
        String actual = tableStatus.toJsonString();
        Assert.assertEquals(actual, expected);

        expected = "{\n    \"sourceUri\": \"source\",\n    \"targetUri\": \"target\",\n"
                + "    \"jobName\": \"jobname\",\n    \"database\": \"default1\",\n"
                + "    \"status\": \"INIT\",\n    \"eventId\": 0\n}";
        actual = dbStatus.toJsonString();
        Assert.assertEquals(actual, expected);
    }

    public void replicationStatusDeserializeTest() throws Exception {
        String tableInput = "{\n    \"sourceUri\": \"source\",\n"
                + "    \"targetUri\": \"target\",\n    \"jobName\": \"testJob\",\n"
                + "    \"database\": \"Test1\",\n    \"table\": \"table1\",\n"
                + "    \"status\": \"SUCCESS\",\n    \"eventId\": 0\n}";
        String dbInput = "{ \"sourceUri\": \"source\", \"targetUri\": \"target\",\"jobName\": \"jobname\",\n"
                + "    \"database\": \"default1\", \"status\": \"FAILURE\","
                + "   \"eventId\": 27, \"statusLog\": \"testLog\"}";

        ReplicationStatus newDbStatus = new ReplicationStatus(dbInput);
        ReplicationStatus newTableStatus = new ReplicationStatus(tableInput);

        Assert.assertEquals(newDbStatus.getTable(), null);
        Assert.assertEquals(newDbStatus.getEventId(), 27);
        Assert.assertEquals(newDbStatus.getDatabase(), "default1");
        Assert.assertEquals(newDbStatus.getLog(), "testLog");
        Assert.assertEquals(newDbStatus.getStatus(), ReplicationStatus.Status.FAILURE);


        Assert.assertEquals(newTableStatus.getTable(), "table1");
        Assert.assertEquals(newTableStatus.getEventId(), 0);
        Assert.assertEquals(newTableStatus.getDatabase(), "test1");
        Assert.assertEquals(newTableStatus.getJobName(), "testJob");

        // no table, no eventId, no log
        dbInput = "{\n    \"sourceUri\": \"source\",\n"
                + "    \"targetUri\": \"target\",\n    \"jobName\": \"testJob\",\n"
                + "    \"database\": \"Test1\",\n"
                + "    \"status\": \"SUCCESS\"\n}";
        newDbStatus = new ReplicationStatus(dbInput);

        Assert.assertEquals(newDbStatus.getDatabase(), "test1");
        Assert.assertEquals(newDbStatus.getTable(), null);
        Assert.assertEquals(newDbStatus.getEventId(), -1);
        Assert.assertEquals(newDbStatus.getLog(), null);

    }

    public void invalidEventIdTest() throws Exception {
        String tableInput = "{\n    \"sourceUri\": \"source\",\n"
                + "    \"targetUri\": \"target\",\n    \"jobName\": \"testJob\",\n"
                + "    \"database\": \"test1\",\n    \"table\": \"table1\",\n"
                + "    \"status\": \"SUCCESS\",\n    \"eventId\": -100\n}";

        ReplicationStatus newTableStatus = new ReplicationStatus(tableInput);
        Assert.assertEquals(newTableStatus.getEventId(), -1);

        newTableStatus.setEventId(-200);
        Assert.assertEquals(newTableStatus.getEventId(), -1);

        String expected = "{\n    \"sourceUri\": \"source\",\n"
                + "    \"targetUri\": \"target\",\n    \"jobName\": \"testJob\",\n"
                + "    \"database\": \"test1\",\n    \"table\": \"table1\",\n"
                + "    \"status\": \"SUCCESS\",\n    \"eventId\": -1\n}";
        String actual = newTableStatus.toJsonString();
        Assert.assertEquals(actual, expected);

        newTableStatus.setEventId(50);
        Assert.assertEquals(newTableStatus.getEventId(), 50);
    }

    public void invalidStatusTest() throws Exception {

        String dbInput = "{ \"sourceUri\": \"source\", \"targetUri\": \"target\",\"jobName\": \"jobname\",\n"
                + "    \"database\": \"default1\", \"status\": \"BLAH\","
                + "   \"eventId\": 27, \"statusLog\": \"testLog\"}";

        try {
            new ReplicationStatus(dbInput);
            Assert.fail();
        } catch (HiveReplicationException e) {
            Assert.assertEquals(e.getMessage(),
                    "Unable to deserialize jsonString to ReplicationStatus. Invalid status BLAH");
        }
    }


}
