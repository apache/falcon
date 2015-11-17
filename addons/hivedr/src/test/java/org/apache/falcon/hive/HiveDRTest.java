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

import com.google.common.base.Function;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.falcon.cluster.util.EmbeddedCluster;
import org.apache.falcon.hadoop.JailedFileSystem;
import org.apache.falcon.hive.util.DRStatusStore;
import org.apache.falcon.hive.util.DelimiterUtils;
import org.apache.falcon.hive.util.EventSourcerUtils;
import org.apache.falcon.hive.util.HiveDRStatusStore;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.api.HCatNotificationEvent;
import org.apache.hive.hcatalog.api.repl.Command;
import org.apache.hive.hcatalog.api.repl.ReplicationTask;
import org.apache.hive.hcatalog.api.repl.ReplicationUtils;
import org.apache.hive.hcatalog.api.repl.StagingDirectoryProvider;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.messaging.MessageFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Test for Hive DR export and import.
 */
public class HiveDRTest {
    private FileSystem fileSystem;
    private HCatClient client;
    private MetaStoreEventSourcer sourcer;
    private EmbeddedCluster cluster;
    private String dbName = "testdb";
    private String tableName = "testtable";
    private StagingDirectoryProvider stagingDirectoryProvider;
    private MessageFactory msgFactory = MessageFactory.getInstance();

    @BeforeMethod
    public void setup() throws Exception {
        client = HCatClient.create(new HiveConf());
        initializeFileSystem();
        sourcer = new MetaStoreEventSourcer(client, null, new EventSourcerUtils(cluster.getConf(),
                false, "hiveReplTest"), null);
        stagingDirectoryProvider = new StagingDirectoryProvider.TrivialImpl("/tmp", "/");
    }

    private void initializeFileSystem() throws Exception {
        cluster =  EmbeddedCluster.newCluster("hivedr");
        fileSystem = new JailedFileSystem();
        Path storePath = new Path(DRStatusStore.BASE_DEFAULT_STORE_PATH);
        fileSystem.initialize(LocalFileSystem.getDefaultUri(cluster.getConf()), cluster.getConf());
        if (fileSystem.exists(storePath)) {
            fileSystem.delete(storePath, true);
        }
        FileSystem.mkdirs(fileSystem, storePath, DRStatusStore.DEFAULT_STORE_PERMISSION);
        HiveDRStatusStore drStatusStore = new HiveDRStatusStore(fileSystem,
                fileSystem.getFileStatus(storePath).getGroup());
    }

    // Dummy mapping used for all db and table name mappings
    private Function<String, String> debugMapping = new Function<String, String>(){
        @Nullable
        @Override
        public String apply(@Nullable String s) {
            if (s == null){
                return null;
            } else {
                StringBuilder sb = new StringBuilder(s);
                return sb.toString() + sb.reverse().toString();
            }
        }
    };

    @Test
    public void testExportImportReplication() throws Exception {
        Table t = new Table();
        t.setDbName(dbName);
        t.setTableName(tableName);
        NotificationEvent event = new NotificationEvent(getEventId(), getTime(),
                HCatConstants.HCAT_CREATE_TABLE_EVENT, msgFactory.buildCreateTableMessage(t).toString());
        event.setDbName(t.getDbName());
        event.setTableName(t.getTableName());

        HCatNotificationEvent hev = new HCatNotificationEvent(event);
        ReplicationTask rtask = ReplicationTask.create(client, hev);

        Assert.assertEquals(hev.toString(), rtask.getEvent().toString());
        verifyExportImportReplicationTask(rtask);
    }

    private void verifyExportImportReplicationTask(ReplicationTask rtask) throws Exception {
        Assert.assertEquals(true, rtask.needsStagingDirs());
        Assert.assertEquals(false, rtask.isActionable());

        rtask.withSrcStagingDirProvider(stagingDirectoryProvider)
                .withDstStagingDirProvider(stagingDirectoryProvider)
                .withDbNameMapping(debugMapping)
                .withTableNameMapping(debugMapping);

        List<ReplicationTask> taskAdd = new ArrayList<ReplicationTask>();
        taskAdd.add(rtask);
        sourcer.processTableReplicationEvents(taskAdd.iterator(), dbName, tableName,
                stagingDirectoryProvider.toString(), stagingDirectoryProvider.toString());

        String metaFileName = sourcer.persistToMetaFile("hiveReplTest");
        String event = readEventFile(new Path(metaFileName));
        Assert.assertEquals(event.split(DelimiterUtils.FIELD_DELIM).length, 4);
        Assert.assertEquals(dbName,
                new String(Base64.decodeBase64(event.split(DelimiterUtils.FIELD_DELIM)[0]), "UTF-8"));
        Assert.assertEquals(tableName,
                new String(Base64.decodeBase64(event.split(DelimiterUtils.FIELD_DELIM)[1]), "UTF-8"));

        String exportStr = readEventFile(new Path(event.split(DelimiterUtils.FIELD_DELIM)[2]));
        String[] commandList = exportStr.split(DelimiterUtils.NEWLINE_DELIM);
        for (String command : commandList) {
            Command cmd = ReplicationUtils.deserializeCommand(command);
            Assert.assertEquals(cmd.getEventId(), 42);
            for(String stmt : cmd.get()) {
                Assert.assertTrue(stmt.startsWith("EXPORT TABLE"));
            }
        }

        String importStr = readEventFile(new Path(event.split(DelimiterUtils.FIELD_DELIM)[3]));
        commandList = importStr.split(DelimiterUtils.NEWLINE_DELIM);
        for (String command : commandList) {
            Command cmd = ReplicationUtils.deserializeCommand(command);
            Assert.assertEquals(cmd.getEventId(), 42);
            for (String stmt : cmd.get()) {
                Assert.assertTrue(stmt.startsWith("IMPORT TABLE"));
            }
        }
    }

    @Test
    public void testImportReplication() throws Exception {
        Table t = new Table();
        t.setDbName("testdb");
        t.setTableName("testtable");
        NotificationEvent event = new NotificationEvent(getEventId(), getTime(),
                HCatConstants.HCAT_DROP_TABLE_EVENT, msgFactory.buildDropTableMessage(t).toString());
        event.setDbName(t.getDbName());
        event.setTableName(t.getTableName());

        HCatNotificationEvent hev = new HCatNotificationEvent(event);
        ReplicationTask rtask = ReplicationTask.create(client, hev);

        Assert.assertEquals(hev.toString(), rtask.getEvent().toString());
        verifyImportReplicationTask(rtask);
    }

    private void verifyImportReplicationTask(ReplicationTask rtask) throws Exception {
        Assert.assertEquals(false, rtask.needsStagingDirs());
        Assert.assertEquals(true, rtask.isActionable());
        rtask.withDbNameMapping(debugMapping)
                .withTableNameMapping(debugMapping);

        List<ReplicationTask> taskAdd = new ArrayList<ReplicationTask>();
        taskAdd.add(rtask);
        sourcer.processTableReplicationEvents(taskAdd.iterator(), dbName, tableName,
                stagingDirectoryProvider.toString(), stagingDirectoryProvider.toString());
        String persistFileName = sourcer.persistToMetaFile("hiveReplTest");
        String event = readEventFile(new Path(persistFileName));

        Assert.assertEquals(event.split(DelimiterUtils.FIELD_DELIM).length, 4);
        Assert.assertEquals(dbName,
                new String(Base64.decodeBase64(event.split(DelimiterUtils.FIELD_DELIM)[0]), "UTF-8"));
        Assert.assertEquals(tableName,
                new String(Base64.decodeBase64(event.split(DelimiterUtils.FIELD_DELIM)[1]), "UTF-8"));

        String exportStr = readEventFile(new Path(event.split(DelimiterUtils.FIELD_DELIM)[2]));
        String[] commandList = exportStr.split(DelimiterUtils.NEWLINE_DELIM);
        for (String command : commandList) {
            Command cmd = ReplicationUtils.deserializeCommand(command);
            Assert.assertEquals(cmd.getEventId(), 42);
            Assert.assertEquals(cmd.get().size(), 0);   //In case of drop size of export is 0. Metadata operation
        }

        String importStr = readEventFile(new Path(event.split(DelimiterUtils.FIELD_DELIM)[3]));
        commandList = importStr.split(DelimiterUtils.NEWLINE_DELIM);
        for (String command : commandList) {
            Command cmd = ReplicationUtils.deserializeCommand(command);
            Assert.assertEquals(cmd.getEventId(), 42);
            for (String stmt : cmd.get()) {
                Assert.assertTrue(stmt.startsWith("DROP TABLE"));
            }
        }
    }

    private long getEventId() {
        // Does not need to be unique, just non-zero distinct value to test against.
        return 42;
    }

    private int getTime() {
        // Does not need to be actual time, just non-zero distinct value to test against.
        return 1729;
    }

    private String readEventFile(Path eventFileName) throws IOException {
        StringBuilder eventString = new StringBuilder();
        BufferedReader in = new BufferedReader(new InputStreamReader(
                fileSystem.open(eventFileName)));
        try {
            String line;
            while ((line=in.readLine())!=null) {
                eventString.append(line);
            }
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            IOUtils.closeQuietly(in);
        }
        return eventString.toString();
    }

    @AfterMethod
    public void tearDown() throws Exception {
        client.close();
    }

}
