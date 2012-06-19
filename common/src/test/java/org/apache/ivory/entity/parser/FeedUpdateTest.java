/*
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

package org.apache.ivory.entity.parser;

import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.AbstractTestBase;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.feed.Feed;
import org.apache.ivory.entity.v0.process.Process;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class FeedUpdateTest extends AbstractTestBase {

    private final FeedEntityParser parser = (FeedEntityParser)
            EntityParserFactory.getParser(EntityType.FEED);
    private final ProcessEntityParser processParser = (ProcessEntityParser)
            EntityParserFactory.getParser(EntityType.PROCESS);
    private static final String FEED1_XML = "/config/feed/feed-0.2.xml";
    private static final String PROCESS1_XML = "/config/process/process-0.2.xml";

    @BeforeClass
    public void init() throws Exception {
        conf.set("hadoop.log.dir", "/tmp");
        this.dfsCluster = new MiniDFSCluster(conf, 1, true, null);
        setup();
    }

	@AfterClass
	public void tearDown() {
		this.dfsCluster.shutdown();
	}

    @Override
    public void setup() throws Exception {
        storeEntity(EntityType.CLUSTER, "testCluster");
        storeEntity(EntityType.CLUSTER, "backupCluster");
        storeEntity(EntityType.CLUSTER, "corp");
        storeEntity(EntityType.FEED, "impressions");
    }

    @Test
    public void testFeedUpdateWithNoDependentProcess() {
        try {
            parser.parseAndValidate(this.getClass()
                    .getResourceAsStream(FEED_XML));
        } catch (IvoryException e) {
            Assert.fail("Didn't expect feed parsing to fail", e);
        }

    }

    @Test
    public void testFeedUpdateWithOneDependentProcess() {
        try {
            ConfigurationStore.get().remove(EntityType.FEED, "clicks");
            ConfigurationStore.get().remove(EntityType.PROCESS, "sample");
            Feed feed = parser.parseAndValidate(this.getClass()
                    .getResourceAsStream(FEED_XML));
            ConfigurationStore.get().publish(EntityType.FEED, feed);
            storeEntity(EntityType.PROCESS, "sample");

            //Try parsing the same feed xml
            parser.parseAndValidate(this.getClass()
                    .getResourceAsStream(FEED_XML));
        } catch (Exception e) {
            Assert.fail("Didn't expect feed parsing to fail", e);
        }
    }

    @Test
    public void testFeedUpdateWithMultipleDependentProcess() {
        try {
            ConfigurationStore.get().remove(EntityType.FEED, "clicks");
            ConfigurationStore.get().remove(EntityType.PROCESS, "sample");
            ConfigurationStore.get().remove(EntityType.PROCESS, "sample2");
            ConfigurationStore.get().remove(EntityType.PROCESS, "sample3");
            Feed feed = parser.parseAndValidate(this.getClass()
                    .getResourceAsStream(FEED_XML));
            ConfigurationStore.get().publish(EntityType.FEED, feed);
            storeEntity(EntityType.PROCESS, "sample");
            storeEntity(EntityType.PROCESS, "sample2");
            storeEntity(EntityType.PROCESS, "sample3");

            //Try parsing the same feed xml
            parser.parseAndValidate(this.getClass()
                    .getResourceAsStream(FEED_XML));
        } catch (Exception e) {
            Assert.fail("Didn't expect feed parsing to fail", e);
        }
    }

    @Test
    public void testFeedUpdateWithViolations() throws Exception {
        ConfigurationStore.get().remove(EntityType.FEED, "clicks");
        ConfigurationStore.get().remove(EntityType.PROCESS, "sample");
        ConfigurationStore.get().remove(EntityType.PROCESS, "sample2");
        storeEntity(EntityType.FEED, "impressionFeed");
        storeEntity(EntityType.FEED, "imp-click-join1");
        storeEntity(EntityType.FEED, "imp-click-join2");
        Feed feed = parser.parseAndValidate(this.getClass()
                .getResourceAsStream(FEED_XML));
        ConfigurationStore.get().publish(EntityType.FEED, feed);
        Process process = processParser.parseAndValidate(this.getClass()
                .getResourceAsStream(PROCESS1_XML));
        ConfigurationStore.get().publish(EntityType.PROCESS, process);
        Process p1 = (Process) process.clone();
        p1.setName("sample2");
        ConfigurationStore.get().publish(EntityType.PROCESS, p1);

        try {
            //Try parsing the same feed xml
            parser.parseAndValidate(this.getClass()
                    .getResourceAsStream(FEED1_XML));
            Assert.fail("Expected feed parsing to fail");
        } catch (ValidationException ignore) {
        }
    }
}
