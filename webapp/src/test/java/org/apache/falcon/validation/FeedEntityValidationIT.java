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

package org.apache.falcon.validation;

import com.sun.jersey.api.client.ClientResponse;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.parser.EntityParserFactory;
import org.apache.falcon.entity.parser.FeedEntityParser;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.LateArrival;
import org.apache.falcon.resource.TestContext;
import org.apache.falcon.util.HiveTestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.xml.bind.Marshaller;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.Map;

/**
 * Tests feed entity validation to verify if the table specified is valid.
 */
public class FeedEntityValidationIT {

    private static final String METASTORE_URL = "thrift://localhost:49083";
    private static final String DATABASE_NAME = "falcondb";
    private static final String TABLE_NAME = "clicks";
    private static final String TABLE_URI =
            "catalog:" + DATABASE_NAME + ":" + TABLE_NAME + "#ds=${YEAR}-${MONTH}-${DAY}-${HOUR}";

    private final TestContext context = new TestContext();

    @BeforeClass
    public void setup() throws Exception {
        TestContext.prepare();

        HiveTestUtils.createDatabase(METASTORE_URL, DATABASE_NAME);
        HiveTestUtils.createTable(METASTORE_URL, DATABASE_NAME, TABLE_NAME);
    }

    @AfterClass
    public void tearDown() throws Exception {
        HiveTestUtils.dropTable(METASTORE_URL, DATABASE_NAME, TABLE_NAME);
        HiveTestUtils.dropDatabase(METASTORE_URL, DATABASE_NAME);
    }

    /**
     * Positive test.
     *
     * @throws Exception
     */
    @Test
    public void testFeedEntityWithValidTable() throws Exception {
        Map<String, String> overlay = context.getUniqueOverlay();
        overlay.put("colo", "default");

        ClientResponse response = context.submitToFalcon(TestContext.CLUSTER_TEMPLATE, overlay, EntityType.CLUSTER);
        context.assertSuccessful(response);

        // submission will parse and validate the feed with table
        overlay.put("tableUri", TABLE_URI);
        response = context.submitToFalcon("/hive-table-feed.xml", overlay, EntityType.FEED);
        context.assertSuccessful(response);
    }

    /**
     * Late data handling test.
     *
     * @throws Exception
     */
    @Test (expectedExceptions = FalconException.class)
    public void testFeedEntityWithValidTableAndLateArrival() throws Exception {
        Map<String, String> overlay = context.getUniqueOverlay();
        overlay.put("colo", "default"); // validations will be ignored if not default & tests fail
        overlay.put("tableUri", TABLE_URI);

        String filePath = TestContext.overlayParametersOverTemplate("/hive-table-feed.xml", overlay);
        InputStream stream = new FileInputStream(filePath);
        FeedEntityParser parser = (FeedEntityParser) EntityParserFactory.getParser(EntityType.FEED);
        Feed feed = parser.parse(stream);
        Assert.assertNotNull(feed);

        final LateArrival lateArrival = new LateArrival();
        lateArrival.setCutOff(new Frequency("4", Frequency.TimeUnit.hours));
        feed.setLateArrival(lateArrival);

        StringWriter stringWriter = new StringWriter();
        Marshaller marshaller = EntityType.FEED.getMarshaller();
        marshaller.marshal(feed, stringWriter);
        System.out.println(stringWriter.toString());
        parser.parseAndValidate(stringWriter.toString());
    }

    @DataProvider(name = "invalidTableUris")
    public Object[][] createInvalidTableUriData() {
        return new Object[][] {
            // does not match with group input's frequency
            {"catalog:" + DATABASE_NAME + ":" + TABLE_NAME + "#ds=ds=${YEAR}-${MONTH}-${DAY}", ""},
            {"catalog:" + DATABASE_NAME + ":" + TABLE_NAME + "#ds=ds=${YEAR}-${MONTH}-${DAY}", ""},
            {"badscheme:" + DATABASE_NAME + ":" + TABLE_NAME + "#ds=ds=${YEAR}-${MONTH}-${DAY}", ""},
            {"catalog:" + DATABASE_NAME + ":" + "badtable" + "#ds=ds=${YEAR}-${MONTH}-${DAY}", ""},
            {"catalog:" + "baddb" + ":" + TABLE_NAME + "#ds=ds=${YEAR}-${MONTH}-${DAY}", ""},
            {"catalog:" + "baddb" + ":" + "badtable" + "#ds=ds=${YEAR}-${MONTH}-${DAY}", ""},
        };
    }

    @Test (dataProvider = "invalidTableUris")
    public void testFeedEntityWithInvalidTableUri(String tableUri, @SuppressWarnings("unused") String ignore)
        throws Exception {

        Map<String, String> overlay = context.getUniqueOverlay();
        overlay.put("colo", "default");

        ClientResponse response = context.submitToFalcon(TestContext.CLUSTER_TEMPLATE, overlay, EntityType.CLUSTER);
        context.assertSuccessful(response);

        // submission will parse and validate the feed with table
        overlay.put("tableUri", tableUri);
        response = context.submitToFalcon("/hive-table-feed.xml", overlay, EntityType.FEED);
        context.assertFailure(response);
    }
}
