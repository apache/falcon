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

package org.apache.ivory.retention;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.ivory.Pair;
import org.apache.ivory.cluster.util.EmbeddedCluster;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class FeedEvictorTest {

    private EmbeddedCluster cluster;
    private InMemoryWriter stream = new InMemoryWriter(System.out);
    private Map<String, String> map = new HashMap<String, String>();

    @BeforeClass
    public void start() throws Exception{
        cluster = EmbeddedCluster.newCluster("test", false);
        FeedEvictor.stream = stream;
    }

    @AfterClass
    public void close() throws Exception {
        cluster.shutdown();
    }

    @Test
    public void testBadArgs() throws Exception {
        try {
            FeedEvictor.main(null);
            Assert.fail("Expected an exception to be thrown");
        } catch (Exception ignore) { }

        try {
            FeedEvictor.main(new String[] {"1","2"});
            Assert.fail("Expected an exception to be thrown");
        } catch (Exception ignore) { }
    }

    @Test
    public void testEviction1() throws Exception {
        try {
            FeedEvictor.main(new String[]{"1", "2", "3", "4", "5"});
        } catch (Exception e) {
        }
    }

    @Test
    public void testEviction2() throws Exception {
        try {
            Configuration conf = cluster.getConf();
            FileSystem fs = FileSystem.get(conf);
            fs.delete(new Path("/"), true);
            stream.clear();

            Pair<List<String>, List<String>>  pair;
            pair = createTestData("feed1", "yyyy-MM-dd", 10, TimeUnit.DAYS);
            FeedEvictor.main(new String[]{cluster.getConf().
                    get("fs.default.name") + "/data/YYYY/feed1/mmHH/dd/MM/" +
                    "${YEAR}-${MONTH}-${DAY}/more/${YEAR}", "instance",
                    "days(10)", "UTC", "daily"});

            assertFailures(fs, pair);
            compare(map.get("feed1"), stream.getBuffer());
        } catch (Exception e) {
            Assert.fail("Unknown exception", e);
        }
    }

    private void compare(String str1, String str2) {
        String[] instances1 = str1.split("=")[1].split(",");
        String[] instances2 = str2.split("=")[1].split(",");

        Arrays.sort(instances1);
        Arrays.sort(instances2);
        Assert.assertEquals(instances1, instances2);
    }

    private void assertFailures(FileSystem fs, Pair<List<String>, List<String>> pair) throws IOException {
        for (String path : pair.second) {
            if (!fs.exists(new Path(path))) {
                Assert.fail("Expecting " + path + " to be present");
            }
        }
        for (String path : pair.first) {
            if (fs.exists(new Path(path))) {
                Assert.fail("Expecting " + path + " to be deleted");
            }
        }
    }

    @Test
    public void testEviction3() throws Exception {
        try {
            Configuration conf = cluster.getConf();
            FileSystem fs = FileSystem.get(conf);
            fs.delete(new Path("/"), true);
            stream.clear();

            Pair<List<String>, List<String>>  pair;
            pair = createTestData("feed2", "yyyyMMddHH", 5, TimeUnit.HOURS);
            FeedEvictor.main(new String[]{cluster.getConf().
                    get("fs.default.name") + "/data/YYYY/feed2/mmHH/dd/MM/" +
                    "${YEAR}${MONTH}${DAY}${HOUR}/more/${YEAR}", "instance",
                    "hours(5)", "UTC", "hourly"});
            assertFailures(fs, pair);

            compare(map.get("feed2"), stream.getBuffer());
        } catch (Exception e) {
            Assert.fail("Unknown exception", e);
        }
    }

    @Test
    public void testEviction4() throws Exception {
        try {
            Configuration conf = cluster.getConf();
            FileSystem fs = FileSystem.get(conf);
            fs.delete(new Path("/"), true);
            stream.clear();

            Pair<List<String>, List<String>>  pair;
            pair = createTestData();
            FeedEvictor.main(new String[]{cluster.getConf().
                    get("fs.default.name") + "/data/YYYY/feed3/dd/MM/" +
                    "${MONTH}/more/${HOUR}", "instance",
                    "months(5)", "UTC", "hourly"});
            Assert.assertEquals("instances=NULL", stream.getBuffer());

            stream.clear();
            FeedEvictor.main(new String[]{cluster.getConf().
                    get("fs.default.name") + "/data/YYYY/feed4/dd/MM/" +
                    "02/more/hello", "instance",
                    "hours(5)", "UTC", "hourly"});
            Assert.assertEquals("instances=NULL", stream.getBuffer());

            assertFailures(fs, pair);
        } catch (Exception e) {
            Assert.fail("Unknown exception", e);
        }
    }

    private Pair<List<String>, List<String>> createTestData() throws Exception {
        Configuration conf = cluster.getConf();
        FileSystem fs = FileSystem.get(conf);

        List<String> outOfRange = new ArrayList<String>();
        List<String> inRange = new ArrayList<String>();

        touch(fs, "/data/YYYY/feed3/dd/MM/02/more/hello");
        touch(fs, "/data/YYYY/feed4/dd/MM/02/more/hello");
        touch(fs, "/data/YYYY/feed1/mmHH/dd/MM/bad-va-lue/more/hello");
        touch(fs, "/data/somedir/feed1/mmHH/dd/MM/bad-va-lue/more/hello");
        outOfRange.add("/data/YYYY/feed3/dd/MM/02/more/hello");
        outOfRange.add("/data/YYYY/feed4/dd/MM/02/more/hello");
        outOfRange.add("/data/YYYY/feed1/mmHH/dd/MM/bad-va-lue/more/hello");
        outOfRange.add("/data/somedir/feed1/mmHH/dd/MM/bad-va-lue/more/hello");

        return Pair.of(inRange, outOfRange);
    }

    private Pair<List<String>, List<String>> createTestData(String feed,
                                                            String mask,
                                                            int period,
                                                            TimeUnit timeUnit)
            throws Exception {

        Configuration conf = cluster.getConf();
        FileSystem fs = FileSystem.get(conf);

        List<String> outOfRange = new ArrayList<String>();
        List<String> inRange = new ArrayList<String>();

        Pair<List<String>, List<String>> pair = createTestData();
        outOfRange.addAll(pair.second);
        inRange.addAll(pair.first);

        pair = generateInstances(fs, feed, mask, period, timeUnit);
        outOfRange.addAll(pair.second);
        inRange.addAll(pair.first);
        return Pair.of(inRange,  outOfRange);
    }

    private Pair<List<String>, List<String>> generateInstances(
            FileSystem fs, String feed, String formatString,
            int range, TimeUnit timeUnit) throws Exception {

        List<String> outOfRange = new ArrayList<String>();
        List<String> inRange = new ArrayList<String>();

        DateFormat format = new SimpleDateFormat(formatString);
        format.setTimeZone(TimeZone.getTimeZone("UTC"));
        long now = System.currentTimeMillis();

        DateFormat displayFormat = new
                SimpleDateFormat(timeUnit == TimeUnit.HOURS ? "yyyyMMddHH" : "yyyyMMdd");
        displayFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

        StringBuffer buffer = new StringBuffer();
        for (long date = now + timeUnit.toMillis(3);
             date > now - timeUnit.toMillis(range + 6);
             date -= timeUnit.toMillis(1)) {
            String path = "/data/YYYY/" + feed + "/mmHH/dd/MM/" +
                    format.format(date) + "/more/2012";
            touch(fs, path);
            if (date <= now && date > now - timeUnit.toMillis(range)) {
                outOfRange.add(path);
            } else {
                inRange.add(path);
                buffer.append((displayFormat.format(date) + "0000").
                        substring(0, 12)).append(',');
            }
        }
        map.put(feed, "instances=" + buffer.substring(0, buffer.length() -1));
        return Pair.of(inRange, outOfRange);
    }

    private void touch(FileSystem fs, String path) throws Exception {
        fs.create(new Path(path)).close();
    }

    private static class InMemoryWriter extends PrintStream {

        private StringBuffer buffer = new StringBuffer();

        public InMemoryWriter(OutputStream out) {
            super(out);
        }

        @Override
        public void println(String x) {
            buffer.append(x);
            super.println(x);
        }

        public String getBuffer() {
            return buffer.toString();
        }

        public void clear() {
            buffer.delete(0, buffer.length());
        }
    }
}
