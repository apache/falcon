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

package org.apache.falcon.retention;

import org.apache.falcon.Pair;
import org.apache.falcon.cluster.util.EmbeddedCluster;
import org.apache.falcon.entity.Storage;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Test for FeedEvictor.
 */
public class FeedEvictorTest {

    private EmbeddedCluster cluster;
    private final InMemoryWriter stream = new InMemoryWriter(System.out);
    private final Map<String, String> map = new HashMap<String, String>();
    private String hdfsUrl;

    @BeforeClass
    public void start() throws Exception {
        cluster = EmbeddedCluster.newCluster("test");
        hdfsUrl = cluster.getConf().get(HadoopClientFactory.FS_DEFAULT_NAME_KEY);
        FeedEvictor.OUT.set(stream);
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
        } catch (Exception ignore) {
            // ignore
        }

        try {
            FeedEvictor.main(new String[]{"1", "2"});
            Assert.fail("Expected an exception to be thrown");
        } catch (Exception ignore) {
            // ignore
        }
    }

    @Test
    public void testEviction1() throws Exception {
        try {
            FeedEvictor.main(new String[]{"1", "2", "3", "4", "5", "6", "7"});
        } catch (Exception ignore) {
            // ignore
        }
    }

    @Test
    public void testEviction2() throws Exception {
        try {
            Configuration conf = cluster.getConf();
            FileSystem fs = FileSystem.get(conf);
            fs.delete(new Path("/"), true);
            stream.clear();

            Pair<List<String>, List<String>> pair =
                    createTestData("feed1", "yyyy-MM-dd/'more'/yyyy", 10, TimeUnit.DAYS, "/data");
            final String storageUrl = cluster.getConf().get(HadoopClientFactory.FS_DEFAULT_NAME_KEY);
            String dataPath = LocationType.DATA.name() + "="
                    + storageUrl + "/data/YYYY/feed1/mmHH/dd/MM/?{YEAR}-?{MONTH}-?{DAY}/more/?{YEAR}";
            String logFile = hdfsUrl + "/falcon/staging/feed/instancePaths-2012-01-01-01-00.csv";

            FeedEvictor.main(new String[]{
                "-feedBasePath", dataPath,
                "-retentionType", "instance",
                "-retentionLimit", "days(10)",
                "-timeZone", "UTC",
                "-frequency", "daily",
                "-logFile", logFile,
                "-falconFeedStorageType", Storage.TYPE.FILESYSTEM.name(),
            });

            assertFailures(fs, pair);
            compare(map.get("feed1"), stream.getBuffer());

            String expectedInstancePaths = getExpectedInstancePaths(dataPath);
            Assert.assertEquals(readLogFile(new Path(logFile)), expectedInstancePaths);

            String deletedPath = expectedInstancePaths.split(",")[0].split("=")[1];
            Assert.assertFalse(fs.exists(new Path(deletedPath)));
            //empty parents
            Assert.assertFalse(fs.exists(new Path(deletedPath).getParent()));
            Assert.assertFalse(fs.exists(new Path(deletedPath).getParent().getParent()));
            //base path not deleted
            Assert.assertTrue(fs.exists(new Path("/data/YYYY/feed1/mmHH/dd/MM/")));

        } catch (Exception e) {
            Assert.fail("Unknown exception", e);
        }
    }

    private String getExpectedInstancePaths(String dataPath) {
        StringBuilder newBuffer = new StringBuilder("instancePaths=");
        DateFormat format = new SimpleDateFormat("yyyyMMddHHmm");
        format.setTimeZone(TimeZone.getTimeZone("UTC"));
        String[] locs = dataPath.split("#");
        String[] instances = stream.getBuffer().split("instances=")[1].split(",");
        if (instances[0].equals("NULL")) {
            return "instancePaths=";
        }

        for (int i = 0; i < locs.length; i++) {
            for (int j = 0, k = i * instances.length / locs.length; j < instances.length / locs.length; j++) {
                String[] paths = locs[i].split("=");
                String path = paths[1];
                String instancePath = path.replaceAll("\\?\\{YEAR\\}", instances[j + k].substring(0, 4));
                instancePath = instancePath.replaceAll("\\?\\{MONTH\\}", instances[j + k].substring(4, 6));
                instancePath = instancePath.replaceAll("\\?\\{DAY\\}", instances[j + k].substring(6, 8));
                instancePath = instancePath.replaceAll("\\?\\{HOUR\\}", instances[j + k].substring(8, 10));
                instancePath = instancePath.replaceAll("\\?\\{MINUTE\\}", instances[j + k].substring(10, 12));
                newBuffer.append(instancePath).append(',');
            }
        }
        return newBuffer.toString();
    }

    private String readLogFile(Path logFile) throws IOException {
        Configuration conf = cluster.getConf();
        FileSystem fs = FileSystem.get(conf);
        ByteArrayOutputStream writer = new ByteArrayOutputStream();
        InputStream date = fs.open(logFile);
        IOUtils.copyBytes(date, writer, 4096, true);
        return writer.toString();
    }

    private void compare(String str1, String str2) {
        String[] instances1 = str1.split("=")[1].split(",");
        String[] instances2 = str2.split("instances=")[1].split(",");

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

            Pair<List<String>, List<String>> pair =
                    createTestData("feed2", "yyyyMMddHH/'more'/yyyy", 5, TimeUnit.HOURS, "/data");
            final String storageUrl = cluster.getConf().get(HadoopClientFactory.FS_DEFAULT_NAME_KEY);
            String dataPath = LocationType.DATA.name() + "="
                    + storageUrl + "/data/YYYY/feed2/mmHH/dd/MM/?{YEAR}?{MONTH}?{DAY}?{HOUR}/more/?{YEAR}";
            String logFile = hdfsUrl + "/falcon/staging/feed/instancePaths-2012-01-01-02-00.csv";
            FeedEvictor.main(new String[]{
                "-feedBasePath", dataPath,
                "-retentionType", "instance",
                "-retentionLimit", "hours(5)",
                "-timeZone", "UTC",
                "-frequency", "hourly",
                "-logFile", logFile,
                "-falconFeedStorageType", Storage.TYPE.FILESYSTEM.name(),
            });
            assertFailures(fs, pair);

            compare(map.get("feed2"), stream.getBuffer());

            Assert.assertEquals(readLogFile(new Path(logFile)),
                    getExpectedInstancePaths(dataPath));

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

            Pair<List<String>, List<String>> pair = createTestData("/data");
            FeedEvictor.main(new String[] {
                "-feedBasePath", LocationType.DATA.name() + "="
                    + cluster.getConf().get(HadoopClientFactory.FS_DEFAULT_NAME_KEY)
                    + "/data/YYYY/feed3/dd/MM/?{MONTH}/more/?{HOUR}",
                "-retentionType", "instance",
                "-retentionLimit", "months(5)",
                "-timeZone", "UTC",
                "-frequency", "hourly",
                "-logFile", conf.get(HadoopClientFactory.FS_DEFAULT_NAME_KEY)
                + "/falcon/staging/feed/2012-01-01-04-00",
                "-falconFeedStorageType", Storage.TYPE.FILESYSTEM.name(),
            });
            Assert.assertEquals("instances=NULL", stream.getBuffer());

            stream.clear();
            String dataPath = "/data/YYYY/feed4/dd/MM/more/hello";
            String logFile = hdfsUrl + "/falcon/staging/feed/instancePaths-2012-01-01-02-00.csv";
            FeedEvictor.main(new String[] {
                "-feedBasePath", LocationType.DATA.name() + "="
                    + cluster.getConf().get(HadoopClientFactory.FS_DEFAULT_NAME_KEY) + dataPath,
                "-retentionType", "instance",
                "-retentionLimit", "hours(5)",
                "-timeZone", "UTC",
                "-frequency", "hourly",
                "-logFile", logFile,
                "-falconFeedStorageType", Storage.TYPE.FILESYSTEM.name(),
            });
            Assert.assertEquals("instances=NULL", stream.getBuffer());

            Assert.assertEquals(readLogFile(new Path(logFile)), getExpectedInstancePaths(dataPath));

            assertFailures(fs, pair);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unknown exception", e);
        }
    }

    @Test
    public void testEviction5() throws Exception {
        try {
            Configuration conf = cluster.getConf();
            FileSystem fs = FileSystem.get(conf);
            fs.delete(new Path("/"), true);
            stream.clear();

            Pair<List<String>, List<String>> pair = createTestData("/data");
            createTestData("/stats");
            createTestData("/meta");
            createTestData("/tmp");
            final String storageUrl = cluster.getConf().get(HadoopClientFactory.FS_DEFAULT_NAME_KEY);
            FeedEvictor.main(new String[] {
                "-feedBasePath", getFeedBasePath(LocationType.DATA, storageUrl)
                + "#" + getFeedBasePath(LocationType.STATS, storageUrl)
                    + "#" + getFeedBasePath(LocationType.META, storageUrl)
                    + "#" + getFeedBasePath(LocationType.TMP, storageUrl),
                "-retentionType", "instance",
                "-retentionLimit", "months(5)",
                "-timeZone", "UTC",
                "-frequency", "hourly",
                "-logFile", conf.get(HadoopClientFactory.FS_DEFAULT_NAME_KEY)
                + "/falcon/staging/feed/2012-01-01-04-00", "-falconFeedStorageType",
                Storage.TYPE.FILESYSTEM.name(),
            });
            Assert.assertEquals("instances=NULL", stream.getBuffer());

            stream.clear();
            String dataPath = LocationType.DATA.name() + "="
                    + cluster.getConf().get(HadoopClientFactory.FS_DEFAULT_NAME_KEY)
                    + "/data/YYYY/feed4/dd/MM/more/hello";
            String logFile = hdfsUrl + "/falcon/staging/feed/instancePaths-2012-01-01-02-00.csv";
            FeedEvictor.main(new String[]{
                "-feedBasePath", dataPath,
                "-retentionType", "instance",
                "-retentionLimit", "hours(5)",
                "-timeZone", "UTC",
                "-frequency", "hourly",
                "-logFile", logFile,
                "-falconFeedStorageType", Storage.TYPE.FILESYSTEM.name(),
            });
            Assert.assertEquals("instances=NULL", stream.getBuffer());

            Assert.assertEquals(readLogFile(new Path(logFile)), getExpectedInstancePaths(dataPath));

            assertFailures(fs, pair);
        } catch (Exception e) {
            Assert.fail("Unknown exception", e);
        }
    }

    @Test
    public void testEviction6() throws Exception {
        try {
            Configuration conf = cluster.getConf();
            FileSystem fs = FileSystem.get(conf);
            fs.delete(new Path("/"), true);
            stream.clear();

            Pair<List<String>, List<String>> pair =
                    createTestData("feed1", "yyyy-MM-dd/'more'/yyyy", 10, TimeUnit.DAYS, "/data");
            createTestData("feed1", "yyyy-MM-dd/'more'/yyyy", 10, TimeUnit.DAYS, "/stats");
            createTestData("feed1", "yyyy-MM-dd/'more'/yyyy", 10, TimeUnit.DAYS, "/meta");

            final String storageUrl = cluster.getConf().get(HadoopClientFactory.FS_DEFAULT_NAME_KEY);
            String dataPath =
                    "DATA=" + storageUrl + "/data/YYYY/feed1/mmHH/dd/MM/?{YEAR}-?{MONTH}-?{DAY}/more/?{YEAR}"
                    + "#STATS=" + storageUrl + "/stats/YYYY/feed1/mmHH/dd/MM/?{YEAR}-?{MONTH}-?{DAY}/more/?{YEAR}"
                    + "#META=" + storageUrl + "/meta/YYYY/feed1/mmHH/dd/MM/?{YEAR}-?{MONTH}-?{DAY}/more/?{YEAR}";
            String logFile = hdfsUrl + "/falcon/staging/feed/instancePaths-2012-01-01-01-00.csv";

            FeedEvictor.main(new String[] {
                "-feedBasePath", dataPath,
                "-retentionType", "instance",
                "-retentionLimit", "days(10)",
                "-timeZone", "UTC",
                "-frequency", "daily",
                "-logFile", logFile,
                "-falconFeedStorageType", Storage.TYPE.FILESYSTEM.name(),
            });

            assertFailures(fs, pair);

            Assert.assertEquals(readLogFile(new Path(logFile)),
                    getExpectedInstancePaths(dataPath));

        } catch (Exception e) {
            Assert.fail("Unknown exception", e);
        }
    }

    @Test
    public void testEvictionWithEmptyDirs() throws Exception {
        try {
            Configuration conf = cluster.getConf();
            FileSystem fs = FileSystem.get(conf);
            fs.delete(new Path("/"), true);
            stream.clear();

            Pair<List<String>, List<String>> pair = generateInstances(fs, "feed1",
                "yyyy/MM/dd/'more'/yyyy", 10, TimeUnit.DAYS, "/data", false);
            final String storageUrl = cluster.getConf().get(HadoopClientFactory.FS_DEFAULT_NAME_KEY);
            String dataPath = LocationType.DATA.name() + "="
                + storageUrl + "/data/YYYY/feed1/mmHH/dd/MM/?{YEAR}/?{MONTH}/?{DAY}/more/?{YEAR}";
            String logFile = hdfsUrl + "/falcon/staging/feed/instancePaths-2012-01-01-01-00.csv";
            long beforeDelCount = fs.getContentSummary(new Path(("/data/YYYY/feed1/mmHH/dd/MM/"))).getDirectoryCount();

            FeedEvictor.main(new String[]{
                "-feedBasePath", dataPath,
                "-retentionType", "instance",
                "-retentionLimit", "days(10)",
                "-timeZone", "UTC",
                "-frequency", "daily",
                "-logFile", logFile,
                "-falconFeedStorageType", Storage.TYPE.FILESYSTEM.name(),
            });

            compare(map.get("feed1"), stream.getBuffer());

            String expectedInstancePaths = getExpectedInstancePaths(dataPath);
            Assert.assertEquals(readLogFile(new Path(logFile)), expectedInstancePaths);

            String deletedPath = expectedInstancePaths.split(",")[0].split("=")[1];
            Assert.assertFalse(fs.exists(new Path(deletedPath)));
            //empty parents
            Assert.assertFalse(fs.exists(new Path(deletedPath).getParent()));
            Assert.assertFalse(fs.exists(new Path(deletedPath).getParent().getParent()));
            //base path not deleted
            Assert.assertTrue(fs.exists(new Path("/data/YYYY/feed1/mmHH/dd/MM/")));
            //non-eligible empty dirs
            long afterDelCount = fs.getContentSummary(new Path(("/data/YYYY/feed1/mmHH/dd/MM/"))).getDirectoryCount();
            Assert.assertEquals((beforeDelCount - afterDelCount), 19);
            for(String path: pair.second){
                Assert.assertTrue(fs.exists(new Path(path)));
            }


        } catch (Exception e) {
            Assert.fail("Unknown exception", e);
        }
    }

    @Test
    public void testFeedBasePathExists() throws Exception {
        try {
            Configuration conf = cluster.getConf();
            FileSystem fs = FileSystem.get(conf);
            fs.delete(new Path("/"), true);
            stream.clear();

            Pair<List<String>, List<String>> pair = generateInstances(fs, "feed3",
                    "yyyy/MM/dd/hh/mm", 1, TimeUnit.MINUTES, "/data", false);
            final String storageUrl = cluster.getConf().get(HadoopClientFactory.FS_DEFAULT_NAME_KEY);
            String dataPath = LocationType.DATA.name() + "="
                    + storageUrl + "/data/YYYY/feed3/mmHH/dd/MM/?{YEAR}/?{MONTH}/?{DAY}/?{HOUR}/?{MINUTE}";
            String logFile = hdfsUrl + "/falcon/staging/feed/instancePaths-2012-01-01-01-00.csv";
            long beforeDelCount = fs.getContentSummary(new Path(("/data/YYYY/feed3/mmHH/dd/MM/"))).getDirectoryCount();

            FeedEvictor.main(new String[]{
                "-feedBasePath", dataPath,
                "-retentionType", "instance",
                "-retentionLimit", "minutes(-1)",
                "-timeZone", "UTC",
                "-frequency", "minutes",
                "-logFile", logFile,
                "-falconFeedStorageType", Storage.TYPE.FILESYSTEM.name(),
            });

            String expectedInstancePaths = getExpectedInstancePaths(dataPath);
            Assert.assertEquals(readLogFile(new Path(logFile)), expectedInstancePaths);

            //Feed Base path must exist
            Assert.assertTrue(fs.exists(new Path("/data/YYYY/feed3/mmHH/dd/MM/")));
            FileStatus[] files = fs.listStatus(new Path(("/data/YYYY/feed3/mmHH/dd/MM/")));
            //Number of directories/files inside feed base path should be 0
            Assert.assertEquals(files.length, 0);
            long afterDelCount = fs.getContentSummary(new Path(("/data/YYYY/feed3/mmHH/dd/MM/"))).getDirectoryCount();
            //Number of directories deleted
            Assert.assertEquals((beforeDelCount - afterDelCount), 11);

        } catch (Exception e) {
            Assert.fail("Unknown exception", e);
        }
    }


    private Pair<List<String>, List<String>> createTestData(String locationType) throws Exception {
        Configuration conf = cluster.getConf();
        FileSystem fs = FileSystem.get(conf);

        List<String> outOfRange = new ArrayList<String>();
        List<String> inRange = new ArrayList<String>();

        touch(fs, locationType + "/YYYY/feed3/dd/MM/02/more/hello", true);
        touch(fs, locationType + "/YYYY/feed4/dd/MM/02/more/hello", true);
        touch(fs, locationType + "/YYYY/feed1/mmHH/dd/MM/bad-va-lue/more/hello", true);
        touch(fs, locationType + "/somedir/feed1/mmHH/dd/MM/bad-va-lue/more/hello", true);
        outOfRange.add(locationType + "/YYYY/feed3/dd/MM/02/more/hello");
        outOfRange.add(locationType + "/YYYY/feed4/dd/MM/02/more/hello");
        outOfRange.add(locationType + "/YYYY/feed1/mmHH/dd/MM/bad-va-lue/more/hello");
        outOfRange.add(locationType + "/somedir/feed1/mmHH/dd/MM/bad-va-lue/more/hello");

        return Pair.of(inRange, outOfRange);
    }

    private Pair<List<String>, List<String>> createTestData(String feed, String mask,
                                                            int period, TimeUnit timeUnit,
                                                            String locationType) throws Exception {
        Configuration conf = cluster.getConf();
        FileSystem fs = FileSystem.get(conf);

        List<String> outOfRange = new ArrayList<String>();
        List<String> inRange = new ArrayList<String>();

        Pair<List<String>, List<String>> pair = createTestData(locationType);
        outOfRange.addAll(pair.second);
        inRange.addAll(pair.first);

        pair = generateInstances(fs, feed, mask, period, timeUnit, locationType, true);
        outOfRange.addAll(pair.second);
        inRange.addAll(pair.first);
        return Pair.of(inRange, outOfRange);
    }

    private Pair<List<String>, List<String>> generateInstances(
            FileSystem fs, String feed, String formatString,
            int range, TimeUnit timeUnit, String locationType, boolean generateFiles) throws Exception {

        List<String> outOfRange = new ArrayList<String>();
        List<String> inRange = new ArrayList<String>();

        DateFormat format = new SimpleDateFormat(formatString);
        format.setTimeZone(TimeZone.getTimeZone("UTC"));
        long now = System.currentTimeMillis();

        DateFormat displayFormat = new
                SimpleDateFormat(timeUnit == TimeUnit.HOURS ? "yyyyMMddHH" : "yyyyMMdd");
        displayFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

        StringBuilder buffer = new StringBuilder();
        for (long date = now;
             date > now - timeUnit.toMillis(range + 6);
             date -= timeUnit.toMillis(1)) {
            String path = locationType + "/YYYY/" + feed + "/mmHH/dd/MM/" + format.format(date);
            touch(fs, path, generateFiles);
            if (date <= now && date > now - timeUnit.toMillis(range)) {
                outOfRange.add(path);
            } else {
                inRange.add(path);
                buffer.append((displayFormat.format(date) + "0000").substring(0, 12)).append(',');
            }
        }

        map.put(feed, "instances=" + buffer.substring(0, buffer.length() - 1));
        return Pair.of(inRange, outOfRange);
    }

    private void touch(FileSystem fs, String path, boolean generateFiles) throws Exception {
        if (generateFiles) {
            fs.create(new Path(path)).close();
        } else {
            fs.mkdirs(new Path(path));
        }
    }

    private String getFeedBasePath(LocationType locationType, String storageUrl) {
        return locationType.name() + "=" + storageUrl
                + "/" + locationType.name().toLowerCase() + "/data/YYYY/feed3/dd/MM/?{MONTH}/more/?{HOUR}";
    }

    private static class InMemoryWriter extends PrintStream {

        private final StringBuffer buffer = new StringBuffer();

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
