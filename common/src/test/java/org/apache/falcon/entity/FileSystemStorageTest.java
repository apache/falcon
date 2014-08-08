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

package org.apache.falcon.entity;

import org.apache.falcon.FalconException;
import org.apache.falcon.cluster.util.EmbeddedCluster;
import org.apache.falcon.entity.v0.AccessControlList;
import org.apache.falcon.entity.v0.feed.Location;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.security.CurrentUser;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Test class for File System Storage.
 */
public class FileSystemStorageTest {

    private static final String USER = "falcon";

    @BeforeClass
    public void setUp() {
        CurrentUser.authenticate(USER);
    }

    @Test
    public void testGetType() throws Exception {
        final Location location = new Location();
        location.setPath("/foo/bar");
        location.setType(LocationType.DATA);
        List<Location> locations = new ArrayList<Location>();
        locations.add(location);

        FileSystemStorage storage = new FileSystemStorage(FileSystemStorage.FILE_SYSTEM_URL, locations);
        Assert.assertEquals(storage.getType(), Storage.TYPE.FILESYSTEM);
    }

    @Test
    public void testCreateFromUriTemplate() throws Exception {
        String feedBasePath = "DATA=hdfs://localhost:8020"
                + "/data/YYYY/feed1/mmHH/dd/MM/${YEAR}-${MONTH}-${DAY}/more/${YEAR}"
                + "#"
                + "META=hdfs://localhost:8020"
                + "/meta/YYYY/feed1/mmHH/dd/MM/${YEAR}-${MONTH}-${DAY}/more/${YEAR}"
                + "#"
                + "STATS=hdfs://localhost:8020"
                + "/stats/YYYY/feed1/mmHH/dd/MM/${YEAR}-${MONTH}-${DAY}/more/${YEAR}";

        FileSystemStorage storage = new FileSystemStorage(feedBasePath);
        Assert.assertEquals(storage.getUriTemplate(), feedBasePath + "#TMP=/tmp");

        Assert.assertEquals("hdfs://localhost:8020", storage.getStorageUrl());
        Assert.assertEquals("hdfs://localhost:8020/data/YYYY/feed1/mmHH/dd/MM/${YEAR}-${MONTH}-${DAY}/more/${YEAR}",
            storage.getUriTemplate(LocationType.DATA));
        Assert.assertEquals("hdfs://localhost:8020/stats/YYYY/feed1/mmHH/dd/MM/${YEAR}-${MONTH}-${DAY}/more/${YEAR}",
                storage.getUriTemplate(LocationType.STATS));
        Assert.assertEquals("hdfs://localhost:8020/meta/YYYY/feed1/mmHH/dd/MM/${YEAR}-${MONTH}-${DAY}/more/${YEAR}",
                storage.getUriTemplate(LocationType.META));
    }

    @Test
    public void testGetUriTemplateForData() throws Exception {
        final Location location = new Location();
        location.setPath("/foo/bar");
        location.setType(LocationType.DATA);
        List<Location> locations = new ArrayList<Location>();
        locations.add(location);

        FileSystemStorage storage = new FileSystemStorage("jail://global:00", locations);
        Assert.assertEquals(storage.getUriTemplate(LocationType.DATA), "jail://global:00/foo/bar");
    }

    @Test
    public void testFSHomeDir() {
        final Location location = new Location();
        location.setPath("foo/bar"); // relative path
        location.setType(LocationType.DATA);
        List<Location> locations = new ArrayList<Location>();
        locations.add(location);

        FileSystemStorage storage = new FileSystemStorage("hdfs://localhost:41020", locations);
        Assert.assertEquals(storage.getWorkingDir().toString(), "/user/falcon");
    }

    @Test
    public void testGetUriTemplateForDataWithRelativePath() throws Exception {
        final Location location = new Location();
        location.setPath("foo/bar"); // relative path
        location.setType(LocationType.DATA);
        List<Location> locations = new ArrayList<Location>();
        locations.add(location);

        FileSystemStorage storage = new FileSystemStorage("hdfs://localhost:41020", locations);
        Assert.assertEquals(storage.getUriTemplate(LocationType.DATA),
                "hdfs://localhost:41020/user/" + USER + "/foo/bar");

        storage = new FileSystemStorage("hdfs://localhost:41020/", locations);
        Assert.assertEquals(storage.getUriTemplate(LocationType.DATA),
                "hdfs://localhost:41020/user/" + USER + "/foo/bar");
    }

    @Test
    public void testGetUriTemplateForDataWithAbsolutePath() throws Exception {
        final Location location = new Location();
        location.setPath("/foo/bar"); // absolute path
        location.setType(LocationType.DATA);
        List<Location> locations = new ArrayList<Location>();
        locations.add(location);

        FileSystemStorage storage = new FileSystemStorage("hdfs://localhost:41020", locations);
        Assert.assertEquals(storage.getUriTemplate(LocationType.DATA), "hdfs://localhost:41020/foo/bar");

        storage = new FileSystemStorage("hdfs://localhost:41020/", locations);
        Assert.assertEquals(storage.getUriTemplate(LocationType.DATA), "hdfs://localhost:41020/foo/bar");
    }

    @Test
    public void testGetUriTemplateForDataWithAbsoluteURL() throws Exception {
        final String absoluteUrl = "s3://host:1000/foo/bar";
        final Location location = new Location();
        location.setPath(absoluteUrl); // absolute url
        location.setType(LocationType.DATA);
        List<Location> locations = new ArrayList<Location>();
        locations.add(location);

        FileSystemStorage storage = new FileSystemStorage("hdfs://localhost:41020", locations);
        Assert.assertEquals(storage.getUriTemplate(LocationType.DATA), absoluteUrl);

        storage = new FileSystemStorage("hdfs://localhost:41020/", locations);
        Assert.assertEquals(storage.getUriTemplate(LocationType.DATA), absoluteUrl);
    }

    @Test
    public void testValidateACL() throws Exception {
        final Location location = new Location();
        Path path = new Path("/foo/bar");
        location.setPath(path.toString());
        location.setType(LocationType.DATA);
        List<Location> locations = new ArrayList<Location>();
        locations.add(location);

        String user = System.getProperty("user.name");
        EmbeddedCluster cluster = EmbeddedCluster.newCluster(user);
        FileSystem fs = cluster.getFileSystem();
        fs.mkdirs(path);

        FileSystemStorage storage = new FileSystemStorage(cluster.getConf().get("fs.default.name"), locations);
        storage.validateACL(new TestACL(user, user, "0x755"));

        //-ve case
        try {
            storage.validateACL(new TestACL("random", user, "0x755"));
            Assert.fail("Validation should have failed");
        } catch(FalconException e) {
            //expected exception
        }

        //Timed path
        location.setPath("/foo/bar/${YEAR}/${MONTH}/${DAY}");
        storage.validateACL(new TestACL(user, user, "rrr"));

        //-ve case
        try {
            storage.validateACL(new TestACL("random", user, "0x755"));
            Assert.fail("Validation should have failed");
        } catch(FalconException e) {
            //expected exception
        }
    }

    @DataProvider(name = "locationTestWithRelativePathDataProvider")
    private Object[][] createLocationTestDataWithRelativePath() {
        return new Object[][] {
            {"hdfs://h:0", "localDC/rc/billing/ua2", "hdfs://h:0/user/" + USER + "/localDC/rc/billing/ua2"},
            {"hdfs://h:0/", "localDC/rc/billing/ua2", "hdfs://h:0/user/" + USER + "/localDC/rc/billing/ua2"},
            {"hdfs://h:0", "localDC/rc/billing/ua2/", "hdfs://h:0/user/" + USER + "/localDC/rc/billing/ua2"},
            {"hdfs://h:0/", "localDC/rc/billing/ua2/", "hdfs://h:0/user/" + USER + "/localDC/rc/billing/ua2"},
            {"hdfs://h:0", "localDC/rc/billing/ua2//", "hdfs://h:0/user/" + USER + "/localDC/rc/billing/ua2"},
            {"hdfs://h:0/", "localDC/rc/billing/ua2//", "hdfs://h:0/user/" + USER + "/localDC/rc/billing/ua2"},
            {"${nameNode}", "localDC/rc/billing/ua2", "${nameNode}/user/" + USER + "/localDC/rc/billing/ua2"},
            {"${nameNode}/", "localDC/rc/billing/ua2", "${nameNode}/user/" + USER + "/localDC/rc/billing/ua2"},
            {"${nameNode}", "localDC/rc/billing/ua2/", "${nameNode}/user/" + USER + "/localDC/rc/billing/ua2"},
            {"${nameNode}/", "localDC/rc/billing/ua2/", "${nameNode}/user/" + USER + "/localDC/rc/billing/ua2"},
            {"${nameNode}", "localDC/rc/billing/ua2//", "${nameNode}/user/" + USER + "/localDC/rc/billing/ua2"},
            {"${nameNode}/", "localDC/rc/billing/ua2//", "${nameNode}/user/" + USER + "/localDC/rc/billing/ua2"},
            {"${nameNode}/", "localDC/rc/billing/ua2//", "${nameNode}/user/" + USER + "/localDC/rc/billing/ua2"},
            {"${nameNode}", "s3://h:p/localDC/rc/billing/ua2//", "s3://h:p/localDC/rc/billing/ua2"},
            {"${nameNode}/", "s3://h:p/localDC/rc/billing/ua2//", "s3://h:p/localDC/rc/billing/ua2"},
            {"hdfs://h:0", "s3://h:p/localDC/rc/billing/ua2//", "s3://h:p/localDC/rc/billing/ua2"},
            {"hdfs://h:0/", "s3://h:p/localDC/rc/billing/ua2//", "s3://h:p/localDC/rc/billing/ua2"},
        };
    }

    @Test (dataProvider = "locationTestWithRelativePathDataProvider")
    public void testGetUriTemplateWithRelativePath(String storageUrl, String path,
                                                   String expected) throws Exception {
        final Location location = new Location();
        location.setPath(path);
        location.setType(LocationType.DATA);
        List<Location> locations = new ArrayList<Location>();
        locations.add(location);

        FileSystemStorage storage = new FileSystemStorage(storageUrl, locations);
        Assert.assertEquals(storage.getUriTemplate(LocationType.DATA), expected);
    }

    @Test
    public void testGetUriTemplate() throws Exception {
        final Location dataLocation = new Location();
        dataLocation.setPath("/data/foo/bar");
        dataLocation.setType(LocationType.DATA);

        final Location metaLocation = new Location();
        metaLocation.setPath("/meta/foo/bar");
        metaLocation.setType(LocationType.META);

        final Location statsLocation = new Location();
        statsLocation.setPath("/stats/foo/bar");
        statsLocation.setType(LocationType.STATS);

        final Location tmpLocation = new Location();
        tmpLocation.setPath("/tmp/foo/bar");
        tmpLocation.setType(LocationType.TMP);

        List<Location> locations = new ArrayList<Location>();
        locations.add(dataLocation);
        locations.add(metaLocation);
        locations.add(statsLocation);
        locations.add(tmpLocation);

        StringBuilder expected = new StringBuilder();
        expected.append(LocationType.DATA)
                .append(FileSystemStorage.LOCATION_TYPE_SEP)
                .append("jail://global:00/data/foo/bar")
                .append(FileSystemStorage.FEED_PATH_SEP)
                .append(LocationType.META)
                .append(FileSystemStorage.LOCATION_TYPE_SEP)
                .append("jail://global:00/meta/foo/bar")
                .append(FileSystemStorage.FEED_PATH_SEP)
                .append(LocationType.STATS)
                .append(FileSystemStorage.LOCATION_TYPE_SEP)
                .append("jail://global:00/stats/foo/bar")
                .append(FileSystemStorage.FEED_PATH_SEP)
                .append(LocationType.TMP)
                .append(FileSystemStorage.LOCATION_TYPE_SEP)
                .append("jail://global:00/tmp/foo/bar");

        FileSystemStorage storage = new FileSystemStorage("jail://global:00", locations);
        Assert.assertEquals(storage.getUriTemplate(), expected.toString());
    }

    @Test
    public void testGetUriTemplateWithOutStorageURL() throws Exception {
        final Location location = new Location();
        location.setPath("/foo/bar");
        location.setType(LocationType.DATA);
        List<Location> locations = new ArrayList<Location>();
        locations.add(location);

        FileSystemStorage storage = new FileSystemStorage(FileSystemStorage.FILE_SYSTEM_URL, locations);
        Assert.assertEquals(storage.getUriTemplate(LocationType.DATA), "${nameNode}/foo/bar");
    }

    @DataProvider(name = "locationTestDataProvider")
    private Object[][] createLocationTestData() {
        return new Object[][] {
            {"jail://global:00", "/localDC/rc/billing/ua2", "/localDC/rc/billing/ua2"},
            {"jail://global:00", "/localDC/rc/billing/ua2/", "/localDC/rc/billing/ua2"},
            {"jail://global:00", "/localDC/rc/billing/ua2//", "/localDC/rc/billing/ua2"},
            {"${nameNode}", "/localDC/rc/billing/ua2", "/localDC/rc/billing/ua2"},
            {"${nameNode}", "/localDC/rc/billing/ua2/", "/localDC/rc/billing/ua2"},
            {"${nameNode}", "/localDC/rc/billing/ua2//", "/localDC/rc/billing/ua2"},
        };
    }

    @Test (dataProvider = "locationTestDataProvider")
    public void testGetUriTemplateWithLocationType(String storageUrl, String path,
                                                   String expected) throws Exception {
        final Location location = new Location();
        location.setPath(path);
        location.setType(LocationType.DATA);
        List<Location> locations = new ArrayList<Location>();
        locations.add(location);

        FileSystemStorage storage = new FileSystemStorage(storageUrl, locations);
        Assert.assertEquals(storage.getUriTemplate(LocationType.DATA), storageUrl + expected);
    }

    @Test
    public void testIsIdentical() throws Exception {
        final String storageUrl = "jail://global:00";
        final Location location1 = new Location();
        location1.setPath("/foo/bar");
        location1.setType(LocationType.DATA);
        List<Location> locations1 = new ArrayList<Location>();
        locations1.add(location1);
        FileSystemStorage storage1 = new FileSystemStorage(storageUrl, locations1);

        final Location location2 = new Location();
        location2.setPath("/foo/bar");
        location2.setType(LocationType.DATA);
        List<Location> locations2 = new ArrayList<Location>();
        locations2.add(location2);
        FileSystemStorage storage2 = new FileSystemStorage(storageUrl, locations2);

        Assert.assertTrue(storage1.isIdentical(storage2));
    }

    @Test
    public void testIsIdenticalNegative() throws Exception {
        final String storageUrl = "jail://global:00";
        final Location location1 = new Location();
        location1.setPath("/foo/baz");
        location1.setType(LocationType.DATA);
        List<Location> locations1 = new ArrayList<Location>();
        locations1.add(location1);
        FileSystemStorage storage1 = new FileSystemStorage(storageUrl, locations1);

        final Location location2 = new Location();
        location2.setPath("/foo/bar");
        location2.setType(LocationType.DATA);
        List<Location> locations2 = new ArrayList<Location>();
        locations2.add(location2);
        FileSystemStorage storage2 = new FileSystemStorage(storageUrl, locations2);

        Assert.assertFalse(storage1.isIdentical(storage2));
    }

    private class TestACL extends AccessControlList {

        /**
         * owner is the Owner of this entity.
         */
        private String owner;

        /**
         * group is the one which has access to read - not used at this time.
         */
        private String group;

        /**
         * permission is not enforced at this time.
         */
        private String permission;

        TestACL(String owner, String group, String permission) {
            this.owner = owner;
            this.group = group;
            this.permission = permission;
        }

        @Override
        public String getOwner() {
            return owner;
        }

        @Override
        public String getGroup() {
            return group;
        }

        @Override
        public String getPermission() {
            return permission;
        }
    }
}
