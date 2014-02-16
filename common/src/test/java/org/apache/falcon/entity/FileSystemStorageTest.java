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

import org.apache.falcon.entity.v0.feed.Location;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Test class for File System Storage.
 */
public class FileSystemStorageTest {

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
}
