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

        FileSystemStorage storage = new FileSystemStorage(locations);
        Assert.assertEquals(storage.getType(), Storage.TYPE.FILESYSTEM);
    }

    @Test
    public void testGetUriTemplate() throws Exception {
        final Location location = new Location();
        location.setPath("/foo/bar");
        location.setType(LocationType.DATA);
        List<Location> locations = new ArrayList<Location>();
        locations.add(location);

        FileSystemStorage storage = new FileSystemStorage("hdfs://localhost:41020", locations);
        Assert.assertEquals(storage.getUriTemplate(), "hdfs://localhost:41020/foo/bar");
    }

    @Test
    public void testGetUriTemplateWithOutStorageURL() throws Exception {
        final Location location = new Location();
        location.setPath("/foo/bar");
        location.setType(LocationType.DATA);
        List<Location> locations = new ArrayList<Location>();
        locations.add(location);

        FileSystemStorage storage = new FileSystemStorage(locations);
        Assert.assertEquals(storage.getUriTemplate(), "${nameNode}/foo/bar");
    }

    @Test
    public void testGetUriTemplateWithLocationType() throws Exception {
        final Location location = new Location();
        location.setPath("/foo/bar");
        location.setType(LocationType.DATA);
        List<Location> locations = new ArrayList<Location>();
        locations.add(location);

        FileSystemStorage storage = new FileSystemStorage("hdfs://localhost:41020", locations);
        Assert.assertEquals(storage.getUriTemplate(LocationType.DATA), "hdfs://localhost:41020/foo/bar");
    }

    @Test
    public void testExists() throws Exception {
        final Location location = new Location();
        location.setPath("/foo/bar");
        location.setType(LocationType.DATA);
        List<Location> locations = new ArrayList<Location>();
        locations.add(location);

        FileSystemStorage storage = new FileSystemStorage("hdfs://localhost:41020", locations);
        Assert.assertTrue(storage.exists());
    }

    @Test
    public void testIsIdentical() throws Exception {
        final String storageUrl = "hdfs://localhost:41020";
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
        final String storageUrl = "hdfs://localhost:41020";
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
