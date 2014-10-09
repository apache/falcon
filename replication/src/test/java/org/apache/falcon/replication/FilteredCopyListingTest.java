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

package org.apache.falcon.replication;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.tools.CopyListingFileStatus;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hadoop.tools.util.DistCpUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.DataOutputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Test class for org.apache.falcon.replication.FilteredCopyListing.
 */
public class FilteredCopyListingTest {

    private static final Credentials CREDENTIALS = new Credentials();

    public static final Map<String, String> EXPECTED_VALUES = new HashMap<String, String>();

    @BeforeClass
    public static void setup() throws Exception {
        createSourceData();
    }

    private static void createSourceData() throws Exception {
        rmdirs("/tmp/source");
        rmdirs("/tmp/target");
        mkdirs("/tmp/source/1");
        mkdirs("/tmp/source/2");
        mkdirs("/tmp/source/2/3");
        mkdirs("/tmp/source/2/3/4");
        mkdirs("/tmp/source/2/3/40");
        mkdirs("/tmp/source/2/3/7");
        mkdirs("/tmp/source/5");
        touchFile("/tmp/source/5/6");
        mkdirs("/tmp/source/7");
        mkdirs("/tmp/source/7/8");
        touchFile("/tmp/source/7/8/9");
    }

    private static void mkdirs(String path) throws Exception {
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.getLocal(new Configuration());
            fileSystem.mkdirs(new Path(path));
            recordInExpectedValues(path);
        } finally {
            IOUtils.cleanup(null, fileSystem);
        }
    }

    private static void rmdirs(String path) throws Exception {
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.getLocal(new Configuration());
            fileSystem.delete(new Path(path), true);
        } finally {
            IOUtils.cleanup(null, fileSystem);
        }
    }

    private static void touchFile(String path) throws Exception {
        FileSystem fileSystem = null;
        DataOutputStream outputStream = null;
        try {
            fileSystem = FileSystem.getLocal(new Configuration());
            outputStream = fileSystem.create(new Path(path), true, 10);
            recordInExpectedValues(path);
        } finally {
            IOUtils.cleanup(null, fileSystem, outputStream);
        }
    }

    private static void recordInExpectedValues(String path) throws Exception {
        FileSystem fileSystem = FileSystem.getLocal(new Configuration());
        Path sourcePath = new Path(fileSystem.getUri().toString() + path);
        EXPECTED_VALUES.put(sourcePath.toString(), DistCpUtils.getRelativePath(
                new Path("/tmp/source"), sourcePath));
    }

    @Test
    public void testRunNoPattern() throws Exception {
        final URI uri = FileSystem.getLocal(new Configuration()).getUri();
        final String pathString = uri.toString();
        Path fileSystemPath = new Path(pathString);
        Path source = new Path(fileSystemPath.toString() + "///tmp/source");
        Path target = new Path(fileSystemPath.toString() + "///tmp/target");
        Path listingPath = new Path(fileSystemPath.toString() + "///tmp/META/fileList.seq");
        DistCpOptions options = new DistCpOptions(Arrays.asList(source), target);
        options.setSyncFolder(true);

        new FilteredCopyListing(new Configuration(), CREDENTIALS).buildListing(listingPath, options);

        verifyContents(listingPath, -1);
    }

    @Test
    public void testRunStarPattern() throws Exception {
        final URI uri = FileSystem.getLocal(new Configuration()).getUri();
        final String pathString = uri.toString();
        Path fileSystemPath = new Path(pathString);
        Path source = new Path(fileSystemPath.toString() + "///tmp/source");
        Path target = new Path(fileSystemPath.toString() + "///tmp/target");
        Path listingPath = new Path(fileSystemPath.toString() + "///tmp/META/fileList.seq");
        DistCpOptions options = new DistCpOptions(Arrays.asList(source), target);
        options.setSyncFolder(true);

        Configuration configuration = new Configuration();
        configuration.set("falcon.include.path", "*/3/*");
        new FilteredCopyListing(configuration, CREDENTIALS).buildListing(listingPath, options);

        verifyContents(listingPath, 3);
    }

    @Test
    public void testRunQuestionPattern() throws Exception {
        final URI uri = FileSystem.getLocal(new Configuration()).getUri();
        final String pathString = uri.toString();
        Path fileSystemPath = new Path(pathString);
        Path source = new Path(fileSystemPath.toString() + "///tmp/source");
        Path target = new Path(fileSystemPath.toString() + "///tmp/target");
        Path listingPath = new Path(fileSystemPath.toString() + "///tmp/META/fileList.seq");
        DistCpOptions options = new DistCpOptions(Arrays.asList(source), target);
        options.setSyncFolder(true);

        Configuration configuration = new Configuration();
        configuration.set("falcon.include.path", "*/3/?");
        new FilteredCopyListing(configuration, CREDENTIALS).buildListing(listingPath, options);

        verifyContents(listingPath, 2);
    }

    @Test
    public void testRunRangePattern() throws Exception {
        final URI uri = FileSystem.getLocal(new Configuration()).getUri();
        final String pathString = uri.toString();
        Path fileSystemPath = new Path(pathString);
        Path source = new Path(fileSystemPath.toString() + "///tmp/source");
        Path target = new Path(fileSystemPath.toString() + "///tmp/target");
        Path listingPath = new Path(fileSystemPath.toString() + "///tmp/META/fileList.seq");
        DistCpOptions options = new DistCpOptions(Arrays.asList(source), target);
        options.setSyncFolder(true);

        Configuration configuration = new Configuration();
        configuration.set("falcon.include.path", "*/3/[47]");
        new FilteredCopyListing(configuration, CREDENTIALS).buildListing(listingPath, options);

        verifyContents(listingPath, 2);
    }

    @Test
    public void testRunSpecificPattern() throws Exception {
        final URI uri = FileSystem.getLocal(new Configuration()).getUri();
        final String pathString = uri.toString();
        Path fileSystemPath = new Path(pathString);
        Path source = new Path(fileSystemPath.toString() + "///tmp/source");
        Path target = new Path(fileSystemPath.toString() + "///tmp/target");
        Path listingPath = new Path(fileSystemPath.toString() + "///tmp/META/fileList.seq");
        DistCpOptions options = new DistCpOptions(Arrays.asList(source), target);
        options.setSyncFolder(true);

        Configuration configuration = new Configuration();
        configuration.set("falcon.include.path", "*/3/40");
        new FilteredCopyListing(configuration, CREDENTIALS).buildListing(listingPath, options);

        verifyContents(listingPath, 1);
    }

    @Test
    public void testRunListPattern() throws Exception {
        final URI uri = FileSystem.getLocal(new Configuration()).getUri();
        final String pathString = uri.toString();
        Path fileSystemPath = new Path(pathString);
        Path source = new Path(fileSystemPath.toString() + "///tmp/source");
        Path target = new Path(fileSystemPath.toString() + "///tmp/target");
        Path listingPath = new Path(fileSystemPath.toString() + "///tmp/META/fileList.seq");
        DistCpOptions options = new DistCpOptions(Arrays.asList(source), target);
        options.setSyncFolder(true);

        Configuration configuration = new Configuration();
        configuration.set("falcon.include.path", "*/3/{4,7}");
        new FilteredCopyListing(configuration, CREDENTIALS).buildListing(listingPath, options);

        verifyContents(listingPath, 2);
    }

    private void verifyContents(Path listingPath, int expected) throws Exception {
        SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.getLocal(new Configuration()),
                listingPath, new Configuration());
        Text key = new Text();
        FileStatus value = new CopyListingFileStatus();
        Map<String, String> actualValues = new HashMap<String, String>();
        while (reader.next(key, value)) {
            actualValues.put(value.getPath().toString(), key.toString());
        }

        Assert.assertEquals(actualValues.size(), expected == -1 ? EXPECTED_VALUES.size() : expected);
        for (Map.Entry<String, String> entry : actualValues.entrySet()) {
            Assert.assertEquals(entry.getValue(), EXPECTED_VALUES.get(entry.getKey()));
        }
    }
}
