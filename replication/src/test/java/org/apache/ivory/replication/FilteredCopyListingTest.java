package org.apache.ivory.replication;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hadoop.tools.util.DistCpUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.DataOutputStream;
import java.net.URI;
import java.util.*;

public class FilteredCopyListingTest {

    private static final Credentials CREDENTIALS = new Credentials();

    public static Map<String, String> expectedValues = new HashMap<String, String>();

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
        }
        finally {
            IOUtils.cleanup(null, fileSystem);
        }
    }

    private static void rmdirs(String path) throws Exception {
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.getLocal(new Configuration());
            fileSystem.delete(new Path(path), true);
        }
        finally {
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
        }
        finally {
            IOUtils.cleanup(null, fileSystem, outputStream);
        }
    }

    private static void recordInExpectedValues(String path) throws Exception {
        FileSystem fileSystem = FileSystem.getLocal(new Configuration());
        Path sourcePath = new Path(fileSystem.getUri().toString() + path);
        expectedValues.put(sourcePath.toString(), DistCpUtils.getRelativePath(
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

        Configuration configuration = new Configuration();
        configuration.set("ivory.include.path", "*/3/*");
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

        Configuration configuration = new Configuration();
        configuration.set("ivory.include.path", "*/3/?");
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

        Configuration configuration = new Configuration();
        configuration.set("ivory.include.path", "*/3/[47]");
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

        Configuration configuration = new Configuration();
        configuration.set("ivory.include.path", "*/3/40");
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

        Configuration configuration = new Configuration();
        configuration.set("ivory.include.path", "*/3/{4,7}");
        new FilteredCopyListing(configuration, CREDENTIALS).buildListing(listingPath, options);

        verifyContents(listingPath, 2);
    }

    private void verifyContents(Path listingPath, int expected) throws Exception {
        SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.getLocal(new Configuration()),
                listingPath, new Configuration());
        Text key   = new Text();
        FileStatus value = new FileStatus();
        Map<String, String> actualValues = new HashMap<String, String>();
        while (reader.next(key, value)) {
            actualValues.put(value.getPath().toString(), key.toString());
        }

        Assert.assertEquals(expected == -1 ? expectedValues.size() : expected, actualValues.size());
        for (Map.Entry<String, String> entry : actualValues.entrySet()) {
            Assert.assertEquals(entry.getValue(), expectedValues.get(entry.getKey()));
        }
    }
}
