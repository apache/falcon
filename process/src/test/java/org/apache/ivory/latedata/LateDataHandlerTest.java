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

package org.apache.ivory.latedata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.*;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.UUID;

public class LateDataHandlerTest {

    private InMemoryWriter writer = new InMemoryWriter(System.out);

    @AfterClass
    public void cleanup() throws Exception {
        Path baseDir = new Path("file://" + getBaseDir().getAbsolutePath());
        FileSystem fs = baseDir.getFileSystem(new Configuration());
        fs.delete(new Path(baseDir, "data"), true);
    }

    @Test
    public void testRecorder1() throws Exception {
        writer.clear();
        Path baseDir = new Path("file://" + getBaseDir().getAbsolutePath());
        FileSystem fs = baseDir.getFileSystem(new Configuration());
        fs.delete(new Path(baseDir, "data"), true);

        long len1 = createTestData(baseDir, "data/feed1");
        Assert.assertEquals(len1, fs.getContentSummary(new Path(baseDir, "data/feed1")).getLength());
        long len2 = createTestData(baseDir, "data/feedhome/feed2");
        Assert.assertEquals(len2, fs.getContentSummary(new Path(baseDir, "data/feedhome/feed2")).getLength());
        long len3 = createTestData(baseDir, "data/feed3");
        Assert.assertEquals(len3, fs.getContentSummary(new Path(baseDir, "data/feed3")).getLength());

        Path outPath = new Path(baseDir, "data/output");
        LateDataHandler.main(new String[]{"record", outPath.toString(),
                new Path(baseDir, "data/feed1").toString(),
                new Path(baseDir, "data/feedhome/feed2").toString(),
                new Path(baseDir, "data/feed3").toString()});
        ByteArrayOutputStream writer = new ByteArrayOutputStream();
        InputStream in = fs.open(outPath);
        IOUtils.copyBytes(in, writer, 1024, true);
        String content = writer.toString();
        String expected = "Path1=" + len1 + "\n" + "Path2=" + len2 + "\n" + "Path3=" + len3 + "\n";
        Assert.assertEquals(content, expected);
    }

    @Test
    public void testInput1() throws Exception {
        writer.clear();

        try {
            LateDataHandler.main(new String[]{});
            Assert.fail("Expected the invocation to fail");
        } catch (IllegalArgumentException ignore) {

        }
    }

    @Test
    public void testInput2() throws Exception {
        writer.clear();

        try {
            LateDataHandler.main(new String[]{"abc", "1", "2"});
            Assert.fail("Expected the invocation to fail");
        } catch (IllegalArgumentException ignore) { }
    }

    @Test
    public void testSuccessDetect() throws Exception {
        writer.clear();

        Path baseDir = new Path("file://" + getBaseDir().getAbsolutePath());
        FileSystem fs = baseDir.getFileSystem(new Configuration());
        fs.delete(new Path(baseDir, "data"), true);

        long len1 = createTestData(baseDir, "data/feed1");
        Assert.assertEquals(len1, fs.getContentSummary(new Path(baseDir, "data/feed1")).getLength());
        long len2 = createTestData(baseDir, "data/feedhome/feed2");
        Assert.assertEquals(len2, fs.getContentSummary(new Path(baseDir, "data/feedhome/feed2")).getLength());
        long len3 = createTestData(baseDir, "data/feed3");
        Assert.assertEquals(len3, fs.getContentSummary(new Path(baseDir, "data/feed3")).getLength());

        Path outPath = new Path(baseDir, "data/output");
        LateDataHandler.main(new String[]{"record", outPath.toString(),
                new Path(baseDir, "data/feed1").toString(),
                new Path(baseDir, "data/feedhome/feed2").toString(),
                new Path(baseDir, "data/feed3").toString()});
        LateDataHandler.stream = writer;
        LateDataHandler.main(new String[]{"detect", outPath.toString(),
                new Path(baseDir, "data/feed1").toString(),
                new Path(baseDir, "data/feedhome/feed2").toString(),
                new Path(baseDir, "data/feed3").toString()});
        Assert.assertEquals(writer.getBuffer(), "changedPaths=");
    }

    @Test
    public void testMorePathsInRecord1() throws Exception {

        writer.clear();
        Path baseDir = new Path("file://" + getBaseDir().getAbsolutePath());
        FileSystem fs = baseDir.getFileSystem(new Configuration());
        fs.delete(new Path(baseDir, "data"), true);

        long len1 = createTestData(baseDir, "data/feed1");
        Assert.assertEquals(len1, fs.getContentSummary(new Path(baseDir, "data/feed1")).getLength());
        long len2 = createTestData(baseDir, "data/feedhome/feed2");
        Assert.assertEquals(len2, fs.getContentSummary(new Path(baseDir, "data/feedhome/feed2")).getLength());
        long len3 = createTestData(baseDir, "data/feed3");
        Assert.assertEquals(len3, fs.getContentSummary(new Path(baseDir, "data/feed3")).getLength());

        Path outPath = new Path(baseDir, "data/output");
        LateDataHandler.main(new String[]{"record", outPath.toString(),
                new Path(baseDir, "data/feed1").toString(),
                new Path(baseDir, "data/feedhome/feed2").toString(),
                new Path(baseDir, "data/feed3").toString()});
        LateDataHandler.stream = writer;
        try {
            LateDataHandler.main(new String[]{"detect", outPath.toString(),
                    new Path(baseDir, "data/feed1").toString(),
                    new Path(baseDir, "data/feed3").toString()});
            Assert.fail("Expected to fail due to insufficient paths either during record or detect phase");
        } catch (NoSuchElementException ignore) { }
    }

    @Test
    public void testMorePathsInDetect1() throws Exception {

        writer.clear();
        Path baseDir = new Path("file://" + getBaseDir().getAbsolutePath());
        FileSystem fs = baseDir.getFileSystem(new Configuration());
        fs.delete(new Path(baseDir, "data"), true);

        long len1 = createTestData(baseDir, "data/feed1");
        Assert.assertEquals(len1, fs.getContentSummary(new Path(baseDir, "data/feed1")).getLength());
        long len2 = createTestData(baseDir, "data/feedhome/feed2");
        Assert.assertEquals(len2, fs.getContentSummary(new Path(baseDir, "data/feedhome/feed2")).getLength());
        long len3 = createTestData(baseDir, "data/feed3");
        Assert.assertEquals(len3, fs.getContentSummary(new Path(baseDir, "data/feed3")).getLength());

        Path outPath = new Path(baseDir, "data/output");
        LateDataHandler.main(new String[]{"record", outPath.toString(),
                new Path(baseDir, "data/feed1").toString(),
                new Path(baseDir, "data/feed3").toString()});
        LateDataHandler.stream = writer;
        try {
            LateDataHandler.main(new String[]{"detect", outPath.toString(),
                    new Path(baseDir, "data/feed1").toString(),
                    new Path(baseDir, "data/feedhome/feed2").toString(),
                    new Path(baseDir, "data/feed3").toString()});
            Assert.fail("Expected to fail due to insufficient paths either during record or detect phase");
        } catch (NotEnoughPathsException ignore) { }
    }

    @Test
    public void testMissingStatsFile() throws Exception {

        writer.clear();
        Path baseDir = new Path("file://" + getBaseDir().getAbsolutePath());
        FileSystem fs = baseDir.getFileSystem(new Configuration());
        fs.delete(new Path(baseDir, "data"), true);

        long len1 = createTestData(baseDir, "data/feed1");
        Assert.assertEquals(len1, fs.getContentSummary(new Path(baseDir, "data/feed1")).getLength());
        long len2 = createTestData(baseDir, "data/feedhome/feed2");
        Assert.assertEquals(len2, fs.getContentSummary(new Path(baseDir, "data/feedhome/feed2")).getLength());
        long len3 = createTestData(baseDir, "data/feed3");
        Assert.assertEquals(len3, fs.getContentSummary(new Path(baseDir, "data/feed3")).getLength());

        Path outPath = new Path(baseDir, "data/output");
        fs.delete(outPath, true);
        LateDataHandler.stream = writer;
        LateDataHandler.main(new String[]{"detect", outPath.toString(),
                new Path(baseDir, "data/feed1").toString(),
                new Path(baseDir, "data/feedhome/feed2").toString(),
                new Path(baseDir, "data/feed3").toString()});
        Assert.assertEquals(writer.getBuffer(), "changedPaths=INVALID");
    }

    @Test
    public void testDetectOneChange() throws Exception {

        writer.clear();
        Path baseDir = new Path("file://" + getBaseDir().getAbsolutePath());
        FileSystem fs = baseDir.getFileSystem(new Configuration());
        fs.delete(new Path(baseDir, "data"), true);

        long len1 = createTestData(baseDir, "data/feed1");
        Assert.assertEquals(len1, fs.getContentSummary(new Path(baseDir, "data/feed1")).getLength());
        long len2 = createTestData(baseDir, "data/feedhome/feed2");
        Assert.assertEquals(len2, fs.getContentSummary(new Path(baseDir, "data/feedhome/feed2")).getLength());
        long len3 = createTestData(baseDir, "data/feed3");
        Assert.assertEquals(len3, fs.getContentSummary(new Path(baseDir, "data/feed3")).getLength());

        LateDataHandler.stream = writer;
        Path outPath = new Path(baseDir, "data/output");
        LateDataHandler.main(new String[]{"record", outPath.toString(),
                new Path(baseDir, "data/feed1").toString(),
                new Path(baseDir, "data/feedhome/feed2").toString(),
                new Path(baseDir, "data/feed3").toString()});
        createFiles(new Path(baseDir, "data/feed1"));
        LateDataHandler.main(new String[]{"detect", outPath.toString(),
                new Path(baseDir, "data/feed1").toString(),
                new Path(baseDir, "data/feedhome/feed2").toString(),
                new Path(baseDir, "data/feed3").toString()});
        Assert.assertEquals(writer.getBuffer(), "changedPaths=Path1");
    }

    @Test
    public void testDetectAllChanges() throws Exception {

        writer.clear();
        Path baseDir = new Path("file://" + getBaseDir().getAbsolutePath());
        FileSystem fs = baseDir.getFileSystem(new Configuration());
        fs.delete(new Path(baseDir, "data"), true);

        long len1 = createTestData(baseDir, "data/feed1");
        Assert.assertEquals(len1, fs.getContentSummary(new Path(baseDir, "data/feed1")).getLength());
        long len2 = createTestData(baseDir, "data/feedhome/feed2");
        Assert.assertEquals(len2, fs.getContentSummary(new Path(baseDir, "data/feedhome/feed2")).getLength());
        long len3 = createTestData(baseDir, "data/feed3");
        Assert.assertEquals(len3, fs.getContentSummary(new Path(baseDir, "data/feed3")).getLength());

        LateDataHandler.stream = writer;
        Path outPath = new Path(baseDir, "data/output");
        LateDataHandler.main(new String[]{"record", outPath.toString(),
                new Path(baseDir, "data/feed1").toString(),
                new Path(baseDir, "data/feedhome/feed2").toString(),
                new Path(baseDir, "data/feed3").toString()});
        createFiles(new Path(baseDir, "data/feed1"));
        createFiles(new Path(baseDir, "data/feedhome/feed2"));
        createFiles(new Path(baseDir, "data/feed3"));
        LateDataHandler.main(new String[]{"detect", outPath.toString(),
                new Path(baseDir, "data/feed1").toString(),
                new Path(baseDir, "data/feedhome/feed2").toString(),
                new Path(baseDir, "data/feed3").toString()});
        Assert.assertEquals(writer.getBuffer(), "changedPaths=Path1,Path2,Path3");
    }

    private File getBaseDir() throws Exception {
        File target = new File("process/target");
        if (!target.exists()) {
            target = new File("target");
        }
        if (!target.exists()) throw new Exception("No target");
        return target;
    }

    private static final Random random = new Random();

    public static long createTestData(Path baseDir, String child) throws Exception {
        Path path = new Path(baseDir, child);
        FileSystem fs = path.getFileSystem(new Configuration());
        fs.delete(path, true);
        fs.mkdirs(path);
        if (random.nextBoolean()) {
            return createTestData(path, UUID.randomUUID().toString());
        } else {
            return createFiles(path);
        }
    }

    public static long createFiles(Path path) throws Exception {
        int files = random.nextInt(5) + 1;

        long sum = 0;
        FileSystem fs = path.getFileSystem(new Configuration());
        for (int file = 0; file < files; file++) {
            int len = random.nextInt(100);
            byte[] arr = new byte[len];
            OutputStream out = fs.create(new Path(path, UUID.randomUUID().toString()));
            out.write(arr);
            out.close();
            sum += len;
        }
        return sum;
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
