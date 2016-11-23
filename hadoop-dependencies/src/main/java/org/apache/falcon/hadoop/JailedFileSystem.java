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
package org.apache.falcon.hadoop;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.net.URI;

/**
 * chroot local file system for tests.
 */
public class JailedFileSystem extends FileSystem {
    private URI uri;
    private String basePath;
    private LocalFileSystem localFS;
    private Path workingDir;

    public JailedFileSystem() {
        localFS = new LocalFileSystem();
        this.workingDir = new Path("/user", System.getProperty("user.name"));
    }

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        super.initialize(name, conf);
        setConf(conf);
        localFS.initialize(LocalFileSystem.getDefaultUri(conf), conf);
        String base = name.getHost();
        if (base == null) {
            throw new IOException("Incomplete Jail URI, no jail base: "+ name);
        }
        basePath = new Path(conf.get("jail.base", System.getProperty("hadoop.tmp.dir",
                        System.getProperty("user.dir") + "/target/falcon/tmp-hadoop-"
                                + System.getProperty("user.name"))) + "/jail-fs/" + base).toUri().getPath();
        this.uri = URI.create(name.getScheme()+"://"+name.getAuthority());
    }

    @Override
    public URI getUri() {
        return uri;
    }

    private Path toLocalPath(Path f) {
        if (!f.isAbsolute()) {
            f = new Path(getWorkingDirectory(), f);
        }
        return new Path(basePath + f.toUri().getPath());
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        return localFS.open(toLocalPath(f), bufferSize);
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize,
                                     short replication, long blockSize, Progressable progress) throws IOException {
        return localFS.create(toLocalPath(f), permission, overwrite, bufferSize,
                replication, blockSize, progress);
    }

    @Override
    public FSDataOutputStream create(Path f) throws IOException {
        return localFS.create(toLocalPath(f));
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
        return localFS.append(toLocalPath(f), bufferSize, progress);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        return localFS.rename(toLocalPath(src), toLocalPath(dst));
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        Path localPath = toLocalPath(f);
        if (localPath.toUri().getPath().trim().equals("/")) {
            throw new AssertionError("Attempting to delete root " + localPath);
        }

        return localFS.delete(localPath, recursive);
    }

    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
        FileStatus[] fileStatuses = localFS.listStatus(toLocalPath(f));
        if (fileStatuses == null || fileStatuses.length == 0) {
            return fileStatuses;
        } else {
            FileStatus[] jailFileStatuses = new FileStatus[fileStatuses.length];
            for (int index = 0; index < fileStatuses.length; index++) {
                FileStatus status = fileStatuses[index];
                jailFileStatuses[index] = new FileStatus(status.getLen(), status.isDirectory(),
                        status.getReplication(), status.getBlockSize(), status.getModificationTime(),
                        status.getAccessTime(), status.getPermission(), status.getOwner(), status.getGroup(),
                        fromLocalPath(status.getPath())
                                .makeQualified(this.getUri(), this.getWorkingDirectory()));
            }
            return jailFileStatuses;
        }
    }

    @Override
    public void setWorkingDirectory(Path newDir) {
        if (newDir != null) {
            workingDir = makeAbsolute(newDir);
        }
    }

    private Path makeAbsolute(Path path) {
        if (path.isAbsolute()) {
            return path;
        }
        return new Path(workingDir, path);
    }

    @Override
    public Path getWorkingDirectory() {
        return workingDir;
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        return localFS.mkdirs(toLocalPath(f), permission);
    }

    @Override
    public void setPermission(Path p, FsPermission permission) throws IOException {
        localFS.setPermission(toLocalPath(p), permission);
    }

    @Override
    public FileChecksum getFileChecksum(Path f) throws IOException {
        final byte[] md5 = DigestUtils.md5(FileUtils.readFileToByteArray(new File(toLocalPath(f).toString())));
        return new FileChecksum() {

            @Override
            public String getAlgorithmName() {
                return "MD5";
            }

            @Override
            public int getLength() {
                return md5.length;
            }

            @Override
            public byte[] getBytes() {
                return md5;
            }

            @Override
            public void write(DataOutput out) throws IOException {
            }

            @Override
            public void readFields(DataInput in) throws IOException {
            }
        };
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        FileStatus status = localFS.getFileStatus(toLocalPath(f));
        if (status == null) {
            return null;
        }
        return new FileStatus(status.getLen(), status.isDirectory(),
                status.getReplication(), status.getBlockSize(), status.getModificationTime(),
                status.getAccessTime(), status.getPermission(), status.getOwner(), status.getGroup(),
                fromLocalPath(status.getPath()).makeQualified(this.getUri(), this.getWorkingDirectory()));
    }

    private Path fromLocalPath(Path path) {
        String pathString = path.toUri().getPath().replaceFirst(basePath, "");
        return new Path(pathString.isEmpty() ? "/" : pathString);
    }

    @Override
    public void setTimes(Path p, long mtime, long atime) throws IOException {
        super.setTimes(p, mtime, atime);
    }

    @Override
    public void close() throws IOException {
        localFS.close();
    }
}
