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

package org.apache.falcon.hive.util;


import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.hive.ReplicationEventMetadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hive.hcatalog.api.repl.Command;
import org.apache.hive.hcatalog.api.repl.ReplicationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

/**
 * Utility methods for event sourcer.
 */
public class EventSourcerUtils {

    private static final String METAFILE_EXTENSION = ".meta";
    private static final String SRCFILE_EXTENSION = ".src";
    private static final String TGTFILE_EXTENSION = ".tgt";
    private Path eventsInputDirPath;
    private final boolean shouldKeepHistory;
    private final FileSystem jobFS;

    private static final Logger LOG = LoggerFactory.getLogger(EventSourcerUtils.class);

    public EventSourcerUtils(final Configuration conf, final boolean shouldKeepHistory,
                             final String jobName) throws Exception {
        this.shouldKeepHistory = shouldKeepHistory;
        jobFS = FileSystem.get(conf);
        init(jobName);
    }

    private void init(final String jobName) throws Exception {
        // Create base dir to store events on cluster where job is running
        Path dir = new Path(FileUtils.DEFAULT_EVENT_STORE_PATH);
        // Validate base path
        FileUtils.validatePath(jobFS, new Path(DRStatusStore.BASE_DEFAULT_STORE_PATH));

        if (!jobFS.exists(dir)) {
            if (!FileSystem.mkdirs(jobFS, dir, FileUtils.DEFAULT_DIR_PERMISSION)) {
                throw new IOException("Creating directory failed: " + dir);
            }
        }

        eventsInputDirPath = new Path(FileUtils.DEFAULT_EVENT_STORE_PATH, jobName);

        if (!jobFS.exists(eventsInputDirPath)) {
            if (!jobFS.mkdirs(eventsInputDirPath)) {
                throw new Exception("Creating directory failed: " + eventsInputDirPath);
            }
        }
    }

    public OutputStream getFileOutputStream(final String path) throws Exception {
        return FileSystem.create(jobFS, new Path(path), FileUtils.FS_PERMISSION_700);
    }

    public void closeOutputStream(OutputStream out) throws IOException {
        if (out != null) {
            try {
                out.flush();
            } finally {
                IOUtils.closeQuietly(out);
            }
        }
    }

    public void persistReplicationEvents(final OutputStream out,
                                         final java.lang.Iterable
                                                 <? extends org.apache.hive.hcatalog.api.repl.Command> cmds)
        throws Exception {
        for (Command cmd : cmds) {
            persistReplicationEvents(out, cmd);
        }
    }

    public void persistReplicationEvents(final OutputStream out,
                                         final Command cmd) throws Exception {
        out.write(ReplicationUtils.serializeCommand(cmd).getBytes());
        LOG.debug("HiveDR Serialized Repl Command : {}", cmd);
        out.write(DelimiterUtils.NEWLINE_DELIM.getBytes());
    }

    public String persistToMetaFile(final ReplicationEventMetadata data, final String identifier) throws IOException {
        if (data != null && data.getEventFileMetadata() != null && !data.getEventFileMetadata().isEmpty()) {
            Path metaFilename = new Path(eventsInputDirPath.toString(), identifier + METAFILE_EXTENSION);
            OutputStream out = null;

            try {
                out = FileSystem.create(jobFS, metaFilename, FileUtils.FS_PERMISSION_700);

                for (Map.Entry<String, String> entry : data.getEventFileMetadata().entrySet()) {
                    out.write(entry.getKey().getBytes());
                    out.write(DelimiterUtils.FIELD_DELIM.getBytes());
                    out.write(entry.getValue().getBytes());
                    out.write(DelimiterUtils.NEWLINE_DELIM.getBytes());
                }
                out.flush();
            } finally {
                IOUtils.closeQuietly(out);
            }
            return jobFS.makeQualified(metaFilename).toString();
        } else {
            return null;
        }
    }

    public static void updateEventMetadata(ReplicationEventMetadata data, final String dbName, final String tableName,
                                           final String srcFilename, final String tgtFilename) {
        if (data == null || data.getEventFileMetadata() == null) {
            return;
        }
        StringBuilder key = new StringBuilder();

        if (StringUtils.isNotEmpty(dbName)) {
            key.append(Base64.encodeBase64URLSafeString(dbName.toLowerCase().getBytes()));
        }
        key.append(DelimiterUtils.FIELD_DELIM);
        if (StringUtils.isNotEmpty(tableName)) {
            key.append(Base64.encodeBase64URLSafeString(tableName.toLowerCase().getBytes()));
        }

        StringBuilder value = new StringBuilder();
        if (StringUtils.isNotEmpty(srcFilename)) {
            value.append(srcFilename);
        }
        value.append(DelimiterUtils.FIELD_DELIM);

        if (StringUtils.isNotEmpty(tgtFilename)) {
            value.append(tgtFilename);
        }

        data.getEventFileMetadata().put(key.toString(), value.toString());
    }

    public static void updateEventMetadata(ReplicationEventMetadata data, final ReplicationEventMetadata inputData) {
        if (data == null || data.getEventFileMetadata() == null || inputData == null
                || inputData.getEventFileMetadata() == null || inputData.getEventFileMetadata().isEmpty()) {
            return;
        }

        data.getEventFileMetadata().putAll(inputData.getEventFileMetadata());
    }

    public Path getSrcFileName(final String identifier) {
        return jobFS.makeQualified(new Path(eventsInputDirPath, identifier + SRCFILE_EXTENSION));
    }

    public Path getTargetFileName(final String identifier) {
        return jobFS.makeQualified(new Path(eventsInputDirPath, identifier + TGTFILE_EXTENSION));
    }

    public void cleanUpEventInputDir() {
        if (!shouldKeepHistory) {
            try {
                jobFS.delete(eventsInputDirPath, true);
                eventsInputDirPath = null;
            } catch (IOException e) {
                LOG.error("Unable to cleanup: {}", eventsInputDirPath, e);
            }
        }
    }
}
