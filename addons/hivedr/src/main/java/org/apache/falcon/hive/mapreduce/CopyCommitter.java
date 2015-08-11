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

package org.apache.falcon.hive.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Copy committer class.
 */
public class CopyCommitter extends FileOutputCommitter {

    private static final Logger LOG = LoggerFactory.getLogger(CopyCommitter.class);

    /**
     * Create a file output committer.
     *
     * @param outputPath the job's output path, or null if you want the output
     *                   committer to act as a noop.
     * @param context    the task's context
     * @throws java.io.IOException
     */
    public CopyCommitter(Path outputPath,
                         TaskAttemptContext context) throws IOException {
        super(outputPath, context);
    }

    @Override
    public void commitJob(JobContext jobContext) throws IOException {
        Configuration conf = jobContext.getConfiguration();

        try {
            super.commitJob(jobContext);
        } finally {
            cleanup(conf);
        }
    }

    private void cleanup(Configuration conf) {
        // clean up staging and other data
    }
}
