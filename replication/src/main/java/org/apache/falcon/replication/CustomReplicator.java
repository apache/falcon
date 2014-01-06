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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * A custom implementation of DistCp that overrides the behavior of CopyListing
 * interface to copy EntityUtil.SUCCEEDED_FILE_NAME last so downstream apps
 * depending on data availability will work correctly.
 */
public class CustomReplicator extends DistCp {

    private static final Logger LOG = Logger.getLogger(CustomReplicator.class);

    /**
     * Public Constructor. Creates DistCp object with specified input-parameters.
     * (E.g. source-paths, target-location, etc.)
     *
     * @param inputOptions:  Options (indicating source-paths, target-location.)
     * @param configuration: The Hadoop configuration against which the Copy-mapper must run.
     * @throws Exception, on failure.
     */
    public CustomReplicator(Configuration configuration, DistCpOptions inputOptions) throws Exception {
        super(configuration, inputOptions);
    }

    @Override
    protected Path createInputFileListing(Job job) throws IOException {
        Path fileListingPath = getFileListingPath();
        FilteredCopyListing copyListing = new FilteredCopyListing(job.getConfiguration(),
                job.getCredentials());
        copyListing.buildListing(fileListingPath, inputOptions);
        LOG.info("Number of paths considered for copy: " + copyListing.getNumberOfPaths());
        LOG.info("Number of bytes considered for copy: " + copyListing.getBytesToCopy()
                + " (Actual number of bytes copied depends on whether any files are "
                + "skipped or overwritten.)");
        return fileListingPath;
    }
}
