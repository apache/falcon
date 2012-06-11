package org.apache.ivory.replication;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.log4j.Logger;

import java.io.IOException;

public class CustomReplicator extends DistCp {

    private static Logger LOG = Logger.getLogger(CustomReplicator.class);
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
