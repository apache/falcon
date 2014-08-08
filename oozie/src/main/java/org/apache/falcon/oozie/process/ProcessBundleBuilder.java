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

package org.apache.falcon.oozie.process;

import org.apache.falcon.FalconException;
import org.apache.falcon.Tag;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency.TimeUnit;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.oozie.OozieBundleBuilder;
import org.apache.falcon.oozie.OozieCoordinatorBuilder;
import org.apache.falcon.update.UpdateHelper;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.CoordinatorJob.Timeunit;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Builds oozie bundle for process - schedulable entity in oozie.
 */
public class ProcessBundleBuilder extends OozieBundleBuilder<Process> {

    public ProcessBundleBuilder(Process entity) {
        super(entity);
    }

    private Properties getAdditionalProperties(Cluster cluster) throws FalconException {
        Properties properties = new Properties();

        //Properties for optional inputs
        if (entity.getInputs() != null) {
            for (Input in : entity.getInputs().getInputs()) {
                if (in.isOptional()) {
                    Feed feed = EntityUtil.getEntity(EntityType.FEED, in.getFeed());
                    org.apache.falcon.entity.v0.feed.Cluster feedCluster =
                        FeedHelper.getCluster(feed, cluster.getName());
                    String inName = in.getName();
                    properties.put(inName + ".frequency", String.valueOf(feed.getFrequency().getFrequency()));
                    properties.put(inName + ".freq_timeunit",
                        mapToCoordTimeUnit(feed.getFrequency().getTimeUnit()).name());
                    properties.put(inName + ".timezone", feed.getTimezone().getID());
                    properties.put(inName + ".end_of_duration", Timeunit.NONE.name());
                    properties.put(inName + ".initial-instance",
                        SchemaHelper.formatDateUTC(feedCluster.getValidity().getStart()));
                    properties.put(inName + ".done-flag", "notused");

                    String locPath = FeedHelper.createStorage(cluster.getName(), feed)
                        .getUriTemplate(LocationType.DATA).replace('$', '%');
                    properties.put(inName + ".uri-template", locPath);

                    properties.put(inName + ".start-instance", in.getStart());
                    properties.put(inName + ".end-instance", in.getEnd());
                }
            }
        }
        return  properties;
    }

    private Timeunit mapToCoordTimeUnit(TimeUnit tu) {
        switch (tu) {
        case days:
            return Timeunit.DAY;

        case hours:
            return Timeunit.HOUR;

        case minutes:
            return Timeunit.MINUTE;

        case months:
            return Timeunit.MONTH;

        default:
            throw new IllegalArgumentException("Unhandled time unit " + tu);
        }
    }

    @Override protected List<Properties> buildCoords(Cluster cluster, Path buildPath) throws FalconException {
        copyUserWorkflow(cluster, buildPath);

        List<Properties> props = OozieCoordinatorBuilder.get(entity, Tag.DEFAULT).buildCoords(cluster, buildPath);
        if (props != null) {
            assert props.size() == 1 : "Process should have only 1 coord";
            props.get(0).putAll(getAdditionalProperties(cluster));
        }

        return props;
    }

    private void copyUserWorkflow(Cluster cluster, Path buildPath) throws FalconException {
        try {
            FileSystem fs = HadoopClientFactory.get().createFileSystem(ClusterHelper.getConfiguration(cluster));

            //Copy user workflow and lib to staging dir
            Map<String, String> checksums = UpdateHelper.checksumAndCopy(fs, new Path(entity.getWorkflow().getPath()),
                new Path(buildPath, EntityUtil.PROCESS_USER_DIR));
            if (entity.getWorkflow().getLib() != null && fs.exists(new Path(entity.getWorkflow().getLib()))) {
                checksums.putAll(UpdateHelper.checksumAndCopy(fs, new Path(entity.getWorkflow().getLib()),
                    new Path(buildPath, EntityUtil.PROCESS_USERLIB_DIR)));
            }

            writeChecksums(fs, new Path(buildPath, EntityUtil.PROCESS_CHECKSUM_FILE), checksums);
        } catch (IOException e) {
            throw new FalconException("Failed to copy user workflow/lib", e);
        }
    }

    private void writeChecksums(FileSystem fs, Path path, Map<String, String> checksums) throws FalconException {
        try {
            FSDataOutputStream stream = fs.create(path);
            try {
                for (Map.Entry<String, String> entry : checksums.entrySet()) {
                    stream.write((entry.getKey() + "=" + entry.getValue() + "\n").getBytes());
                }
            } finally {
                stream.close();
            }
        } catch (IOException e) {
            throw new FalconException("Failed to copy user workflow/lib", e);
        }
    }
}
