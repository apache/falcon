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

import org.apache.falcon.hive.HiveDRArgs;
import org.apache.falcon.hive.util.EventUtils;
import org.apache.falcon.hive.util.HiveDRUtils;
import org.apache.falcon.hive.util.ReplicationStatus;
import org.apache.falcon.job.ReplicationJobCountersList;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Map class for Hive DR.
 */
public class CopyMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final Logger LOG = LoggerFactory.getLogger(CopyMapper.class);
    private EventUtils eventUtils;
    private ScheduledThreadPoolExecutor timer;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        eventUtils = new EventUtils(context.getConfiguration());
        eventUtils.initializeFS();
        try {
            eventUtils.setupConnection();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    protected void map(LongWritable key, Text value,
                       final Context context) throws IOException, InterruptedException {
        LOG.debug("Processing Event value: {}", value.toString());
        timer = new ScheduledThreadPoolExecutor(1);
        timer.scheduleAtFixedRate(new Runnable() {
            public void run() {
                System.out.println("Hive DR copy mapper progress heart beat");
                context.progress();
            }
        }, 0, 30, TimeUnit.SECONDS);
        try {
            eventUtils.processEvents(value.toString());
        } catch (Exception e) {
            LOG.error("Exception in processing events:", e);
            throw new IOException(e);
        } finally {
            timer.shutdownNow();
            cleanup(context);
        }
        List<ReplicationStatus> replicationStatusList = eventUtils.getListReplicationStatus();
        if (replicationStatusList != null && !replicationStatusList.isEmpty()) {
            for (ReplicationStatus rs : replicationStatusList) {
                context.write(new Text(rs.getJobName()), new Text(rs.toString()));
            }
        }

        // In case of export stage, populate custom counters
        if (context.getConfiguration().get(HiveDRArgs.EXECUTION_STAGE.getName())
                .equalsIgnoreCase(HiveDRUtils.ExecutionStage.EXPORT.name())
                && !eventUtils.isCountersMapEmpty()) {
            context.getCounter(ReplicationJobCountersList.BYTESCOPIED).increment(
                    eventUtils.getCounterValue(ReplicationJobCountersList.BYTESCOPIED.getName()));
            context.getCounter(ReplicationJobCountersList.COPY).increment(
                    eventUtils.getCounterValue(ReplicationJobCountersList.COPY.getName()));
        }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        LOG.info("Invoking cleanup process");
        super.cleanup(context);
        try {
            if (context.getConfiguration().get(HiveDRArgs.EXECUTION_STAGE.getName())
                    .equalsIgnoreCase(HiveDRUtils.ExecutionStage.IMPORT.name())) {
                eventUtils.cleanEventsDirectory();
            }
        } catch (IOException e) {
            LOG.error("Cleaning up of events directories failed", e);
        } finally {
            try {
                eventUtils.closeConnection();
            } catch (SQLException e) {
                LOG.error("Closing the connections failed", e);
            }
        }
    }
}
