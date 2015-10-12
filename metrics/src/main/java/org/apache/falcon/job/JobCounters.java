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

package org.apache.falcon.job;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.tools.mapred.CopyMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Job Counters abstract class to be extended by supported job type.
 */
public abstract class JobCounters {
    private static final Logger LOG = LoggerFactory.getLogger(JobCounters.class);
    protected Map<String, Long> countersMap = null;

    public JobCounters() {
        countersMap = new HashMap<String, Long>();
    }

    public void obtainJobCounters(Configuration conf, Job job, boolean isDistCp) throws IOException {
        try {
            long timeTaken = job.getFinishTime() - job.getStartTime();
            countersMap.put(ReplicationJobCountersList.TIMETAKEN.getName(), timeTaken);
            Counters jobCounters = job.getCounters();
            parseJob(job, jobCounters, isDistCp);
        } catch (Exception e) {
            LOG.info("Exception occurred while obtaining job counters: {}", e);
        }
    }

    protected void populateReplicationCountersMap(Counters jobCounters) {
        for(CopyMapper.Counter copyCounterVal : CopyMapper.Counter.values()) {
            if (ReplicationJobCountersList.getCountersKey(copyCounterVal.name()) != null) {
                Counter counter = jobCounters.findCounter(copyCounterVal);
                if (counter != null) {
                    String counterName = counter.getName();
                    long counterValue = counter.getValue();
                    countersMap.put(counterName, counterValue);
                }
            }
        }
    }

    public void storeJobCounters(Configuration conf, Path counterFile) throws IOException {
        FileSystem sourceFs = FileSystem.get(conf);
        OutputStream out = null;
        try {
            out = sourceFs.create(counterFile);
            for (Map.Entry<String, Long> counter : countersMap.entrySet()) {
                out.write((counter.getKey() + ":" + counter.getValue()).getBytes());
                out.write("\n".getBytes());
            }
            out.flush();
        } finally {
            IOUtils.closeQuietly(out);
        }
    }

    public Map<String, Long> getCountersMap() {
        return countersMap;
    }

    protected abstract void parseJob(Job job, Counters jobCounters, boolean isDistCp) throws IOException;
}
