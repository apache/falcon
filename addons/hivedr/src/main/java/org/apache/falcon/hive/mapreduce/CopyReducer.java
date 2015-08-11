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
import org.apache.falcon.hive.exception.HiveReplicationException;
import org.apache.falcon.hive.util.DRStatusStore;
import org.apache.falcon.hive.util.FileUtils;
import org.apache.falcon.hive.util.HiveDRStatusStore;
import org.apache.falcon.hive.util.ReplicationStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Reducer class for Hive DR.
 */
public class CopyReducer extends Reducer<Text, Text, Text, Text> {
    private DRStatusStore hiveDRStore;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        FileSystem fs= FileSystem.get(FileUtils.getConfiguration(
                conf.get(HiveDRArgs.TARGET_NN.getName()),
                conf.get(HiveDRArgs.TARGET_NN_KERBEROS_PRINCIPAL.getName())));
        hiveDRStore = new HiveDRStatusStore(fs);
    }

    private List<ReplicationStatus> sortStatusList(List<ReplicationStatus> replStatusList) {
        Collections.sort(replStatusList, new Comparator<ReplicationStatus>() {
            @Override
            public int compare(ReplicationStatus r1, ReplicationStatus r2) {
                return (int) (r1.getEventId() - r2.getEventId());
            }
        });
        return replStatusList;
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<ReplicationStatus> replStatusList = new ArrayList<ReplicationStatus>();
        ReplicationStatus rs;
        try {
            for (Text value : values) {
                String[] fields = (value.toString()).split("\t");
                rs = new ReplicationStatus(fields[0], fields[1], fields[2], fields[3], fields[4],
                        ReplicationStatus.Status.valueOf(fields[5]), Long.parseLong(fields[6]));
                replStatusList.add(rs);
            }

            hiveDRStore.updateReplicationStatus(key.toString(), sortStatusList(replStatusList));
        } catch (HiveReplicationException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}
