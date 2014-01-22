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
package org.apache.hadoop.mapred;

import org.apache.falcon.JobTrackerService;
import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.Test;

/**
 * Test for LocalRunner.
 */
@Test   (enabled = false)
public class LocalRunnerTest {

    @SuppressWarnings("unchecked")
    public void testLocalRunner() throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.job.tracker", "localhost:41021");
        conf.set("mapreduce.framework.name", "unittests");
        String hadoopProfle = System.getProperty("hadoop.profile", "1");
        if (hadoopProfle.equals("1")) {
            String className = "org.apache.hadoop.mapred.LocalRunnerV1";
            Class<? extends JobTrackerService> runner =
                    (Class<? extends JobTrackerService>) Class.forName(className);
            JobTrackerService service = runner.newInstance();
            service.start();
        }
        JobClient client = new JobClient(new JobConf(conf));
        System.out.println(client.getSystemDir());
    }
}
