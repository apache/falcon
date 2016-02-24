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

package org.apache.falcon.adfservice;

import org.apache.falcon.FalconException;

/**
 * Azure ADB Job factory to generate ADFJob for each job type.
 */
public final class ADFJobFactory {
    public static ADFJob buildADFJob(String msg, String id) throws FalconException {
        ADFJob.JobType jobType = ADFJob.getJobType(msg);
        switch (jobType) {
        case REPLICATION:
            return new ADFReplicationJob(msg, id);
        case HIVE:
            return new ADFHiveJob(msg, id);
        case PIG:
            return new ADFPigJob(msg, id);
        default:
            throw new FalconException("Invalid job type: " + jobType.toString());
        }
    }

    private ADFJobFactory() {
    }
}
