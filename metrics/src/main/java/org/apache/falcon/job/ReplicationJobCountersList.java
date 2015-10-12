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

/**
 * List of counters for replication job.
 */
public enum ReplicationJobCountersList {
    TIMETAKEN("TIMETAKEN", "time taken by the distcp job"),
    BYTESCOPIED("BYTESCOPIED", "number of bytes copied"),
    COPY("COPY", "number of files copied");

    private final String name;
    private final String description;

    ReplicationJobCountersList(String name, String description) {
        this.name = name;
        this.description = description;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public static ReplicationJobCountersList getCountersKey(String counterKey) {
        if (counterKey != null) {
            for (ReplicationJobCountersList value : ReplicationJobCountersList.values()) {
                if (counterKey.equals(value.getName())) {
                    return value;
                }
            }
        }

        return null;
    }

    @Override
    public String toString() {
        return getName();
    }
}
