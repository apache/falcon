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
package org.apache.falcon.rerun.event;

import org.apache.falcon.rerun.event.RerunEvent.RerunType;

import java.util.HashMap;
import java.util.Map;

/**
 * Factory implementation to dole out specific implementations of RerunEvent.
 *
 * @param <T>
 */
public class RerunEventFactory<T extends RerunEvent> {

    public T getRerunEvent(String type, String line) {
        if (type.startsWith(RerunType.RETRY.name())) {
            return retryEventFromString(line);
        } else if (type.startsWith(RerunType.LATE.name())) {
            return lateEventFromString(line);
        } else {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private T lateEventFromString(String line) {
        Map<String, String> map = getMap(line);
        return (T) new LaterunEvent(map.get("clusterName"), map.get("wfId"), map.get("parentId"),
                Long.parseLong(map.get("msgInsertTime")), Long.parseLong(map.get("delayInMilliSec")),
                map.get("entityType"), map.get("entityName"), map.get("instance"),
                Integer.parseInt(map.get("runId")), map.get("workflowUser"));
    }

    @SuppressWarnings("unchecked")
    public T retryEventFromString(String line) {
        Map<String, String> map = getMap(line);
        return (T) new RetryEvent(map.get("clusterName"), map.get("wfId"), map.get("parentId"),
                Long.parseLong(map.get("msgInsertTime")), Long.parseLong(map.get("delayInMilliSec")),
                map.get("entityType"), map.get("entityName"), map.get("instance"),
                Integer.parseInt(map.get("runId")), Integer.parseInt(map.get("attempts")),
                Integer.parseInt(map.get("failRetryCount")), map.get("workflowUser"));
    }

    private Map<String, String> getMap(String message) {
        String[] items = message.split("\\" + RerunEvent.SEP);
        Map<String, String> map = new HashMap<String, String>();
        for (String item : items) {
            String[] pair = item.split("=");
            map.put(pair[0], pair[1]);
        }
        return map;
    }
}
