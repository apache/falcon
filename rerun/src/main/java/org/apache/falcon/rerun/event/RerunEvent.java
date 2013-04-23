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

import java.util.Date;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * Event representing a rerun.
 */
public class RerunEvent implements Delayed {

    protected static final String SEP = "*";

    /**
     * Rerun Event type.
     */
    public enum RerunType {
        RETRY, LATE
    }

    protected String clusterName;
    protected String wfId;
    protected long msgInsertTime;
    protected long delayInMilliSec;
    protected String entityType;
    protected String entityName;
    protected String instance;
    protected int runId;

    //SUSPEND CHECKSTYLE CHECK VisibilityModifierCheck
    public RerunEvent(String clusterName, String wfId, long msgInsertTime, long delay,
                      String entityType, String entityName, String instance, int runId) {
        this.clusterName = clusterName;
        this.wfId = wfId;
        this.msgInsertTime = msgInsertTime;
        this.delayInMilliSec = delay;
        this.entityName = entityName;
        this.instance = instance;
        this.runId = runId;
        this.entityType = entityType;
    }
    //RESUME CHECKSTYLE CHECK VisibilityModifierCheck

    public String getClusterName() {
        return clusterName;
    }

    public String getWfId() {
        return wfId;
    }

    public long getDelayInMilliSec() {
        return delayInMilliSec;
    }

    public String getEntityName() {
        return entityName;
    }

    public String getInstance() {
        return instance;
    }

    public int getRunId() {
        return runId;
    }

    public String getEntityType() {
        return entityType;
    }

    @Override
    public int compareTo(Delayed o) {
        RerunEvent event = (RerunEvent) o;
        return new Date(msgInsertTime + delayInMilliSec).
                compareTo(new Date(event.msgInsertTime + event.delayInMilliSec));
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return unit.convert((msgInsertTime - System.currentTimeMillis())
                + delayInMilliSec, TimeUnit.MILLISECONDS);
    }

    public long getMsgInsertTime() {
        return msgInsertTime;
    }

    public void setMsgInsertTime(long msgInsertTime) {
        this.msgInsertTime = msgInsertTime;
    }

    public RerunType getType() {
        if (this instanceof RetryEvent) {
            return RerunType.RETRY;
        } else if (this instanceof LaterunEvent) {
            return RerunType.LATE;
        } else {
            return null;
        }
    }

}
