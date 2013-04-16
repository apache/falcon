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

package org.apache.falcon.service;

import org.apache.falcon.FalconException;
import org.apache.falcon.aspect.GenericAlert;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.workflow.WorkflowEngineFactory;
import org.apache.falcon.workflow.engine.WorkflowEngineActionListener;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class SLAMonitoringService implements FalconService, WorkflowEngineActionListener {
    private static final Logger LOG = Logger.getLogger(SLAMonitoringService.class);
    public static final String SERVICE_NAME = "SLAMonitor";

    private ConcurrentMap<String, Long> monitoredEntities =
            new ConcurrentHashMap<String, Long>();

    private ConcurrentMap<String, ConcurrentMap<Date, Date>> pendingJobs =
            new ConcurrentHashMap<String, ConcurrentMap<Date, Date>>();

    private static final long INITIAL_LATENCY_SECS = 12 * 3600;

    private static final long POLL_PERIODICITY_SECS = 300;

    @Override
    public String getName() {
        return SERVICE_NAME;
    }

    @Override
    public void init() throws FalconException {
        WorkflowEngineFactory.getWorkflowEngine().registerListener(this);
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        executor.scheduleWithFixedDelay(new Monitor(), POLL_PERIODICITY_SECS, POLL_PERIODICITY_SECS, TimeUnit.SECONDS);
    }

    @Override
    public void destroy() throws FalconException {
    }

    @Override
    public void afterSchedule(Entity entity, String cluster) throws FalconException {
        addEntityForMonitoring(entity, cluster);
    }

    @Override
    public void afterDelete(Entity entity, String cluster) throws FalconException {
        removeMonitoredEntity(entity, cluster);
    }

    @Override
    public void afterSuspend(Entity entity, String cluster) throws FalconException {
        removeMonitoredEntity(entity, cluster);
    }

    @Override
    public void afterResume(Entity entity, String cluster) throws FalconException {
        addEntityForMonitoring(entity, cluster);
    }

    public void notifyCompletion(Entity entity, String cluster, Date nominalTime, long duration) {
        if (!isEntityMonitored(entity, cluster)) {
            addEntityForMonitoring(entity, cluster);
        }
        updateLatency(entity, cluster, duration);
        removeFromPendingList(entity, cluster, nominalTime);
    }

    private String getKey(Entity entity, String cluster) {
        return entity.toShortString() + "/" + cluster;
    }

    private void addEntityForMonitoring(Entity entity, String cluster) {
        monitoredEntities.putIfAbsent(getKey(entity, cluster), INITIAL_LATENCY_SECS);
    }

    private void removeMonitoredEntity(Entity entity, String cluster) {
        monitoredEntities.remove(getKey(entity, cluster));
        pendingJobs.remove(getKey(entity, cluster));
    }

    private boolean isEntityMonitored(Entity entity, String cluster) {
        return monitoredEntities.containsKey(getKey(entity, cluster));
    }

    private void updateLatency(Entity entity, String cluster, long duration) {
        long newLatency = (duration + monitoredEntities.get(getKey(entity, cluster))) / 2;
        monitoredEntities.put(getKey(entity, cluster), newLatency);
    }

    private void removeFromPendingList(Entity entity, String cluster, Date nominalTime) {
        ConcurrentMap<Date, Date> pendingInstances = pendingJobs.get(getKey(entity, cluster));
        if (pendingInstances != null) {
            LOG.debug("Removing from pending jobs: " + getKey(entity, cluster) + " ---> " +
                    SchemaHelper.formatDateUTC(nominalTime));
            pendingInstances.remove(nominalTime);
        }
    }

    private class Monitor implements Runnable {

        @Override
        public void run() {
            try {
                if (monitoredEntities.isEmpty()) {
                    return;
                }
                Set<String> keys = new HashSet<String>(monitoredEntities.keySet());
                checkSLAMissOnPendingEntities(keys);
                addNewPendingEntities(keys);
            } catch (Throwable e) {
                LOG.error("Monitor failed: ", e);
            }
        }

        private void checkSLAMissOnPendingEntities(Set<String> keys) throws FalconException {
            Date now = new Date();
            for (String key : keys) {
                ConcurrentMap<Date, Date> pendingInstances = pendingJobs.get(key);
                if (pendingInstances == null) {
                    continue;
                }
                ConcurrentMap<Date, Date> interim =
                        new ConcurrentHashMap<Date, Date>(pendingInstances);
                for (Map.Entry<Date, Date> entry : interim.entrySet()) {
                    if (entry.getValue().before(now)) {
                        Entity entity = getEntity(key);
                        String cluster = getCluster(key);
                        GenericAlert.alertOnLikelySLAMiss(cluster, entity.getEntityType().name(),
                                entity.getName(), SchemaHelper.formatDateUTC(entry.getKey()));
                        LOG.debug("Removing from pending jobs: " + key + " ---> " +
                                SchemaHelper.formatDateUTC(entry.getKey()));
                        pendingInstances.remove(entry.getKey());
                    }
                }
                interim.clear();
            }
        }

        private void addNewPendingEntities(Set<String> keys) throws FalconException {
            Date now = new Date();
            Date windowEndTime = new Date(now.getTime() + POLL_PERIODICITY_SECS * 1000);
            for (String key : keys) {
                Entity entity = getEntity(key);
                String cluster = getCluster(key);
                if (entity == null) {
                    LOG.warn("No entity for " + key);
                    continue;
                }
                Date startTime = EntityUtil.getStartTime(entity, cluster);
                Frequency frequency = EntityUtil.getFrequency(entity);
                TimeZone timeZone = EntityUtil.getTimeZone(entity);
                Date nextStart = EntityUtil.getNextStartTime(startTime, frequency, timeZone, now);
                if (nextStart.after(windowEndTime)) {
                    continue;
                }
                ConcurrentMap<Date, Date> pendingInstances = pendingJobs.get(key);
                while (!nextStart.after(windowEndTime)) {
                    if (pendingInstances == null) {
                        pendingJobs.putIfAbsent(key, new ConcurrentHashMap<Date, Date>());
                        pendingInstances = pendingJobs.get(key);
                    }
                    Long latency = monitoredEntities.get(key);
                    if (latency == null) {
                        break;
                    }
                    pendingInstances.putIfAbsent(nextStart, new Date(nextStart.getTime() +
                            latency * 1500));  //1.5 times latency is when it is supposed to have breached
                    LOG.debug("Adding to pending jobs: " + key + " ---> " +
                            SchemaHelper.formatDateUTC(nextStart));
                    Calendar startCal = Calendar.getInstance(timeZone);
                    startCal.setTime(nextStart);
                    startCal.add(frequency.getTimeUnit().getCalendarUnit(), frequency.getFrequency());
                    nextStart = startCal.getTime();
                }
            }
        }
    }

    private static final Pattern regex = Pattern.compile("[()\\s/]");

    private Entity getEntity(String key) throws FalconException {
        String[] parts = regex.split(key);
        String name = parts[3];
        String type = parts[1];
        return EntityUtil.getEntity(type, name);
    }

    private String getCluster(String key) throws FalconException {
        String[] parts = regex.split(key);
        return parts[4];
    }

    @Override
    public void beforeSchedule(Entity entity, String cluster) throws FalconException {
    }

    @Override
    public void beforeDelete(Entity entity, String cluster) throws FalconException {
    }

    @Override
    public void beforeSuspend(Entity entity, String cluster) throws FalconException {
    }

    @Override
    public void beforeResume(Entity entity, String cluster) throws FalconException {
    }
}
