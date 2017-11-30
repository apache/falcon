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

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.LifeCycle;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.entity.v0.process.Cluster;
import org.apache.falcon.entity.v0.process.Clusters;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.jdbc.BacklogMetricStore;
import org.apache.falcon.metrics.MetricNotificationService;
import org.apache.falcon.resource.InstancesResult;
import org.apache.falcon.security.CurrentUser;
import org.apache.falcon.util.MetricInfo;
import org.apache.falcon.util.StartupProperties;
import org.apache.falcon.workflow.WorkflowExecutionContext;
import org.apache.falcon.workflow.WorkflowExecutionListener;
import org.apache.falcon.workflow.engine.AbstractWorkflowEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.falcon.workflow.WorkflowEngineFactory.getWorkflowEngine;

/**
 * Backlog Metric Emitter Service to publish metrics to Graphite.
 */
public final class BacklogMetricEmitterService implements FalconService,
        EntitySLAListener, WorkflowExecutionListener, ConfigurationChangeListener {

    private static final String METRIC_PREFIX = StartupProperties.get().getProperty("falcon.graphite.prefix");
    private static final String METRIC_SEPARATOR = ".";
    private static final String BACKLOG_METRIC_EMIT_INTERVAL = "falcon.backlog.metricservice.emit.interval.millisecs";
    private static final String BACKLOG_METRIC_RECHECK_INTERVAL = "falcon.backlog.metricservice."
            + "recheck.interval.millisecs";
    private static final String DEFAULT_PIPELINE = "DEFAULT";

    private static final Logger LOG = LoggerFactory.getLogger(BacklogMetricEmitterService.class);

    private static BacklogMetricStore backlogMetricStore = new BacklogMetricStore();

    private static final BacklogMetricEmitterService SERVICE = new BacklogMetricEmitterService();

    private static MetricNotificationService metricNotificationService =
            Services.get().getService(MetricNotificationService.SERVICE_NAME);

    private static final List<LifeCycle> PROCESS_LIFE_CYCLE =
            Collections.singletonList(LifeCycle.valueOf(LifeCycle.EXECUTION.name()));

    public static BacklogMetricEmitterService get() {
        return SERVICE;
    }

    private BacklogMetricEmitterService() {
    }

    private ScheduledThreadPoolExecutor scheduledThreadPoolExecutor1 = new ScheduledThreadPoolExecutor(1);
    private ScheduledThreadPoolExecutor scheduledThreadPoolExecutor2 = new ScheduledThreadPoolExecutor(1);


    public static final ThreadLocal<SimpleDateFormat> DATE_FORMAT = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH-mm'Z'");
            format.setTimeZone(TimeZone.getTimeZone("UTC"));
            return format;
        }
    };

    private static ConcurrentHashMap<Entity, List<MetricInfo>> entityBacklogs = new ConcurrentHashMap<>();

    @Override
    public void onAdd(Entity entity) throws FalconException {
        addToBacklog(entity);
    }

    @Override
    public void onRemove(Entity entity) throws FalconException {
        if (entity.getEntityType() != EntityType.PROCESS) {
            return;
        }
        Process process = (Process) entity;
        if (process.getSla() != null) {
            LOG.debug("Removing process:{} from monitoring", process.getName());
            backlogMetricStore.deleteEntityBackLogInstances(entity.getName(), entity.getEntityType().name());
            entityBacklogs.remove(entity);
            process = EntityUtil.getEntity(entity.getEntityType(), entity.getName());
            for (Cluster cluster : process.getClusters().getClusters()) {
                dropMetric(cluster.getName(), process);
            }
        }
    }

    private void dropMetric(String clusterName, Process process) {
        String pipelinesStr = process.getPipelines();
        String metricName;

        if (pipelinesStr != null && !pipelinesStr.isEmpty()) {
            String[] pipelines = pipelinesStr.split(",");
            for (String pipeline : pipelines) {
                metricName = getMetricName(clusterName, process.getName(), pipeline);
                metricNotificationService.deleteMetric(metricName);
            }
        } else {
            metricName = getMetricName(clusterName, process.getName(), DEFAULT_PIPELINE);
            metricNotificationService.deleteMetric(metricName);
        }
    }

    @Override
    public void onChange(Entity oldEntity, Entity newEntity) throws FalconException {
        if (oldEntity.getEntityType() != EntityType.PROCESS) {
            return;
        }
        Process newProcess = (Process) newEntity;
        Process oldProcess = EntityUtil.getEntity(oldEntity.getEntityType(), oldEntity.getName());
        if (newProcess.getSla() == null || newProcess.getSla().getShouldEndIn() == null) {
            if (oldProcess.getSla() != null) {
                backlogMetricStore.deleteEntityBackLogInstances(newProcess.getName(), newEntity.getEntityType().name());
                entityBacklogs.remove(newProcess);
                for (Cluster cluster : oldProcess.getClusters().getClusters()) {
                    dropMetric(cluster.getName(), oldProcess);
                }
            }
        } else {
            addToBacklog(newEntity);
        }
    }

    @Override
    public void onReload(Entity entity) throws FalconException {
        addToBacklog(entity);
    }

    private void addToBacklog(Entity entity) {
        if (entity.getEntityType() != EntityType.PROCESS) {
            return;
        }
        Process process = (Process) entity;
        if (process.getSla() == null) {
            return;
        }
        entityBacklogs.putIfAbsent(entity, Collections.synchronizedList(new ArrayList<MetricInfo>()));
    }

    @Override
    public void highSLAMissed(String entityName, String clusterName, EntityType entityType,
                              Date nominalTime) throws FalconException {
        if (entityType != EntityType.PROCESS) {
            return;
        }
        Entity entity = EntityUtil.getEntity(entityType, entityName);
        entityBacklogs.putIfAbsent(entity, Collections.synchronizedList(new ArrayList<MetricInfo>()));
        List<MetricInfo> metricInfoList = entityBacklogs.get(entity);
        String nominalTimeStr = DATE_FORMAT.get().format(nominalTime);
        MetricInfo metricInfo = new MetricInfo(nominalTimeStr, clusterName);
        if (!metricInfoList.contains(metricInfo)) {
            synchronized (metricInfoList) {
                backlogMetricStore.addInstance(entityName, clusterName, nominalTime, entityType);
                metricInfoList.add(metricInfo);
            }
        }
    }

    @Override
    public String getName() {
        return this.getClass().getSimpleName();
    }

    @Override
    public void init() throws FalconException {
        initInstances();
        int emitInterval = Integer.parseInt(StartupProperties.get().getProperty(BACKLOG_METRIC_EMIT_INTERVAL,
                "60000"));
        int recheckInterval = Integer.parseInt(StartupProperties.get().getProperty(BACKLOG_METRIC_RECHECK_INTERVAL,
                "60000"));
        scheduledThreadPoolExecutor1.scheduleAtFixedRate(new BacklogMetricEmitter(),
                1, emitInterval, TimeUnit.MILLISECONDS);
        scheduledThreadPoolExecutor2.scheduleAtFixedRate(new BacklogCheckService(),
                1, recheckInterval, TimeUnit.MILLISECONDS);
    }

    private void initInstances() throws FalconException {
        LOG.info("Initializing backlog instances from state store");
        Map<Entity, List<MetricInfo>> backlogInstances = backlogMetricStore.getAllInstances();
        if (backlogInstances != null && !backlogInstances.isEmpty()) {
            for (Map.Entry<Entity, List<MetricInfo>> entry : backlogInstances.entrySet()) {
                List<MetricInfo> metricsInDB = entry.getValue();
                List<MetricInfo> metricInfoList = Collections.synchronizedList(metricsInDB);
                entityBacklogs.put(entry.getKey(), metricInfoList);
                LOG.debug("Initializing backlog for entity " + entry.getKey().getName());
            }
        }
    }

    @Override
    public void destroy() throws FalconException {
        scheduledThreadPoolExecutor1.shutdown();
        scheduledThreadPoolExecutor2.shutdown();
    }

    @Override
    public synchronized void onSuccess(WorkflowExecutionContext context) throws FalconException {
        Entity entity = EntityUtil.getEntity(context.getEntityType(), context.getEntityName());
        if (entity.getEntityType() != EntityType.PROCESS) {
            return;
        }
        if (entityBacklogs.containsKey(entity)) {
            List<MetricInfo> metrics = entityBacklogs.get(entity);
            synchronized (metrics) {
                Date date = SchemaHelper.parseDateUTC(context.getNominalTimeAsISO8601());
                backlogMetricStore.deleteMetricInstance(entity.getName(), context.getClusterName(),
                        date, entity.getEntityType());
                metrics.remove(new MetricInfo(DATE_FORMAT.get().format(date), context.getClusterName()));
                if (metrics.isEmpty()) {
                    entityBacklogs.remove(entity);
                    publishBacklog((Process) entity, context.getClusterName(), 0L);
                }
            }
        }
    }

    @Override
    public void onFailure(WorkflowExecutionContext context) throws FalconException {
        // Do Nothing
    }

    @Override
    public void onStart(WorkflowExecutionContext context) throws FalconException {
        // Do Nothing
    }

    @Override
    public void onSuspend(WorkflowExecutionContext context) throws FalconException {
        // Do Nothing
    }

    @Override
    public void onWait(WorkflowExecutionContext context) throws FalconException {
        // Do Nothing
    }

    /**
     * Service that executes backlog evaluation and publishes metrics to Graphite for entities in parallel.
     */
    private static class BacklogMetricEmitter implements Runnable {
        private ThreadPoolExecutor executor;

        @Override
        public void run() {
            LOG.debug("BacklogMetricEmitter running for entities");
            executor = new ScheduledThreadPoolExecutor(10);
            List<Future> futures = new ArrayList<>();
            try {
                for (Entity entity : entityBacklogs.keySet()) {
                    futures.add(executor.submit(new BacklogCalcService(entity, entityBacklogs.get(entity))));
                }
                waitForFuturesToComplete(futures);
            } finally {
                executor.shutdown();
            }
        }

        private void waitForFuturesToComplete(List<Future> futures) {
            try {
                for (Future future : futures) {
                    future.get();
                }
            } catch (InterruptedException e) {
                LOG.error("Interruption while executing tasks " + e);
            } catch (ExecutionException e) {
                LOG.error("Error in executing threads " + e);
            }
        }
    }

    /**
     * Service that calculates backlog for given entity and publishes them to graphite.
     */
    private static class BacklogCalcService implements Runnable {

        private Entity entityObj;
        private List<MetricInfo> metrics;

        BacklogCalcService(Entity entity, List<MetricInfo> metricInfoList) {
            this.entityObj = entity;
            this.metrics = metricInfoList;
        }

        @Override
        public void run() {

            MetricInfo metricInfo = null;
            HashMap<String, Long> backLogsCluster = new HashMap<>();
            synchronized (metrics) {
                if (metrics.isEmpty()) {
                    Process process = (Process) entityObj;
                    Clusters clusters = process.getClusters();
                    for (Cluster cluster : clusters.getClusters()) {
                        publishBacklog(process, cluster.getName(), 0L);
                    }
                } else {
                    long currentTime = System.currentTimeMillis();
                    for (MetricInfo metric : metrics) {
                        try {
                            metricInfo = metric;
                            long time = DATE_FORMAT.get().parse(metricInfo.getNominalTime()).getTime();
                            long backlog = backLogsCluster.containsKey(metricInfo.getCluster())
                                    ? backLogsCluster.get(metricInfo.getCluster()) : 0;
                            backlog += (currentTime - time);
                            backLogsCluster.put(metricInfo.getCluster(), backlog);
                        } catch (ParseException e) {
                            LOG.error("Unable to parse nominal time" + metricInfo.getNominalTime());
                        }
                    }

                }
            }
            org.apache.falcon.entity.v0.process.Process process = (Process) entityObj;

            if (backLogsCluster != null && !backLogsCluster.isEmpty()) {
                for (Map.Entry<String, Long> entry : backLogsCluster.entrySet()) {
                    String clusterName = entry.getKey();
                    Long backlog = entry.getValue() / (60 * 1000L); // Converting to minutes
                    publishBacklog(process, clusterName, backlog);
                }
            }
        }
    }


    private static void publishBacklog(Process process, String clusterName, Long backlog) {
        String pipelinesStr = process.getPipelines();
        String metricName;

        if (pipelinesStr != null && !pipelinesStr.isEmpty()) {
            String[] pipelines = pipelinesStr.split(",");
            for (String pipeline : pipelines) {
                metricName = getMetricName(clusterName, process.getName(), pipeline);
                metricNotificationService.publish(metricName, backlog);
            }
        } else {
            metricName = getMetricName(clusterName, process.getName(), DEFAULT_PIPELINE);
            metricNotificationService.publish(metricName, backlog);
        }
    }

    private static String getMetricName(String clusterName, String processName, String pipeline) {
        return METRIC_PREFIX + METRIC_SEPARATOR + clusterName + METRIC_SEPARATOR
                + pipeline + METRIC_SEPARATOR + LifeCycle.EXECUTION.name()
                + METRIC_SEPARATOR + processName + METRIC_SEPARATOR
                + "backlogInMins";
    }

    /**
     * Service runs periodically and removes succeeded instances from backlog list.
     */
    private static class BacklogCheckService implements Runnable {
        @Override
        public void run() {
            LOG.trace("BacklogCheckService running for entities");
            try {
                AbstractWorkflowEngine wfEngine = getWorkflowEngine();
                for (Entity entity : entityBacklogs.keySet()) {
                    List<MetricInfo> metrics = entityBacklogs.get(entity);
                    if (!metrics.isEmpty()) {
                        synchronized (metrics) {
                            Iterator iterator = metrics.iterator();
                            while (iterator.hasNext()) {
                                MetricInfo metricInfo = (MetricInfo) iterator.next();
                                String nominalTimeStr = metricInfo.getNominalTime();
                                Date nominalTime;
                                try {
                                    nominalTime = DATE_FORMAT.get().parse(nominalTimeStr);
                                    authenticateUser(entity);
                                    if (wfEngine.isMissing(entity)) {
                                        LOG.info("Entity of name {} was deleted so removing instance of "
                                                + "nominal time {} ", entity.getName(), nominalTimeStr);
                                        backlogMetricStore.deleteMetricInstance(entity.getName(),
                                                metricInfo.getCluster(), nominalTime, entity.getEntityType());
                                        iterator.remove();
                                        continue;
                                    }
                                    InstancesResult status = null;
                                    try {
                                        status = wfEngine.getStatus(entity, nominalTime,
                                                new Date(nominalTime.getTime() + 200), PROCESS_LIFE_CYCLE, false);
                                    } catch (FalconException e) {
                                        LOG.error("Unable to get status for :" + entity.getName(), e);
                                    }
                                    if (status !=null && status.getInstances().length > 0
                                            && status.getInstances()[0].status == InstancesResult.
                                            WorkflowStatus.SUCCEEDED) {
                                        LOG.debug("Instance of nominal time {} of entity {} has succeeded, removing "
                                                + "from backlog entries", nominalTimeStr, entity.getName());
                                        backlogMetricStore.deleteMetricInstance(entity.getName(),
                                                metricInfo.getCluster(), nominalTime, entity.getEntityType());
                                        iterator.remove();
                                    }
                                } catch (ParseException e) {
                                    LOG.error("Unable to parse date " + nominalTimeStr);
                                }
                            }
                        }
                    }
                }
            } catch (Throwable e) {
                LOG.error("Error while checking backlog metrics" + e);
            }
        }
    }

    private static void authenticateUser(Entity entity) {
        if (!CurrentUser.isAuthenticated()) {
            if (StringUtils.isNotBlank(entity.getACL().getOwner())) {
                CurrentUser.authenticate(entity.getACL().getOwner());
            } else {
                CurrentUser.authenticate(System.getProperty("user.name"));
            }
        }
    }
}
