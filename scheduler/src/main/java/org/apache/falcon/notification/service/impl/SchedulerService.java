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
package org.apache.falcon.notification.service.impl;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.exception.NotificationServiceException;
import org.apache.falcon.exception.StateStoreException;
import org.apache.falcon.execution.ExecutionInstance;
import org.apache.falcon.execution.NotificationHandler;
import org.apache.falcon.notification.service.FalconNotificationService;
import org.apache.falcon.notification.service.NotificationServicesRegistry;
import org.apache.falcon.notification.service.event.Event;
import org.apache.falcon.notification.service.event.EventType;
import org.apache.falcon.notification.service.event.JobScheduledEvent;
import org.apache.falcon.notification.service.request.JobCompletionNotificationRequest;
import org.apache.falcon.notification.service.request.JobScheduleNotificationRequest;
import org.apache.falcon.notification.service.request.NotificationRequest;
import org.apache.falcon.predicate.Predicate;
import org.apache.falcon.state.EntityClusterID;
import org.apache.falcon.state.ID;
import org.apache.falcon.state.InstanceID;
import org.apache.falcon.state.InstanceState;
import org.apache.falcon.state.store.AbstractStateStore;
import org.apache.falcon.state.store.StateStore;
import org.apache.falcon.util.ReflectionUtils;
import org.apache.falcon.util.RuntimeProperties;
import org.apache.falcon.workflow.engine.DAGEngineFactory;
import org.apache.falcon.workflow.engine.FalconWorkflowEngine;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * This notification service notifies {@link NotificationHandler} when an execution
 * instance is scheduled on a DAG Engine.
 * Current implementation of scheduler handles parallel scheduling of instances,
 * dependencies (an instance depending on completion of another) and priority.
 */
public class SchedulerService implements FalconNotificationService, NotificationHandler,
        RemovalListener<ID, List<ExecutionInstance>> {

    private static final Logger LOG = LoggerFactory.getLogger(SchedulerService.class);

    public static final String DEFAULT_NUM_OF_SCHEDULER_THREADS = "5";
    public static final String NUM_OF_SCHEDULER_THREADS_PROP = "scheduler.threads.count";

    // Once scheduling conditions are met, it goes to run queue to be run on DAGEngine, based on priority.
    private ThreadPoolExecutor runQueue;

    private static final StateStore STATE_STORE = AbstractStateStore.get();

    // TODO : limit the no. of awaiting instances per entity
    private LoadingCache<EntityClusterID, SortedMap<Integer, ExecutionInstance>> executorAwaitedInstances;

    @Override
    public void register(NotificationRequest notifRequest) throws NotificationServiceException {
        JobScheduleNotificationRequest request = (JobScheduleNotificationRequest) notifRequest;
        if (request.getInstance() == null) {
            throw new NotificationServiceException("Request must contain an instance.");
        }
        LOG.debug("Received request to schedule instance {} with sequence {}.", request.getInstance().getId(),
                request.getInstance().getInstanceSequence());
        runQueue.execute(new InstanceRunner(request));
    }

    @Override
    public void unregister(NotificationHandler handler, ID listenerID) throws NotificationServiceException {
        // If ID is that of an entity, do nothing
        if (listenerID instanceof InstanceID) {
            try {
                InstanceID instanceID = (InstanceID) listenerID;
                SortedMap<Integer, ExecutionInstance> instances = executorAwaitedInstances.get(instanceID
                            .getEntityClusterID());
                if (instances != null && !instances.isEmpty()) {
                    synchronized (instances) {
                        instances.remove(STATE_STORE.getExecutionInstance(instanceID)
                                .getInstance().getInstanceSequence());
                    }
                }
            } catch (Exception e) {
                throw new NotificationServiceException(e);
            }
        }
    }

    @Override
    public RequestBuilder createRequestBuilder(NotificationHandler handler, ID callbackID) {
        return new JobScheduleRequestBuilder(handler, callbackID);
    }

    @Override
    public String getName() {
        return "JobSchedulerService";
    }

    @Override
    public void init() throws FalconException {
        int numThreads = Integer.parseInt(RuntimeProperties.get().getProperty(NUM_OF_SCHEDULER_THREADS_PROP,
                DEFAULT_NUM_OF_SCHEDULER_THREADS));

        // Uses a priority queue to ensure instances with higher priority gets run first.
        PriorityBlockingQueue<Runnable> pq = new PriorityBlockingQueue<Runnable>(20, new PriorityComparator());
        runQueue = new ThreadPoolExecutor(1, numThreads, 0L, TimeUnit.MILLISECONDS, pq);

        CacheLoader instanceCacheLoader = new CacheLoader<EntityClusterID, SortedMap<Integer, ExecutionInstance>>() {
            @Override
            public SortedMap<Integer, ExecutionInstance> load(EntityClusterID id) throws Exception {
                List<InstanceState.STATE> states = new ArrayList<InstanceState.STATE>();
                states.add(InstanceState.STATE.READY);
                SortedMap<Integer, ExecutionInstance> readyInstances = Collections.synchronizedSortedMap(
                        new TreeMap<Integer, ExecutionInstance>());
                // TODO : Limit it to no. of instances that can be run in parallel.
                for (InstanceState state : STATE_STORE.getExecutionInstances(id, states)) {
                    readyInstances.put(state.getInstance().getInstanceSequence(), state.getInstance());
                }
                return readyInstances;
            }
        };

        executorAwaitedInstances = CacheBuilder.newBuilder()
                .maximumSize(100)
                .concurrencyLevel(1)
                .removalListener(this)
                .build(instanceCacheLoader);

        // Interested in all job completion events.
        JobCompletionNotificationRequest completionRequest = (JobCompletionNotificationRequest)
                NotificationServicesRegistry.getService(NotificationServicesRegistry.SERVICE.JOB_COMPLETION)
                        .createRequestBuilder(this, null).build();
        NotificationServicesRegistry.register(completionRequest);
    }

    @Override
    public void onRemoval(RemovalNotification<ID, List<ExecutionInstance>> removalNotification) {
        // When instances are removed due to size...
        // Ensure instances are persisted in state store.
        if (removalNotification.wasEvicted()) {
            for (ExecutionInstance instance : removalNotification.getValue()) {
                InstanceState state = new InstanceState(instance);
                state.setCurrentState(InstanceState.STATE.READY);
                try {
                    STATE_STORE.updateExecutionInstance(state);
                } catch (StateStoreException e) {
                    throw new RuntimeException("Unable to persist the ready instance " + instance.getId(), e);
                }
            }
        }
    }

    @Override
    public void onEvent(Event event) throws FalconException {
        // Interested only in job completion events.
        if (event.getType() == EventType.JOB_COMPLETED) {
            try {
                ID targetID = event.getTarget();
                SortedMap<Integer, ExecutionInstance> instances = null;
                // Check if the instance is awaited.
                if (targetID instanceof EntityClusterID) {
                    EntityClusterID id = (EntityClusterID) event.getTarget();
                    instances = executorAwaitedInstances.get(id);
                    if (instances != null && instances.isEmpty()) {
                        executorAwaitedInstances.invalidate(id);
                    }
                } else if (targetID instanceof InstanceID) {
                    InstanceID id = (InstanceID) event.getTarget();
                    instances = executorAwaitedInstances.get(id.getEntityClusterID());
                }
                if (instances != null && !instances.isEmpty()) {
                    synchronized (instances) {
                        // Order is FIFO..
                        ExecutionInstance instance = instances.get(instances.firstKey());
                        if (instance != null && instance.getAwaitingPredicates() != null) {
                            for (Predicate predicate : instance.getAwaitingPredicates()) {
                                if (predicate.getType() == Predicate.TYPE.JOB_COMPLETION) {
                                    // Construct a request object
                                    NotificationHandler handler = ReflectionUtils
                                            .getInstanceByClassName(predicate.getClauseValue("handler").toString());
                                    JobScheduleRequestBuilder requestBuilder = new JobScheduleRequestBuilder(
                                            handler, instance.getId());
                                    requestBuilder.setInstance(instance);
                                    //The update kicks in for new instances, but, when old waiting instances are
                                    // scheduled and it retrieves the parallelism for entity definition,
                                    // it will use the "new" parallelism (if the user has updated it).
                                    // Since there is no versioning of entities yet,
                                    // need to retrieve what was the parallelism when that instance was created.
                                    Integer runParallel = (Integer)predicate.getClauseValue("parallelInstances");
                                    InstanceRunner runner = new InstanceRunner(requestBuilder.build(), runParallel);
                                    runQueue.execute(runner);
                                    instances.remove(instance.getInstanceSequence());
                                    break;
                                }
                            }
                        }
                    }
                }
            } catch (ExecutionException e) {
                throw new FalconException(e);
            }
        }
    }

    @Override
    public PRIORITY getPriority() {
        return PRIORITY.MEDIUM;
    }

    @Override
    public void destroy() throws FalconException {
        runQueue.shutdownNow();
    }

    private void notifyFailureEvent(JobScheduleNotificationRequest request) throws FalconException {
        JobScheduledEvent event = new JobScheduledEvent(request.getCallbackId(), JobScheduledEvent.STATUS.FAILED);
        request.getHandler().onEvent(event);
    }

    private class InstanceRunner implements Runnable {
        private final ExecutionInstance instance;
        private final JobScheduleNotificationRequest request;
        private short priority;
        private int allowedParallelInstances = 1;

        public InstanceRunner(JobScheduleNotificationRequest request) {
            this(request, EntityUtil.getParallel(request.getInstance().getEntity()));
        }

        /**
         * @param request
         * @param runParallel - concurrency at the time the Instance was run,
         *                    coz., runParallel can be updated later by user.
         */
        public InstanceRunner(JobScheduleNotificationRequest request, Integer runParallel) {
            this.request = request;
            this.instance = request.getInstance();
            this.priority = getPriority(instance.getEntity()).getPriority();
            allowedParallelInstances = runParallel;
        }

        private EntityUtil.JOBPRIORITY getPriority(Entity entity) {
            switch(entity.getEntityType()) {
            case PROCESS :
                return EntityUtil.getPriority((Process)entity);
            default :
                throw new UnsupportedOperationException("Scheduling of entities other "
                        + "than process is not supported yet.");
            }
        }

        public ExecutionInstance getInstance() {
            return instance;
        }

        @Override
        public void run() {
            try {
                LOG.debug("Received request to run instance {}", instance.getId());
                if (checkConditions()) {
                    String externalId = instance.getExternalID();
                    if (externalId != null) {
                        Properties props = instance.getProperties();
                        boolean isForced = false;
                        if (props != null) {
                            isForced = Boolean.valueOf(props.getProperty(FalconWorkflowEngine.FALCON_FORCE_RERUN));
                        }
                        if (isReRun(props)) {
                            DAGEngineFactory.getDAGEngine(instance.getCluster()).reRun(instance, props, isForced);
                        }
                    } else {
                        externalId = DAGEngineFactory.getDAGEngine(instance.getCluster()).run(instance);
                    }
                    LOG.info("Scheduled job {} for instance {}", externalId, instance.getId());
                    JobScheduledEvent event = new JobScheduledEvent(instance.getId(),
                            JobScheduledEvent.STATUS.SUCCESSFUL);
                    event.setExternalID(externalId);
                    event.setStartTime(new DateTime(DAGEngineFactory.getDAGEngine(instance.getCluster())
                            .info(externalId).getStartTime()));
                    request.getHandler().onEvent(event);
                }
            } catch (FalconException e) {
                LOG.error("Error running the instance : " + instance.getId(), e);
                try {
                    notifyFailureEvent(request);
                } catch (FalconException fe) {
                    throw new RuntimeException("Unable to invoke onEvent : " + request.getCallbackId(), fe);
                }
            }
        }

        private boolean isReRun(Properties props) {
            if (props != null && !props.isEmpty()) {
                return Boolean.valueOf(props.getProperty(FalconWorkflowEngine.FALCON_RERUN));
            }
            return false;
        }

        public short getPriority() {
            return priority;
        }

        private boolean checkConditions() throws FalconException {
            try {
                // TODO : If and when the no. of scheduling conditions increase, consider chaining condition checks.
                // Run if all conditions are met.
                if (instanceCheck() && dependencyCheck()) {
                    return true;
                } else {
                    EntityClusterID entityID = instance.getId().getEntityClusterID();
                    // Instance is awaiting scheduling conditions to be met. Add predicate to that effect.
                    instance.getAwaitingPredicates().add(Predicate.createJobCompletionPredicate(request.getHandler(),
                            entityID, EntityUtil.getParallel(instance.getEntity())));
                    updateExecutorAwaitedInstances(entityID);
                    LOG.debug("Schedule conditions not met for instance {}. Awaiting on {}",
                            instance.getId(), entityID);
                }
            } catch (Exception e) {
                LOG.error("Instance run failed with error : ", e);
                throw new FalconException("Instance run failed", e);
            }
            return false;
        }

        private void updateExecutorAwaitedInstances(EntityClusterID id) throws ExecutionException {
            SortedMap<Integer, ExecutionInstance> instances = executorAwaitedInstances.get(id);
            // instances will never be null as it is initialized in the loading cache.
            instances.put(instance.getInstanceSequence(), instance);
        }

        private boolean dependencyCheck() throws FalconException, ExecutionException {
            if (request.getDependencies() == null || request.getDependencies().isEmpty()) {
                return true;
            }

            for (ExecutionInstance execInstance : request.getDependencies()) {
                // Dependants should wait for this instance to complete. Add predicate to that effect.
                instance.getAwaitingPredicates().add(Predicate.createJobCompletionPredicate(
                        request.getHandler(), execInstance.getId(), EntityUtil.getParallel(instance.getEntity())));
                updateExecutorAwaitedInstances(execInstance.getId().getEntityClusterID());
            }
            return false;
        }

        // Ensure no. of instances running in parallel is per entity specification.
        private boolean instanceCheck() throws StateStoreException {
            return STATE_STORE.getExecutionInstances(instance.getEntity(), instance.getCluster(),
                    InstanceState.getRunningStates()).size() < allowedParallelInstances;
        }
    }

    // A priority based comparator to be used by the {@link java.util.concurrent.PriorityBlockingQueue}
    private static class PriorityComparator<T extends InstanceRunner> implements Comparator<T>, Serializable {
        @Override
        public int compare(T o1, T o2) {
            // If both instances have same priority, go by instance sequence.
            if (o1.getPriority() == o2.getPriority()) {
                return o1.getInstance().getInstanceSequence() - o2.getInstance().getInstanceSequence();
            }
            return o1.getPriority() - o2.getPriority();
        }
    }

    /**
     * Builds {@link JobScheduleNotificationRequest}.
     */
    public static class JobScheduleRequestBuilder extends RequestBuilder<JobScheduleNotificationRequest> {
        private List<ExecutionInstance> dependencies;
        private ExecutionInstance instance;

        public JobScheduleRequestBuilder(NotificationHandler handler, ID callbackID) {
            super(handler, callbackID);
        }

        /**
         * @param execInstance that needs to be scheduled
         * @return
         */
        public JobScheduleRequestBuilder setInstance(ExecutionInstance execInstance) {
            this.instance = execInstance;
            return this;
        }

        /**
         * Dependencies to wait for before scheduling.
         * @param dependencies
         */
        public void setDependencies(List<ExecutionInstance> dependencies) {
            this.dependencies = dependencies;
        }

        @Override
        public JobScheduleNotificationRequest build() {
            if (callbackId == null  || instance == null) {
                throw new IllegalArgumentException("Missing one or more of the mandatory arguments:"
                        + " callbackId, execInstance");
            }
            return new JobScheduleNotificationRequest(handler, callbackId, instance, dependencies);
        }
    }
}
