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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
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
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
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

    private Cache<InstanceID, Object> instancesToIgnore;
    // TODO : limit the no. of awaiting instances per entity
    private LoadingCache<EntityClusterID, List<ExecutionInstance>> executorAwaitedInstances;

    @Override
    public void register(NotificationRequest notifRequest) throws NotificationServiceException {
        JobScheduleNotificationRequest request = (JobScheduleNotificationRequest) notifRequest;
        if (request.getInstance() == null) {
            throw new NotificationServiceException("Request must contain an instance.");
        }
        // When the instance is getting rescheduled for run. As in the case of suspend and resume.
        Object obj = instancesToIgnore.getIfPresent(request.getInstance().getId());
        if (obj != null) {
            instancesToIgnore.invalidate(request.getInstance().getId());
        }
        runQueue.execute(new InstanceRunner(request));
    }

    @Override
    public void unregister(NotificationHandler handler, ID listenerID) {
        // If ID is that of an entity, do nothing
        if (listenerID instanceof InstanceID) {
            // Not efficient to iterate over elements to remove this. Add to ignore list.
            instancesToIgnore.put((InstanceID) listenerID, new Object());
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

        CacheLoader instanceCacheLoader = new CacheLoader<EntityClusterID, Collection<ExecutionInstance>>() {
            @Override
            public Collection<ExecutionInstance> load(EntityClusterID id) throws Exception {
                List<InstanceState.STATE> states = new ArrayList<InstanceState.STATE>();
                states.add(InstanceState.STATE.READY);
                List<ExecutionInstance> readyInstances = new ArrayList<>();
                // TODO : Limit it to no. of instances that can be run in parallel.
                for (InstanceState state : STATE_STORE.getExecutionInstances(id, states)) {
                    readyInstances.add(state.getInstance());
                }
                return readyInstances;
            }
        };

        executorAwaitedInstances = CacheBuilder.newBuilder()
                .maximumSize(100)
                .concurrencyLevel(1)
                .removalListener(this)
                .build(instanceCacheLoader);

        instancesToIgnore = CacheBuilder.newBuilder()
                .expireAfterWrite(1, TimeUnit.HOURS)
                .concurrencyLevel(1)
                .build();
        // Interested in all job completion events.
        JobCompletionNotificationRequest completionRequest = (JobCompletionNotificationRequest)
                NotificationServicesRegistry.getService(NotificationServicesRegistry.SERVICE.JOB_COMPLETION)
                        .createRequestBuilder(this, null).build();
        NotificationServicesRegistry.register(completionRequest);
    }

    @Override
    public void onRemoval(RemovalNotification<ID, List<ExecutionInstance>> removalNotification) {
        // When instances are removed due to size...
        // Ensure instances are persisted in state store and add to another list of awaited entities.
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
                List<ExecutionInstance> instances = null;
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
                    ExecutionInstance instance = instances.get(0);
                    if (instance != null && instance.getAwaitingPredicates() != null) {
                        for (Predicate predicate : instance.getAwaitingPredicates()) {
                            if (predicate.getType() == Predicate.TYPE.JOB_COMPLETION) {
                                // Construct a request object
                                NotificationHandler handler = ReflectionUtils
                                        .getInstanceByClassName(predicate.getClauseValue("handler").toString());
                                JobScheduleRequestBuilder requestBuilder = new JobScheduleRequestBuilder(
                                        handler, instance.getId());
                                requestBuilder.setInstance(instance);
                                InstanceRunner runner = new InstanceRunner(requestBuilder.build());
                                runQueue.execute(runner);
                                instances.remove(instance);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                throw new FalconException(e);
            }
        }
    }

    @Override
    public void destroy() throws FalconException {
        runQueue.shutdownNow();
        instancesToIgnore.invalidateAll();
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
            this.request = request;
            this.instance = request.getInstance();
            this.priority = getPriority(instance.getEntity()).getPriority();
            allowedParallelInstances = EntityUtil.getParallel(instance.getEntity());
        }

        public int incrementAllowedInstances() {
            return ++allowedParallelInstances;
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

        @Override
        public void run() {
            try {
                // If de-registered
                if (instancesToIgnore.getIfPresent(instance.getId()) != null) {
                    LOG.debug("Instance {} has been deregistered. Ignoring.", instance.getId());
                    instancesToIgnore.invalidate(instance.getId());
                    return;
                }
                LOG.debug("Received request to run instance {}", instance.getId());
                if (checkConditions()) {
                    // If instance not already scheduled.
                    String externalId = instance.getExternalID();
                    if (externalId == null) {
                        externalId = DAGEngineFactory.getDAGEngine(instance.getCluster()).run(instance);
                        LOG.info("Scheduled job {} for instance {}", externalId, instance.getId());
                    }
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
                    throw new RuntimeException("Unable to onEvent : " + request.getCallbackId(), fe);
                }
            }
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
                            entityID));
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
            synchronized (id) {
                List<ExecutionInstance> instances = executorAwaitedInstances.get(id);
                if (instances == null) {
                    // Order is FIFO.
                    instances = new LinkedList<>();
                    executorAwaitedInstances.put(id, instances);
                }
                instances.add(instance);
            }
        }

        private boolean dependencyCheck() throws FalconException, ExecutionException {
            if (request.getDependencies() == null || request.getDependencies().isEmpty()) {
                return true;
            }

            for (ExecutionInstance execInstance : request.getDependencies()) {
                // Dependants should wait for this instance to complete. Add predicate to that effect.
                instance.getAwaitingPredicates().add(Predicate.createJobCompletionPredicate(
                        request.getHandler(), execInstance.getId()));
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
