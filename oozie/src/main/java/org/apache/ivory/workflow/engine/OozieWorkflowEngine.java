/*
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

package org.apache.ivory.workflow.engine;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.workflow.WorkflowBuilder;
import org.apache.log4j.Logger;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;

/**
 * Workflow engine which uses oozies APIs
 * 
 */
@SuppressWarnings("unchecked")
public class OozieWorkflowEngine implements WorkflowEngine {

    private static final Logger LOG = Logger.getLogger(OozieWorkflowEngine.class);

    public static final String ENGINE = "oozie";
    private static final BundleJob MISSING = new NullBundleJob();

    private static final WorkflowEngineActionListener listener = new OozieHouseKeepingService();

    @Override
    public String schedule(Entity entity) throws IvoryException {
        WorkflowBuilder builder = WorkflowBuilder.getBuilder(ENGINE, entity);

        Map<String, Object> newFlows = builder.newWorkflowSchedule(entity);

        List<Properties> workflowProps = (List<Properties>) newFlows.get(WorkflowBuilder.PROPS);
        List<Cluster> clusters = (List<Cluster>) newFlows.get(WorkflowBuilder.CLUSTERS);

        StringBuilder buffer = new StringBuilder();
        try {
            for (int index = 0; index < workflowProps.size(); index++) {
                OozieClient client = OozieClientFactory.get(clusters.get(index));

                listener.beforeSchedule(clusters.get(index), entity);
                String result = client.run(workflowProps.get(0));
                listener.afterSchedule(clusters.get(index), entity);

                buffer.append(result).append(',');
            }
        } catch (OozieClientException e) {
            LOG.error("Unable to schedule workflows", e);
            throw new IvoryException("Unable to schedule workflows", e);
        }
        return buffer.toString();
    }

    @Override
    public String dryRun(Entity entity) throws IvoryException {
        WorkflowBuilder builder = WorkflowBuilder.getBuilder(ENGINE, entity);

        Map<String, Object> newFlows = builder.newWorkflowSchedule(entity);

        List<Properties> workflowProps = (List<Properties>) newFlows.get(WorkflowBuilder.PROPS);
        List<Cluster> clusters = (List<Cluster>) newFlows.get(WorkflowBuilder.CLUSTERS);

        StringBuilder buffer = new StringBuilder();
        try {
            for (int index = 0; index < workflowProps.size(); index++) {
                OozieClient client = OozieClientFactory.get(clusters.get(index));
                String result = client.dryrun(workflowProps.get(0));
                buffer.append(result).append(',');
            }
        } catch (OozieClientException e) {
            throw new IvoryException("Unable to schedule workflows", e);
        }
        return buffer.toString();
    }

    private static String ACTIVE_FILTER = "";
    private static String RUNNING_FILTER = "";
    private static String SUSPENDED_FILTER = "";
    
    static {
        StringBuilder runningFilter = new StringBuilder();
        runningFilter.append(OozieClient.FILTER_STATUS).append('=').append(Job.Status.PREP).append(';');
        runningFilter.append(OozieClient.FILTER_STATUS).append('=').append(Job.Status.RUNNING).append(';');
        runningFilter.append(OozieClient.FILTER_STATUS).append('=').append(Job.Status.PAUSED).append(';');
        runningFilter.append(OozieClient.FILTER_STATUS).append('=').append(Job.Status.PREPPAUSED).append(';');

        StringBuilder suspendedFilter = new StringBuilder();
        suspendedFilter.append(OozieClient.FILTER_STATUS).append('=').append(Job.Status.PREPSUSPENDED).append(';');
        suspendedFilter.append(OozieClient.FILTER_STATUS).append('=').append(Job.Status.SUSPENDED).append(';');

        ACTIVE_FILTER = runningFilter.toString() + suspendedFilter.toString();
        RUNNING_FILTER = runningFilter.toString();
        SUSPENDED_FILTER = suspendedFilter.toString();
    }

    public Map<Cluster, BundleJob> findActiveBundle(Entity entity) throws IvoryException {
        return findBundleInternal(entity, ACTIVE_FILTER);
    }

    public Map<Cluster, BundleJob> findSuspendedBundle(Entity entity) throws IvoryException {
        return findBundleInternal(entity, SUSPENDED_FILTER);
    }

    public Map<Cluster, BundleJob> findRunningBundle(Entity entity) throws IvoryException {
        return findBundleInternal(entity, RUNNING_FILTER);
    }

    public boolean isActive(Entity entity) throws IvoryException {
        Map<Cluster, BundleJob> activeBundles = findActiveBundle(entity);
        boolean active = false;
        for (Map.Entry<Cluster, BundleJob> entry : activeBundles.entrySet()) {
            if (entry.getValue() != MISSING) {
                active = true;
            }
        }
        return active;
    }

    public boolean isSuspended(Entity entity) throws IvoryException {
        Map<Cluster, BundleJob> suspendedBundles = findSuspendedBundle(entity);
        boolean suspended = false;
        for (Map.Entry<Cluster, BundleJob> entry : suspendedBundles.entrySet()) {
            if (entry.getValue() != MISSING) {
                suspended = true;
            }
        }
        return suspended;
    }

    public boolean isRunning(Entity entity) throws IvoryException {
        Map<Cluster, BundleJob> runningBundles = findRunningBundle(entity);
        boolean running = false;
        for (Map.Entry<Cluster, BundleJob> entry : runningBundles.entrySet()) {
            if (entry.getValue() != MISSING) {
                running = true;
            }
        }
        return running;
    }

    private Map<Cluster, BundleJob> findBundleInternal(Entity entity, String filter) throws IvoryException {
        try {
            WorkflowBuilder builder = WorkflowBuilder.getBuilder(ENGINE, entity);
            String name = getBundleName(entity);

            Cluster[] clusters = builder.getScheduledClustersFor(entity);
            Map<Cluster, BundleJob> jobArray = new HashMap<Cluster, BundleJob>();

            for (Cluster cluster : clusters) {
                OozieClient client = OozieClientFactory.get(cluster);
                List<BundleJob> jobs = client.getBundleJobsInfo(filter + OozieClient.FILTER_NAME + "=" + name + ";", 0, 10);
                if (jobs.size() > 1) {
                    throw new IllegalStateException("Too many jobs qualified " + jobs);
                } else if (jobs.isEmpty()) {
                    jobArray.put(cluster, MISSING);
                } else {
                    jobArray.put(cluster, jobs.get(0));
                }
            }
            return jobArray;
        } catch (OozieClientException e) {
            throw new IvoryException(e);
        }
    }

    private String getBundleName(Entity entity) {
        return "IVORY_" + entity.getEntityType().name().toUpperCase() + "_" + entity.getName();
    }

    @Override
    public String suspend(Entity entity) throws IvoryException {
        boolean success = true;
        Map<Cluster, BundleJob> jobs = findRunningBundle(entity);
        for (Cluster cluster : jobs.keySet()) {
            BundleJob job = jobs.get(cluster);
            if (job == MISSING) {
                LOG.warn("No active job found for " + entity.getName());
            } else {
                try {
                    OozieClient client = OozieClientFactory.get(cluster);

                    listener.beforeSuspend(cluster, entity);
                    client.suspend(job.getId());
                    listener.afterSuspend(cluster, entity);

                } catch (OozieClientException e) {
                    LOG.warn("Unable to suspend workflow " + job.getId(), e);
                    success = false;
                }
            }
        }
        return success ? "SUCCESS" : "FAILED";
    }

    @Override
    public String resume(Entity entity) throws IvoryException {
        boolean success = true;
        Map<Cluster, BundleJob> jobs = findSuspendedBundle(entity);
        for (Cluster cluster : jobs.keySet()) {
            BundleJob job = jobs.get(cluster);
            if (job == MISSING) {
                LOG.warn("No active job found for " + entity.getName());
            } else {
                try {
                    OozieClient client = OozieClientFactory.get(cluster);

                    listener.beforeResume(cluster, entity);
                    client.resume(job.getId());
                    listener.afterResume(cluster, entity);

                } catch (OozieClientException e) {
                    LOG.error("Unable to suspend workflow " + job.getId(), e);
                    success = false;
                }
            }
        }
        return success ? "SUCCESS" : "FAILED";
    }

    @Override
    public String delete(Entity entity) throws IvoryException {
        boolean success = true;
        Map<Cluster, BundleJob> jobs = findActiveBundle(entity);
        for (Cluster cluster : jobs.keySet()) {
            BundleJob job = jobs.get(cluster);
            if (job == MISSING) {
                LOG.warn("No active job found for " + entity.getName());
            } else {
                try {
                    OozieClient client = OozieClientFactory.get(cluster);

                    listener.beforeDelete(cluster, entity);
                    client.kill(job.getId());
                    listener.afterDelete(cluster, entity);

                } catch (OozieClientException e) {
                    LOG.error("Unable to suspend workflow " + job.getId(), e);
                    success = false;
                }
            }
        }
        return success ? "SUCCESS" : "FAILED";
    }
}
