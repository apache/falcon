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

import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.workflow.WorkflowBuilder;
import org.apache.log4j.Logger;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClientException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Workflow engine which uses oozies APIs
 * 
 */
@SuppressWarnings("unchecked")
public class OozieWorkflowEngine implements WorkflowEngine {

    private static final Logger LOG = Logger.getLogger(OozieWorkflowEngine.class);

    private static final String ENGINE = "oozie";
    private static final CoordinatorJob MISSING = new NullCoordJob();

    @Override
    public String schedule(Entity entity) throws IvoryException {
        WorkflowBuilder builder = WorkflowBuilder.getBuilder(ENGINE, entity);

        //TODO actually be sending a bundle out.
        Map<String, Object> newFlows = builder.newWorkflowSchedule(entity);

        List<Properties> workflowProps = (List<Properties>)
                newFlows.get(WorkflowBuilder.PROPS);
        List<Cluster> clusters = (List<Cluster>)
                newFlows.get(WorkflowBuilder.CLUSTERS);

        StringBuilder buffer = new StringBuilder();
        try {
            for (int index = 0; index < workflowProps.size(); index++) {
                OozieClient client = OozieClient.get(clusters.get(index));
                String result = client.run(workflowProps.get(0));
                buffer.append(result).append(',');
            }
        } catch (OozieClientException e) {
            throw new IvoryException("Unable to schedule workflows", e);
        }
        return buffer.toString();
    }

    @Override
    public String dryRun(Entity entity) throws IvoryException {
        WorkflowBuilder builder = WorkflowBuilder.getBuilder(ENGINE, entity);

        //TODO actually be sending a bundle out.
        Map<String, Object> newFlows = builder.newWorkflowSchedule(entity);

        List<Properties> workflowProps = (List<Properties>)
                newFlows.get(WorkflowBuilder.PROPS);
        List<Cluster> clusters = (List<Cluster>)
                newFlows.get(WorkflowBuilder.CLUSTERS);

        StringBuilder buffer = new StringBuilder();
        try {
            for (int index = 0; index < workflowProps.size(); index++) {
                OozieClient client = OozieClient.get(clusters.get(index));
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
        runningFilter.append(OozieClient.FILTER_STATUS).append('=').
                append(Job.Status.PREP).append(';');
        runningFilter.append(OozieClient.FILTER_STATUS).append('=')
                .append(Job.Status.RUNNING).append(';');
        runningFilter.append(OozieClient.FILTER_STATUS).append('=')
                .append(Job.Status.PAUSED).append(';');
        runningFilter.append(OozieClient.FILTER_STATUS).append('=')
                .append(Job.Status.PREPPAUSED).append(';');

        StringBuilder suspendedFilter = new StringBuilder();
        suspendedFilter.append(OozieClient.FILTER_STATUS).append('=')
                .append(Job.Status.PREPSUSPENDED).append(';');
        suspendedFilter.append(OozieClient.FILTER_STATUS).append('=')
                .append(Job.Status.SUSPENDED).append(';');

        ACTIVE_FILTER = runningFilter.toString() + suspendedFilter.toString();
        RUNNING_FILTER = runningFilter.toString();
        SUSPENDED_FILTER = suspendedFilter.toString();
    }

    //To be replaced with bundle. We should actually be operating at bundle granularity
    public Map<Cluster, CoordinatorJob> findActiveCoordinator(Entity entity)
            throws IvoryException{
        return findCoordinatorInternal(entity, ACTIVE_FILTER);
    }

    public Map<Cluster, CoordinatorJob> findSuspendedCoordinator(Entity entity)
            throws IvoryException{
        return findCoordinatorInternal(entity, SUSPENDED_FILTER);
    }

    public Map<Cluster, CoordinatorJob> findRunningCoordinator(Entity entity)
            throws IvoryException{
        return findCoordinatorInternal(entity, RUNNING_FILTER);
    }

    public boolean isActive(Entity entity) throws IvoryException {
        return findActiveCoordinator(entity) != null;
    }

    public boolean isSuspended(Entity entity) throws IvoryException {
        return findSuspendedCoordinator(entity) != null;
    }

    public boolean isRunning(Entity entity) throws IvoryException {
        return findRunningCoordinator(entity) != null;
    }

    private Map<Cluster, CoordinatorJob> findCoordinatorInternal(Entity entity,
                                                                 String filter)
            throws IvoryException {

        try {
            WorkflowBuilder builder = WorkflowBuilder.getBuilder(ENGINE, entity);
            String name = getBundleName(entity);

            Cluster[] clusters = builder.getScheduledClustersFor(entity);
            Map<Cluster, CoordinatorJob> jobArray = new
                    HashMap<Cluster, CoordinatorJob>();

            for (Cluster cluster : clusters) {
                OozieClient client = OozieClient.get(cluster);
                List<CoordinatorJob> jobs = client.getCoordJobsInfo(filter +
                        OozieClient.FILTER_NAME + "=" + name + ";", 0, 10);
                if (jobs.size() > 1) {
                    throw new IllegalStateException("Too many jobs qualified " +
                            jobs);
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
        return "IVORY_" + entity.getEntityType().name().toUpperCase() + "_" +
                entity.getName();
    }

    @Override
    public String suspend(Entity entity) throws IvoryException {
        boolean success = true;
        Map<Cluster, CoordinatorJob> jobs = findRunningCoordinator(entity);
        for (Cluster cluster : jobs.keySet()) {
            CoordinatorJob job = jobs.get(cluster);
            if (job == MISSING) {
                LOG.warn("No active job found for " + entity.getName());
            } else {
                try {
                    OozieClient client = OozieClient.get(cluster);
                    client.suspend(job.getId());
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
        Map<Cluster, CoordinatorJob> jobs = findSuspendedCoordinator(entity);
        for (Cluster cluster : jobs.keySet()) {
            CoordinatorJob job = jobs.get(cluster);
            if (job == MISSING) {
                LOG.warn("No active job found for " + entity.getName());
            } else {
                try {
                    OozieClient client = OozieClient.get(cluster);
                    client.resume(job.getId());
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
        Map<Cluster, CoordinatorJob> jobs = findActiveCoordinator(entity);
        for (Cluster cluster : jobs.keySet()) {
            CoordinatorJob job = jobs.get(cluster);
            if (job == MISSING) {
                LOG.warn("No active job found for " + entity.getName());
            } else {
                try {
                    OozieClient client = OozieClient.get(cluster);
                    client.kill(job.getId());
                } catch (OozieClientException e) {
                    LOG.error("Unable to suspend workflow " + job.getId(), e);
                    success = false;
                }
            }
        }
        return success ? "SUCCESS" : "FAILED";
    }
}
