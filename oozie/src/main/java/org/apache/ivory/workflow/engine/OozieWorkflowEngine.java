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
import org.apache.ivory.workflow.WorkflowBuilder;
import org.apache.log4j.Logger;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClientException;

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

    private static final String OOZIE_MIME_TYPE = "application/xml;charset=UTF-8";
    private static final String JOBS_RESOURCE = "jobs";
    private static final String JOB_RESOURCE = "job";

    private static final String URI_SEPERATOR = "/";

    private static final OozieClient client = OozieClient.get();

    @Override
    public String schedule(Entity entity) throws IvoryException {
        WorkflowBuilder builder = WorkflowBuilder.getBuilder("oozie", entity);

        //TODO actually be sending a bundle out.
        Map<String, Object> newFlows = builder.newWorkflowSchedule(entity);

        List<Properties> workflowProps = (List<Properties>)
                newFlows.get("PROPS");

        StringBuilder buffer = new StringBuilder();
        try {
            for (Properties props : workflowProps) {
                String result = client.run(props);
                buffer.append(result).append(',');
            }
        } catch (OozieClientException e) {
            throw new IvoryException("Unable to schedule workflows", e);
        }
        return buffer.toString();
    }

    @Override
    public String dryRun(Entity entity) throws IvoryException {
        WorkflowBuilder builder = WorkflowBuilder.getBuilder("oozie", entity);

        //TODO actually be sending a bundle out.
        Map<String, Object> newFlows = builder.newWorkflowSchedule(entity);

        List<Properties> workflowProps = (List<Properties>)
                newFlows.get(WorkflowBuilder.PROPS);

        StringBuilder buffer = new StringBuilder();
        try {
            for (Properties props : workflowProps) {
                String result = client.dryrun(props);
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
    public CoordinatorJob findActiveCoordinator(Entity entity)
            throws IvoryException{
        return findCoordinatorInternal(entity, ACTIVE_FILTER);
    }

    public CoordinatorJob findSuspendedCoordinator(Entity entity)
            throws IvoryException{
        return findCoordinatorInternal(entity, SUSPENDED_FILTER);
    }

    public CoordinatorJob findRunningCoordinator(Entity entity)
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

    private CoordinatorJob findCoordinatorInternal(Entity entity, String filter)
            throws IvoryException {
        try {
            String name = getBundleName(entity);
            List<CoordinatorJob> jobs = client.getCoordJobsInfo(filter +
                    OozieClient.FILTER_NAME + "=" + name + ";", 0, 10);
            if (jobs.size() > 1) {
                throw new IllegalStateException("Too many jobs qualified " +
                        jobs);
            } else if (jobs.isEmpty()) {
                return null;
            } else {
                return jobs.get(0);
            }
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
        CoordinatorJob job = findRunningCoordinator(entity);
        if (job == null) {
            throw new IvoryException("No active job found for " +
                    entity.getName());
        } else {
            try {
                client.suspend(job.getId());
                return "SUCCESS";
            } catch (OozieClientException e) {
                throw new IvoryException("Unable to suspend workflow " +
                        job.getId(), e);
            }
        }
    }

    @Override
    public String resume(Entity entity) throws IvoryException {
        CoordinatorJob job = findSuspendedCoordinator(entity);
        if (job == null) {
            throw new IvoryException("No active job found for " +
                    entity.getName());
        } else {
            try {
                client.resume(job.getId());
                return "SUCCESS";
            } catch (OozieClientException e) {
                throw new IvoryException("Unable to suspend workflow " +
                        job.getId(), e);
            }
        }
    }

    @Override
    public String delete(Entity entity) throws IvoryException {
        CoordinatorJob job = findActiveCoordinator(entity);
        if (job == null) {
            throw new IvoryException("No active job found for " +
                    entity.getName());
        } else {
            try {
                client.kill(job.getId());
                return "SUCCESS";
            } catch (OozieClientException e) {
                throw new IvoryException("Unable to suspend workflow " +
                        job.getId(), e);
            }
        }
    }
}
