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
import org.apache.ivory.entity.EntityUtil;
import org.apache.ivory.transaction.Action;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;

public class OozieWorkflowEngineAction extends Action {
    private static final String CLUSTER_NAME_KEY = "cluster";
    private static final String JOB_ID_KEY = "jobId";
    private static final String CHANGE_VALUE_KEY = "changeValue";

    private static final WorkflowEngineActionListener listener = new OozieHouseKeepingService();

    public static enum Action {
        RUN, SUSPEND, RESUME, KILL, CHANGE
    }

    protected OozieWorkflowEngineAction() {
        super();
    }

    public OozieWorkflowEngineAction(Action action, String cluster, String jobId) {
        super(action.name());
        setPayload(new Payload(CLUSTER_NAME_KEY, cluster, JOB_ID_KEY, jobId));
    }

    // Collects info for change rollback
    public void preparePayload() throws IvoryException {
        String cluster = getCluster();
        String jobId = getJobId();
        if (getAction() == Action.CHANGE) {
            OozieClient client = OozieClientFactory.get(cluster);
            StringBuilder builder = new StringBuilder();
            try {
                if (jobId.endsWith("-B")) { // bundle
                    BundleJob bundle = client.getBundleJobInfo(jobId);
                    builder.append(OozieClient.CHANGE_VALUE_ENDTIME).append('=').append(bundle.getEndTime());
                } else if (jobId.endsWith("-C")) { // coord
                    CoordinatorJob coord = client.getCoordJobInfo(jobId);
                    builder.append(OozieClient.CHANGE_VALUE_CONCURRENCY).append('=').append(coord.getConcurrency()).append(';');
                    builder.append(OozieClient.CHANGE_VALUE_ENDTIME).append('=').append(EntityUtil.formatDateUTC(coord.getEndTime()));
                    //pause time can't be rolled back as pause time > now
//                    builder.append(OozieClient.CHANGE_VALUE_PAUSETIME).append('=')
//                            .append(coord.getPauseTime() == null ? "" : DateUtils.formatDateUTC(coord.getPauseTime()));
                }
            } catch (Exception e) {
                throw new IvoryException(e);
            }
            getPayload().add(CHANGE_VALUE_KEY, builder.toString());
        }
    }

    private String getJobId() {
        return getPayload().get(JOB_ID_KEY);
    }

    private Action getAction() {
        return Action.valueOf(getCategory());
    }

    private String getCluster() throws IvoryException {
        return getPayload().get(CLUSTER_NAME_KEY);
    }

    @Override
    public void rollback() throws IvoryException {
        OozieWorkflowEngine workflowEngine = new OozieWorkflowEngine();
        String cluster = getCluster();
        String jobId = getJobId();
        switch (getAction()) {
            case RUN:
                workflowEngine.kill(cluster, jobId);
                break;

            case SUSPEND:
                workflowEngine.resume(cluster, jobId);
                break;

            case RESUME:
                workflowEngine.suspend(cluster, jobId);
                break;

            case KILL:
                if(jobId.endsWith("-B") || jobId.endsWith("-C"))
                    throw new IvoryException("Can't rollback bundle/coord kill");
                
                workflowEngine.reRun(cluster, jobId, null);
                break;

            case CHANGE:
                workflowEngine.change(cluster, jobId, getPayload().get(CHANGE_VALUE_KEY));
                break;
        }
    }

    @Override
    public void commit() throws IvoryException {
        String cluster = getCluster();
        String jobId = getJobId();
        switch (getAction()) {
            case RUN:
                listener.afterSchedule(cluster, jobId);
                break;

            case SUSPEND:
                listener.afterSuspend(cluster, jobId);
                break;

            case RESUME:
                listener.afterResume(cluster, jobId);
                break;

            case KILL:
                listener.afterDelete(cluster, jobId);
                break;
        }
    }
}