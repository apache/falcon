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

import org.apache.commons.lang.StringUtils;
import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.transaction.Action;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.util.DateUtils;

public class OozieWorkflowEngineAction extends Action {
    private static final String CLUSTER_NAME_KEY = "cluster";
    private static final String JOB_ID_KEY = "jobId";
    private static final String ENTITY_TYPE_KEY = "entityType";
    private static final String ENTITY_NAME_KEY = "entityName";
    private static final String CHANGE_VALUE_KEY = "changeValue";

    private static final WorkflowEngineActionListener listener = new OozieHouseKeepingService();

    public static enum Action {
        RUN, SUSPEND, RESUME, KILL, CHANGE
    }

    protected OozieWorkflowEngineAction() {
        super();
    }

    public OozieWorkflowEngineAction(Action action, Cluster cluster, String jobId) {
        this(action, cluster, jobId, null);
    }

    public OozieWorkflowEngineAction(Action action, Cluster cluster, String jobId, Entity entity) {
        super(action.name());
        Payload payload = new Payload(CLUSTER_NAME_KEY, cluster.getName(), JOB_ID_KEY, jobId);
        if (entity != null)
            payload.add(ENTITY_TYPE_KEY, entity.getEntityType().name(), ENTITY_NAME_KEY, entity.getName());
        setPayload(payload);
    }

    // Collects info for change rollback
    public void preparePayload() throws IvoryException {
        Cluster cluster = getCluster();
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
                    builder.append(OozieClient.CHANGE_VALUE_ENDTIME).append('=').append(DateUtils.formatDateUTC(coord.getEndTime()));
                    //pause time can't be rolled back
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

    private Cluster getCluster() throws IvoryException {
        return ConfigurationStore.get().get(EntityType.CLUSTER, getPayload().get(CLUSTER_NAME_KEY));
    }

    private Entity getEntity() throws IvoryException {
        String entityTypeStr = getPayload().get(ENTITY_TYPE_KEY);
        if (StringUtils.isNotEmpty(entityTypeStr)) {
            EntityType entityType = EntityType.valueOf(entityTypeStr);
            String entityName = getPayload().get(ENTITY_NAME_KEY);
            return ConfigurationStore.get().get(entityType, entityName);
        }
        return null;
    }

    @Override
    public void rollback() throws IvoryException {
        Cluster cluster = getCluster();
        Entity entity = getEntity();
        String jobId = getJobId();
        switch (getAction()) {
            case RUN:
                OozieWorkflowEngine.kill(cluster, jobId, entity);
                break;

            case SUSPEND:
                OozieWorkflowEngine.resume(cluster, jobId, entity);
                break;

            case RESUME:
                OozieWorkflowEngine.suspend(cluster, jobId, entity);
                break;

            case KILL:
                if(jobId.endsWith("-B") || jobId.endsWith("-C"))
                    throw new IvoryException("Can't rollback bundle/coord kill");
                
                OozieWorkflowEngine.reRun(cluster, jobId, null);
                break;

            case CHANGE:
                OozieWorkflowEngine.change(cluster, jobId, getPayload().get(CHANGE_VALUE_KEY));
                break;
        }
    }

    @Override
    public void commit() throws IvoryException {
        Entity entity = getEntity();
        if (entity == null)
            return;

        Cluster cluster = getCluster();
        switch (getAction()) {
            case RUN:
                listener.afterSchedule(cluster, entity);
                break;

            case SUSPEND:
                listener.afterSuspend(cluster, entity);
                break;

            case RESUME:
                listener.afterResume(cluster, entity);
                break;

            case KILL:
                break;
        }
    }
}