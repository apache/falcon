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

package org.apache.falcon.oozie;

import org.apache.falcon.FalconException;
import org.apache.falcon.LifeCycle;
import org.apache.falcon.Tag;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.ExternalId;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.oozie.coordinator.CONFIGURATION;
import org.apache.falcon.oozie.coordinator.CONFIGURATION.Property;
import org.apache.falcon.oozie.coordinator.COORDINATORAPP;
import org.apache.falcon.oozie.coordinator.ObjectFactory;
import org.apache.falcon.oozie.feed.FeedReplicationCoordinatorBuilder;
import org.apache.falcon.oozie.feed.FeedRetentionCoordinatorBuilder;
import org.apache.falcon.oozie.process.ProcessExecutionCoordinatorBuilder;
import org.apache.falcon.util.OozieUtils;
import org.apache.falcon.util.StartupProperties;
import org.apache.falcon.workflow.WorkflowExecutionArgs;
import org.apache.falcon.workflow.WorkflowExecutionContext;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.OozieClient;

import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

/**
 * Base class for building oozie coordinator.
 * @param <T>
 */
public abstract class OozieCoordinatorBuilder<T extends Entity> extends OozieEntityBuilder<T> {
    protected static final String NOMINAL_TIME_EL = "${coord:formatTime(coord:nominalTime(), 'yyyy-MM-dd-HH-mm')}";
    protected static final String ACTUAL_TIME_EL = "${coord:formatTime(coord:actualTime(), 'yyyy-MM-dd-HH-mm')}";
    protected static final Long DEFAULT_BROKER_MSG_TTL = 3 * 24 * 60L;
    protected static final String MR_QUEUE_NAME = "queueName";
    protected static final String MR_JOB_PRIORITY = "jobPriority";

    protected static final String IGNORE = "IGNORE";
    protected final LifeCycle lifecycle;

    public OozieCoordinatorBuilder(T entity, LifeCycle lifecycle) {
        super(entity);
        this.lifecycle = lifecycle;
    }

    public LifeCycle getLifecycle() {
        return lifecycle;
    }

    public Tag getTag() {
        return lifecycle.getTag();
    }

    public static OozieCoordinatorBuilder get(Entity entity, Tag tag) {
        switch(entity.getEntityType()) {
        case FEED:
            switch (tag) {
            case RETENTION:
                return new FeedRetentionCoordinatorBuilder((Feed)entity);

            case REPLICATION:
                return new FeedReplicationCoordinatorBuilder((Feed)entity);

            default:
                throw new IllegalArgumentException("Unhandled type " + entity.getEntityType() + ", lifecycle " + tag);
            }

        case PROCESS:
            return new ProcessExecutionCoordinatorBuilder((org.apache.falcon.entity.v0.process.Process) entity);

        default:
            break;
        }

        throw new IllegalArgumentException("Unhandled type " + entity.getEntityType() + ", lifecycle " + tag);
    }

    protected Path getBuildPath(Path buildPath) {
        return new Path(buildPath, getTag().name());
    }

    protected String getEntityName() {
        return EntityUtil.getWorkflowName(getTag(), entity).toString();
    }

    protected Path marshal(Cluster cluster, COORDINATORAPP coord,
                           Path outPath) throws FalconException {
        return marshal(cluster, new ObjectFactory().createCoordinatorApp(coord),
            OozieUtils.COORD_JAXB_CONTEXT, new Path(outPath, "coordinator.xml"));
    }

    protected Properties createCoordDefaultConfiguration(Cluster cluster,
                                                         String coordName)  throws FalconException {
        Properties props = new Properties();
        props.put(WorkflowExecutionArgs.ENTITY_NAME.getName(), entity.getName());
        props.put(WorkflowExecutionArgs.ENTITY_TYPE.getName(), entity.getEntityType().name());
        props.put(WorkflowExecutionArgs.CLUSTER_NAME.getName(), cluster.getName());
        props.put(WorkflowExecutionArgs.NOMINAL_TIME.getName(), NOMINAL_TIME_EL);
        props.put(WorkflowExecutionArgs.TIMESTAMP.getName(), ACTUAL_TIME_EL);
        props.put("falconDataOperation", getOperation().name());

        props.put(WorkflowExecutionArgs.LOG_DIR.getName(),
                getStoragePath(EntityUtil.getLogPath(cluster, entity)));
        props.put(OozieClient.EXTERNAL_ID,
            new ExternalId(entity.getName(), EntityUtil.getWorkflowNameTag(coordName, entity),
                "${coord:nominalTime()}").getId());
        props.put(WorkflowExecutionArgs.WF_ENGINE_URL.getName(), ClusterHelper.getOozieUrl(cluster));

        addLateDataProperties(props);
        addBrokerProperties(cluster, props);

        props.put(MR_QUEUE_NAME, "default");
        props.put(MR_JOB_PRIORITY, "NORMAL");

        //props in entity override the set props.
        props.putAll(getEntityProperties(entity));
        return props;
    }

    protected abstract WorkflowExecutionContext.EntityOperations getOperation();

    private void addLateDataProperties(Properties props) throws FalconException {
        if (EntityUtil.getLateProcess(entity) == null
            || EntityUtil.getLateProcess(entity).getLateInputs() == null
            || EntityUtil.getLateProcess(entity).getLateInputs().size() == 0) {
            props.put("shouldRecord", "false");
        } else {
            props.put("shouldRecord", "true");
        }
    }

    private void addBrokerProperties(Cluster cluster, Properties props) {
        props.put(WorkflowExecutionArgs.USER_BRKR_URL.getName(),
                ClusterHelper.getMessageBrokerUrl(cluster));
        props.put(WorkflowExecutionArgs.USER_BRKR_IMPL_CLASS.getName(),
                ClusterHelper.getMessageBrokerImplClass(cluster));

        String falconBrokerUrl = StartupProperties.get().getProperty(
                "broker.url", "tcp://localhost:61616?daemon=true");
        props.put(WorkflowExecutionArgs.BRKR_URL.getName(), falconBrokerUrl);

        String falconBrokerImplClass = StartupProperties.get().getProperty(
                "broker.impl.class", ClusterHelper.DEFAULT_BROKER_IMPL_CLASS);
        props.put(WorkflowExecutionArgs.BRKR_IMPL_CLASS.getName(), falconBrokerImplClass);

        String jmsMessageTTL = StartupProperties.get().getProperty("broker.ttlInMins",
            DEFAULT_BROKER_MSG_TTL.toString());
        props.put(WorkflowExecutionArgs.BRKR_TTL.getName(), jmsMessageTTL);
    }

    protected CONFIGURATION getConfig(Properties props) {
        CONFIGURATION conf = new CONFIGURATION();
        for (Entry<Object, Object> prop : props.entrySet()) {
            Property confProp = new Property();
            confProp.setName((String) prop.getKey());
            confProp.setValue((String) prop.getValue());
            conf.getProperty().add(confProp);
        }
        return conf;
    }

    public final Properties build(Cluster cluster, Path buildPath) throws FalconException {
        throw new IllegalStateException("Not implemented for coordinator!");
    }

    public abstract List<Properties> buildCoords(Cluster cluster, Path buildPath) throws FalconException;

    protected COORDINATORAPP unmarshal(String template) throws FalconException {
        return unmarshal(template, OozieUtils.COORD_JAXB_CONTEXT, COORDINATORAPP.class);
    }

    @Override
    protected Path getLibPath(Cluster cluster, Path buildPath) throws FalconException {
        return super.getLibPath(cluster, buildPath.getParent());
    }
}
