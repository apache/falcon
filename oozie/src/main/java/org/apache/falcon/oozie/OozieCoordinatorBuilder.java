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
import org.apache.falcon.Tag;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.ExternalId;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.messaging.EntityInstanceMessage.ARG;
import org.apache.falcon.oozie.coordinator.CONFIGURATION;
import org.apache.falcon.oozie.coordinator.CONFIGURATION.Property;
import org.apache.falcon.oozie.coordinator.COORDINATORAPP;
import org.apache.falcon.oozie.coordinator.ObjectFactory;
import org.apache.falcon.oozie.feed.FeedReplicationCoordinatorBuilder;
import org.apache.falcon.oozie.feed.FeedRetentionCoordinatorBuilder;
import org.apache.falcon.oozie.process.ProcessExecutionCoordinatorBuilder;
import org.apache.falcon.util.OozieUtils;
import org.apache.falcon.util.StartupProperties;
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
    protected final Tag lifecycle;

    public OozieCoordinatorBuilder(T entity, Tag tag) {
        super(entity);
        this.lifecycle = tag;
    }

    public static final OozieCoordinatorBuilder get(Entity entity, Tag tag) {
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
        return new Path(buildPath, lifecycle.name());
    }

    protected String getEntityName() {
        return EntityUtil.getWorkflowName(lifecycle, entity).toString();
    }

    protected Path marshal(Cluster cluster, COORDINATORAPP coord, Path outPath) throws FalconException {
        return marshal(cluster, new ObjectFactory().createCoordinatorApp(coord),
            OozieUtils.COORD_JAXB_CONTEXT, new Path(outPath, "coordinator.xml"));
    }

    protected Properties createCoordDefaultConfiguration(Cluster cluster, String coordName)  throws FalconException {
        Properties props = new Properties();
        props.put(ARG.entityName.getPropName(), entity.getName());
        props.put(ARG.nominalTime.getPropName(), NOMINAL_TIME_EL);
        props.put(ARG.timeStamp.getPropName(), ACTUAL_TIME_EL);
        props.put("userBrokerUrl", ClusterHelper.getMessageBrokerUrl(cluster));
        props.put("userBrokerImplClass", ClusterHelper.getMessageBrokerImplClass(cluster));
        String falconBrokerUrl = StartupProperties.get().getProperty(ARG.brokerUrl.getPropName(),
            "tcp://localhost:61616?daemon=true");
        props.put(ARG.brokerUrl.getPropName(), falconBrokerUrl);
        String falconBrokerImplClass = StartupProperties.get().getProperty(ARG.brokerImplClass.getPropName(),
            ClusterHelper.DEFAULT_BROKER_IMPL_CLASS);
        props.put(ARG.brokerImplClass.getPropName(), falconBrokerImplClass);
        String jmsMessageTTL = StartupProperties.get().getProperty("broker.ttlInMins",
            DEFAULT_BROKER_MSG_TTL.toString());
        props.put(ARG.brokerTTL.getPropName(), jmsMessageTTL);
        props.put(ARG.entityType.getPropName(), entity.getEntityType().name());
        props.put("logDir", getLogDirectory(cluster));
        props.put(OozieClient.EXTERNAL_ID,
            new ExternalId(entity.getName(), EntityUtil.getWorkflowNameTag(coordName, entity),
                "${coord:nominalTime()}").getId());
        props.put("workflowEngineUrl", ClusterHelper.getOozieUrl(cluster));

        if (EntityUtil.getLateProcess(entity) == null
            || EntityUtil.getLateProcess(entity).getLateInputs() == null
            || EntityUtil.getLateProcess(entity).getLateInputs().size() == 0) {
            props.put("shouldRecord", "false");
        } else {
            props.put("shouldRecord", "true");
        }

        props.put("entityName", entity.getName());
        props.put("entityType", entity.getEntityType().name().toLowerCase());
        props.put(ARG.cluster.getPropName(), cluster.getName());

        props.put(MR_QUEUE_NAME, "default");
        props.put(MR_JOB_PRIORITY, "NORMAL");
        //props in entity override the set props.
        props.putAll(getEntityProperties(entity));
        return props;
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

}
