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
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.ExternalId;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.oozie.coordinator.CONFIGURATION;
import org.apache.falcon.oozie.coordinator.COORDINATORAPP;
import org.apache.falcon.oozie.coordinator.ObjectFactory;
import org.apache.falcon.oozie.feed.FeedReplicationCoordinatorBuilder;
import org.apache.falcon.oozie.feed.FeedRetentionCoordinatorBuilder;
import org.apache.falcon.oozie.process.ProcessExecutionCoordinatorBuilder;
import org.apache.falcon.util.OozieUtils;
import org.apache.falcon.workflow.WorkflowExecutionArgs;
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

    private static final Object USER_JMS_NOTIFICATION_ENABLED = "userJMSNotificationEnabled";
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

            case IMPORT:
                return new FeedImportCoordinatorBuilder((Feed)entity);

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

    protected Properties createCoordDefaultConfiguration(String coordName)  throws FalconException {
        Properties props = new Properties();
        props.put(WorkflowExecutionArgs.NOMINAL_TIME.getName(), NOMINAL_TIME_EL);
        props.put(WorkflowExecutionArgs.TIMESTAMP.getName(), ACTUAL_TIME_EL);
        props.put(OozieClient.EXTERNAL_ID,
            new ExternalId(entity.getName(), EntityUtil.getWorkflowNameTag(coordName, entity),
                "${coord:nominalTime()}").getId());
        props.put(USER_JMS_NOTIFICATION_ENABLED, "true");

        return props;
    }

    public final Properties build(Cluster cluster, Path buildPath) throws FalconException {
        throw new IllegalStateException("Not implemented for coordinator!");
    }

    public abstract List<Properties> buildCoords(Cluster cluster, Path buildPath) throws FalconException;

    protected COORDINATORAPP unmarshal(String template) throws FalconException {
        return unmarshal(template, OozieUtils.COORD_JAXB_CONTEXT, COORDINATORAPP.class);
    }

    protected CONFIGURATION getConfig(Properties props) {
        CONFIGURATION conf = new CONFIGURATION();
        for (Entry<Object, Object> prop : props.entrySet()) {
            CONFIGURATION.Property confProp = new CONFIGURATION.Property();
            confProp.setName((String) prop.getKey());
            confProp.setValue((String) prop.getValue());
            conf.getProperty().add(confProp);
        }
        return conf;
    }
}
