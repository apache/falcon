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

package org.apache.falcon.metadata;

import com.thinkaurelius.titan.graphdb.blueprints.TitanBlueprintsGraph;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphFactory;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.TransactionRetryHelper;
import com.tinkerpop.blueprints.util.TransactionWork;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.service.ConfigurationChangeListener;
import org.apache.falcon.service.FalconService;
import org.apache.falcon.service.Services;
import org.apache.falcon.util.StartupProperties;
import org.apache.falcon.workflow.WorkflowJobEndNotificationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.falcon.workflow.WorkflowExecutionContext;
import org.apache.falcon.workflow.WorkflowExecutionListener;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Metadata relationship mapping service. Maps relationships into a graph database.
 */
public class MetadataMappingService
        implements FalconService, ConfigurationChangeListener, WorkflowExecutionListener {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataMappingService.class);

    /**
     * Constance for the service name.
     */
    public static final String SERVICE_NAME = MetadataMappingService.class.getSimpleName();

    /**
     * Constant for the configuration property that indicates the prefix.
     */
    private static final String FALCON_PREFIX = "falcon.graph.";


    private Graph graph;
    private Set<String> vertexIndexedKeys;
    private Set<String> edgeIndexedKeys;
    private EntityRelationshipGraphBuilder entityGraphBuilder;
    private InstanceRelationshipGraphBuilder instanceGraphBuilder;

    private int transactionRetries;
    private long transactionRetryDelayInMillis;

    @Override
    public String getName() {
        return SERVICE_NAME;
    }

    @Override
    public void init() throws FalconException {
        graph = initializeGraphDB();
        createIndicesForVertexKeys();
        // todo - create Edge Cardinality Constraints
        LOG.info("Initialized graph db: {}", graph);

        vertexIndexedKeys = getIndexableGraph().getIndexedKeys(Vertex.class);
        LOG.info("Init vertex property keys: {}", vertexIndexedKeys);

        edgeIndexedKeys = getIndexableGraph().getIndexedKeys(Edge.class);
        LOG.info("Init edge property keys: {}", edgeIndexedKeys);

        boolean preserveHistory = Boolean.valueOf(StartupProperties.get().getProperty(
                "falcon.graph.preserve.history", "false"));
        entityGraphBuilder = new EntityRelationshipGraphBuilder(graph, preserveHistory);
        instanceGraphBuilder = new InstanceRelationshipGraphBuilder(graph, preserveHistory);

        ConfigurationStore.get().registerListener(this);
        Services.get().<WorkflowJobEndNotificationService>getService(
                WorkflowJobEndNotificationService.SERVICE_NAME).registerListener(this);
        try {
            transactionRetries = Integer.parseInt(StartupProperties.get().getProperty(
                    "falcon.graph.transaction.retry.count", "3"));
            transactionRetryDelayInMillis = Long.parseLong(StartupProperties.get().getProperty(
                    "falcon.graph.transaction.retry.delay", "5"));
        } catch (NumberFormatException e) {
            throw new FalconException("Invalid values for graph transaction retry delay/count " + e);
        }
    }

    protected Graph initializeGraphDB() {
        LOG.info("Initializing graph db");

        Configuration graphConfig = getConfiguration();
        return GraphFactory.open(graphConfig);
    }

    public static Configuration getConfiguration() {
        Configuration graphConfig = new BaseConfiguration();

        Properties configProperties = StartupProperties.get();
        for (Map.Entry entry : configProperties.entrySet()) {
            String name = (String) entry.getKey();
            if (name.startsWith(FALCON_PREFIX)) {
                String value = (String) entry.getValue();
                name = name.substring(FALCON_PREFIX.length());
                graphConfig.setProperty(name, value);
            }
        }

        return graphConfig;
    }

    /**
     * This unfortunately requires a handle to Titan implementation since
     * com.tinkerpop.blueprints.KeyIndexableGraph#createKeyIndex does not create an index.
     */
    protected void createIndicesForVertexKeys() {
        if (!((KeyIndexableGraph) graph).getIndexedKeys(Vertex.class).isEmpty()) {
            LOG.info("Indexes already exist for graph");
            return;
        }

        LOG.info("Indexes does not exist, Creating indexes for graph");
        // todo - externalize this
        makeNameKeyIndex();
        makeKeyIndex(RelationshipProperty.TYPE.getName());
        makeKeyIndex(RelationshipProperty.TIMESTAMP.getName());
        makeKeyIndex(RelationshipProperty.VERSION.getName());
    }

    private void makeNameKeyIndex() {
        getTitanGraph().makeKey(RelationshipProperty.NAME.getName())
                .dataType(String.class)
                .indexed(Vertex.class)
                .indexed(Edge.class)
                // .unique() todo this ought to be unique?
                .make();
        getTitanGraph().commit();
    }

    private void makeKeyIndex(String key) {
        getTitanGraph().makeKey(key)
                .dataType(String.class)
                .indexed(Vertex.class)
                .make();
        getTitanGraph().commit();
    }

    public Graph getGraph() {
        return graph;
    }

    public KeyIndexableGraph getIndexableGraph() {
        return (KeyIndexableGraph) graph;
    }

    public TransactionalGraph getTransactionalGraph() {
        return (TransactionalGraph) graph;
    }

    public TitanBlueprintsGraph getTitanGraph() {
        return (TitanBlueprintsGraph) graph;
    }

    public Set<String> getVertexIndexedKeys() {
        return vertexIndexedKeys;
    }

    public Set<String> getEdgeIndexedKeys() {
        return edgeIndexedKeys;
    }

    @Override
    public void destroy() throws FalconException {
        Services.get().<WorkflowJobEndNotificationService>getService(
                WorkflowJobEndNotificationService.SERVICE_NAME).unregisterListener(this);

        LOG.info("Shutting down graph db");
        graph.shutdown();
    }

    @Override
    public void onAdd(final Entity entity) throws FalconException {
        EntityType entityType = entity.getEntityType();
        LOG.info("Adding lineage for entity: {}, type: {}", entity.getName(), entityType);
        try {
            new TransactionRetryHelper.Builder<Void>(getTransactionalGraph())
                    .perform(new TransactionWork<Void>() {
                        @Override
                        public Void execute(TransactionalGraph transactionalGraph) throws Exception {
                            entityGraphBuilder.addEntity(entity);
                            transactionalGraph.commit();
                            return null;
                        }
                    }).build().exponentialBackoff(transactionRetries, transactionRetryDelayInMillis);

        } catch (Exception e) {
            getTransactionalGraph().rollback();
            throw new FalconException(e);
        }
    }

    @Override
    public void onRemove(Entity entity) throws FalconException {
        // do nothing, we'd leave the deleted entities as-is for historical purposes
        // should we mark 'em as deleted?
    }

    @Override
    public void onChange(final Entity oldEntity, final Entity newEntity) throws FalconException {
        EntityType entityType = newEntity.getEntityType();
        LOG.info("Updating lineage for entity: {}, type: {}", newEntity.getName(), entityType);
        try {
            new TransactionRetryHelper.Builder<Void>(getTransactionalGraph())
                    .perform(new TransactionWork<Void>() {
                        @Override
                        public Void execute(TransactionalGraph transactionalGraph) throws Exception {
                            entityGraphBuilder.updateEntity(oldEntity, newEntity);
                            transactionalGraph.commit();
                            return null;
                        }
                    }).build().exponentialBackoff(transactionRetries, transactionRetryDelayInMillis);

        } catch (Exception e) {
            getTransactionalGraph().rollback();
            throw new FalconException(e);
        }
    }

    @Override
    public void onReload(Entity entity) throws FalconException {
        onAdd(entity);
    }

    @Override
    public void onSuccess(final WorkflowExecutionContext context) throws FalconException {
        LOG.info("Adding lineage for context {}", context);
        try {
            new TransactionRetryHelper.Builder<Void>(getTransactionalGraph())
                    .perform(new TransactionWork<Void>() {
                        @Override
                        public Void execute(TransactionalGraph transactionalGraph) throws Exception {
                            onSuccessfulExecution(context);
                            transactionalGraph.commit();
                            return null;
                        }
                    }).build().exponentialBackoff(transactionRetries, transactionRetryDelayInMillis);
        } catch (Exception e) {
            getTransactionalGraph().rollback();
            throw new FalconException(e);
        }
    }

    private void onSuccessfulExecution(final WorkflowExecutionContext context) throws FalconException {
        WorkflowExecutionContext.EntityOperations entityOperation = context.getOperation();
        switch (entityOperation) {
        case GENERATE:
            onProcessInstanceExecuted(context);
            break;
        case REPLICATE:
            onFeedInstanceReplicated(context);
            break;
        case DELETE:
            onFeedInstanceEvicted(context);
            break;
        case IMPORT:
            onFeedInstanceImported(context);
            break;
        case EXPORT:
            onFeedInstanceExported(context);
            break;
        default:
            throw new IllegalArgumentException("Invalid EntityOperation - " + entityOperation);
        }
    }

    @Override
    public void onFailure(WorkflowExecutionContext context) throws FalconException {
        // do nothing since lineage is only recorded for successful workflow
    }

    @Override
    public void onStart(WorkflowExecutionContext context) throws FalconException {
        // Do nothing
    }

    @Override
    public void onSuspend(WorkflowExecutionContext context) throws FalconException {
        // Do nothing
    }

    @Override
    public void onWait(WorkflowExecutionContext context) throws FalconException {
        // TBD
    }


    private void onProcessInstanceExecuted(WorkflowExecutionContext context) throws FalconException {
        Vertex processInstance = instanceGraphBuilder.addProcessInstance(context);
        instanceGraphBuilder.addOutputFeedInstances(context, processInstance);
        instanceGraphBuilder.addInputFeedInstances(context, processInstance);
    }

    private void onFeedInstanceReplicated(WorkflowExecutionContext context) throws FalconException {
        LOG.info("Adding replicated feed instance: {}", context.getNominalTimeAsISO8601());
        instanceGraphBuilder.addReplicatedInstance(context);
    }

    private void onFeedInstanceEvicted(WorkflowExecutionContext context) throws FalconException {
        LOG.info("Adding evicted feed instance: {}", context.getNominalTimeAsISO8601());
        instanceGraphBuilder.addEvictedInstance(context);
    }
    private void onFeedInstanceImported(WorkflowExecutionContext context) throws FalconException {
        LOG.info("Adding imported feed instance: {}", context.getNominalTimeAsISO8601());
        instanceGraphBuilder.addImportedInstance(context);
    }
    private void onFeedInstanceExported(WorkflowExecutionContext context) throws FalconException {
        LOG.info("Adding exported feed instance: {}", context.getNominalTimeAsISO8601());
        instanceGraphBuilder.addExportedInstance(context);
    }
}
