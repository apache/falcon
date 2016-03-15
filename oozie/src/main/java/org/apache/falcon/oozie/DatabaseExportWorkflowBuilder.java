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

import com.google.common.base.Splitter;
import org.apache.falcon.FalconException;
import org.apache.falcon.Tag;
import org.apache.falcon.entity.DatasourceHelper;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.Storage;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.datasource.Datasource;
import org.apache.falcon.entity.v0.feed.CatalogTable;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.LoadMethod;
import org.apache.falcon.oozie.workflow.ACTION;
import org.apache.falcon.oozie.workflow.WORKFLOWAPP;
import org.apache.falcon.util.OozieUtils;
import org.apache.falcon.workflow.WorkflowExecutionContext;
import org.apache.hadoop.fs.Path;

import javax.xml.bind.JAXBElement;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * Builds Datasource export workflow for Oozie.
 */

public class DatabaseExportWorkflowBuilder extends ExportWorkflowBuilder {
    protected static final String EXPORT_SQOOP_ACTION_TEMPLATE = "/action/feed/export-sqoop-database-action.xml";
    protected static final String EXPORT_ACTION_NAME="db-export-sqoop";

    public DatabaseExportWorkflowBuilder(Feed entity) { super(entity); }

    @Override
    protected WorkflowExecutionContext.EntityOperations getOperation() {
        return WorkflowExecutionContext.EntityOperations.EXPORT;
    }

    @Override
    protected Properties getWorkflow(Cluster cluster, WORKFLOWAPP workflow, Path buildPath)
        throws FalconException {

        ACTION action = unmarshalAction(EXPORT_SQOOP_ACTION_TEMPLATE);
        JAXBElement<org.apache.falcon.oozie.sqoop.ACTION> actionJaxbElement = OozieUtils.unMarshalSqoopAction(action);
        org.apache.falcon.oozie.sqoop.ACTION sqoopExport = actionJaxbElement.getValue();

        Properties props = new Properties();
        ImportExportCommon.addHCatalogProperties(props, entity, cluster, workflow, this, buildPath);
        sqoopExport.getJobXml().add("${wf:appPath()}/conf/hive-site.xml");
        OozieUtils.marshalSqoopAction(action, actionJaxbElement);

        addTransition(action, SUCCESS_POSTPROCESS_ACTION_NAME, FAIL_POSTPROCESS_ACTION_NAME);
        workflow.getDecisionOrForkOrJoin().add(action);

        //Add post-processing actions
        ACTION success = getSuccessPostProcessAction();
        addTransition(success, OK_ACTION_NAME, FAIL_ACTION_NAME);
        workflow.getDecisionOrForkOrJoin().add(success);

        ACTION fail = getFailPostProcessAction();
        addTransition(fail, FAIL_ACTION_NAME, FAIL_ACTION_NAME);
        workflow.getDecisionOrForkOrJoin().add(fail);

        decorateWorkflow(workflow, workflow.getName(), EXPORT_ACTION_NAME);
        addLibExtensionsToWorkflow(cluster, workflow, Tag.EXPORT);

        // build the sqoop command and put it in the properties
        String sqoopCmd = buildSqoopCommand(cluster, entity);
        LOG.info("SQOOP EXPORT COMMAND : " + sqoopCmd);
        props.put("sqoopCommand", sqoopCmd);
        return props;
    }

    private String buildSqoopCommand(Cluster cluster, Feed feed) throws FalconException {
        Map<String, String> extraArgs = getArguments(cluster);
        StringBuilder sqoopArgs = new StringBuilder();
        StringBuilder sqoopOptions = new StringBuilder();

        buildConnectArg(sqoopArgs, cluster).append(ImportExportCommon.ARG_SEPARATOR);
        buildTableArg(sqoopArgs, cluster).append(ImportExportCommon.ARG_SEPARATOR);
        Datasource datasource = DatasourceHelper.getDatasource(FeedHelper.getExportDatasourceName(
                FeedHelper.getCluster(entity, cluster.getName())));
        ImportExportCommon.buildUserPasswordArg(sqoopArgs, sqoopOptions, datasource)
                .append(ImportExportCommon.ARG_SEPARATOR);
        buildNumMappers(sqoopArgs, extraArgs).append(ImportExportCommon.ARG_SEPARATOR);
        buildArguments(sqoopArgs, extraArgs, feed, cluster).append(ImportExportCommon.ARG_SEPARATOR);
        buildLoadType(sqoopArgs, cluster).append(ImportExportCommon.ARG_SEPARATOR);
        buildExportArg(sqoopArgs, feed, cluster).append(ImportExportCommon.ARG_SEPARATOR);

        StringBuilder sqoopCmd = new StringBuilder();
        return sqoopCmd.append("export").append(ImportExportCommon.ARG_SEPARATOR)
                .append(sqoopOptions).append(ImportExportCommon.ARG_SEPARATOR)
                .append(sqoopArgs).toString();
    }

    private StringBuilder buildConnectArg(StringBuilder builder, Cluster cluster) throws FalconException {
        org.apache.falcon.entity.v0.feed.Cluster feedCluster = FeedHelper.getCluster(entity, cluster.getName());
        return builder.append("--connect").append(ImportExportCommon.ARG_SEPARATOR)
                .append(DatasourceHelper.getReadOnlyEndpoint(
                        DatasourceHelper.getDatasource(FeedHelper.getExportDatasourceName(feedCluster))));
    }

    private StringBuilder buildTableArg(StringBuilder builder, Cluster cluster) throws FalconException {
        org.apache.falcon.entity.v0.feed.Cluster feedCluster = FeedHelper.getCluster(entity, cluster.getName());
        return builder.append("--table").append(ImportExportCommon.ARG_SEPARATOR)
                .append(FeedHelper.getExportDataSourceTableName(feedCluster));
    }

    private StringBuilder buildLoadType(StringBuilder builder, Cluster cluster)
        throws FalconException {
        org.apache.falcon.entity.v0.feed.Cluster feedCluster = FeedHelper.getCluster(entity, cluster.getName());
        builder.append("--update-mode").append(ImportExportCommon.ARG_SEPARATOR);
        String modeType = LoadMethod.UPDATEONLY.value();
        if (FeedHelper.getExportLoadMethod(feedCluster).getType() != null) {
            modeType = FeedHelper.getExportLoadMethod(feedCluster).getType().value();
        }
        return builder.append(modeType);
    }

    private StringBuilder buildExportArg(StringBuilder builder, Feed feed, Cluster cluster)
        throws FalconException {
        Storage.TYPE feedStorageType = FeedHelper.getStorageType(feed, cluster);
        if (feedStorageType == Storage.TYPE.TABLE) {
            return buildExportTableArg(builder, feed.getTable());
        } else {
            return buildExportDirArg(builder, cluster);
        }
    }

    private StringBuilder buildExportDirArg(StringBuilder builder, Cluster cluster)
        throws FalconException {
        return builder.append("--export-dir").append(ImportExportCommon.ARG_SEPARATOR)
            .append(String.format("${coord:dataIn('%s')}",
                FeedExportCoordinatorBuilder.EXPORT_DATAIN_NAME));
    }

    private StringBuilder buildArguments(StringBuilder builder, Map<String, String> extraArgs, Feed feed,
        Cluster cluster) throws FalconException {
        Storage.TYPE feedStorageType = FeedHelper.getStorageType(feed, cluster);
        for(Map.Entry<String, String> e : extraArgs.entrySet()) {
            if ((feedStorageType == Storage.TYPE.TABLE) && (e.getKey().equals("--update-key"))) {
                continue;
            }
            builder.append(e.getKey()).append(ImportExportCommon.ARG_SEPARATOR).append(e.getValue())
                .append(ImportExportCommon.ARG_SEPARATOR);
        }
        return builder;
    }

    /**
     *
     * Feed validation checks to make sure --split-by column is supplied when --num-mappers > 1
     * if --num-mappers is not specified, set it to 1.
     *
     * @param builder contains command
     * @param extraArgs map of extra arguments
     * @return command string
     */

    private StringBuilder buildNumMappers(StringBuilder builder, Map<String, String> extraArgs) {
        if (!extraArgs.containsKey("--num-mappers")) {
            builder.append("--num-mappers").append(ImportExportCommon.ARG_SEPARATOR).append(1);
        }
        return builder;
    }

    private Map<String, String> getArguments(Cluster cluster) throws FalconException {
        org.apache.falcon.entity.v0.feed.Cluster feedCluster = FeedHelper.getCluster(entity, cluster.getName());
        return FeedHelper.getExportArguments(feedCluster);
    }

    private StringBuilder buildExportTableArg(StringBuilder builder, CatalogTable catalog) throws FalconException {

        LOG.info("Catalog URI {}", catalog.getUri());
        builder.append("--skip-dist-cache").append(ImportExportCommon.ARG_SEPARATOR);
        Iterator<String> itr = Splitter.on("#").split(catalog.getUri()).iterator();
        String dbTable = itr.next();
        String partitions = itr.next();
        Iterator<String> itrDbTable = Splitter.on(":").split(dbTable).iterator();
        itrDbTable.next();
        String db = itrDbTable.next();
        String table = itrDbTable.next();
        LOG.debug("Target database {}, table {}", db, table);
        builder.append("--hcatalog-database").append(ImportExportCommon.ARG_SEPARATOR)
                .append(String.format("${coord:databaseIn('%s')}", FeedExportCoordinatorBuilder.EXPORT_DATAIN_NAME))
                .append(ImportExportCommon.ARG_SEPARATOR);

        builder.append("--hcatalog-table").append(ImportExportCommon.ARG_SEPARATOR)
                .append(String.format("${coord:tableIn('%s')}", FeedExportCoordinatorBuilder.EXPORT_DATAIN_NAME))
                .append(ImportExportCommon.ARG_SEPARATOR);

        Map<String, String> partitionsMap = ImportExportCommon.getPartitionKeyValues(partitions);
        if (partitionsMap.size() > 0) {
            StringBuilder partitionKeys = new StringBuilder();
            StringBuilder partitionValues = new StringBuilder();
            for (Map.Entry<String, String> e : partitionsMap.entrySet()) {
                partitionKeys.append(e.getKey());
                partitionKeys.append(',');
                partitionValues.append(String.format("${coord:dataInPartitionMin('%s','%s')}",
                        FeedExportCoordinatorBuilder.EXPORT_DATAIN_NAME,
                        e.getKey()));
                partitionValues.append(',');
            }
            if (partitionsMap.size() > 0) {
                partitionKeys.setLength(partitionKeys.length()-1);
                partitionValues.setLength(partitionValues.length()-1);
            }
            LOG.debug("partitionKeys {} and partitionValue {}", partitionKeys.toString(), partitionValues.toString());
            builder.append("--hcatalog-partition-keys").append(ImportExportCommon.ARG_SEPARATOR)
                    .append(partitionKeys.toString()).append(ImportExportCommon.ARG_SEPARATOR);
            builder.append("--hcatalog-partition-values").append(ImportExportCommon.ARG_SEPARATOR)
                    .append(partitionValues.toString()).append(ImportExportCommon.ARG_SEPARATOR);
        }
        return builder;
    }
}

