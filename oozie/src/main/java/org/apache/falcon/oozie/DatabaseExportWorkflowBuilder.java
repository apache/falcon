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
import org.apache.falcon.entity.DatasourceHelper;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.datasource.Datasource;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.LoadMethod;
import org.apache.falcon.oozie.workflow.ACTION;
import org.apache.falcon.oozie.workflow.WORKFLOWAPP;
import org.apache.falcon.workflow.WorkflowExecutionContext;

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
    protected Properties getWorkflow(Cluster cluster, WORKFLOWAPP workflow) throws FalconException {

        addLibExtensionsToWorkflow(cluster, workflow, Tag.EXPORT);

        ACTION sqoopExport = unmarshalAction(EXPORT_SQOOP_ACTION_TEMPLATE);
        addTransition(sqoopExport, SUCCESS_POSTPROCESS_ACTION_NAME, FAIL_POSTPROCESS_ACTION_NAME);
        workflow.getDecisionOrForkOrJoin().add(sqoopExport);

        //Add post-processing actions
        ACTION success = getSuccessPostProcessAction();
        // delete addHDFSServersConfig(success, src, target);
        addTransition(success, OK_ACTION_NAME, FAIL_ACTION_NAME);
        workflow.getDecisionOrForkOrJoin().add(success);

        ACTION fail = getFailPostProcessAction();
        // delete addHDFSServersConfig(fail, src, target);
        addTransition(fail, FAIL_ACTION_NAME, FAIL_ACTION_NAME);
        workflow.getDecisionOrForkOrJoin().add(fail);

        decorateWorkflow(workflow, workflow.getName(), EXPORT_ACTION_NAME);
        addLibExtensionsToWorkflow(cluster, workflow, Tag.EXPORT);

        // build the sqoop command and put it in the properties
        String sqoopCmd = buildSqoopCommand(cluster, entity);
        LOG.info("SQOOP EXPORT COMMAND : " + sqoopCmd);
        Properties props = new Properties();
        props.put("sqoopCommand", sqoopCmd);
        return props;
    }

    private String buildSqoopCommand(Cluster cluster, Feed feed) throws FalconException {
        Map<String, String> extraArgs = getArguments(cluster);
        StringBuilder sqoopArgs = new StringBuilder();
        StringBuilder sqoopOptions = new StringBuilder();

        buildConnectArg(sqoopArgs, cluster).append(ImportExportCommon.ARG_SEPARATOR);
        buildTableArg(sqoopArgs, cluster).append(ImportExportCommon.ARG_SEPARATOR);
        ImportExportCommon.buildUserPasswordArg(sqoopArgs, sqoopOptions, cluster, entity)
                .append(ImportExportCommon.ARG_SEPARATOR);
        buildNumMappers(sqoopArgs, extraArgs).append(ImportExportCommon.ARG_SEPARATOR);
        buildArguments(sqoopArgs, extraArgs).append(ImportExportCommon.ARG_SEPARATOR);
        buildLoadType(sqoopArgs, cluster).append(ImportExportCommon.ARG_SEPARATOR);
        buildExportDirArg(sqoopArgs, cluster).append(ImportExportCommon.ARG_SEPARATOR);

        StringBuffer sqoopCmd = new StringBuffer();
        return sqoopCmd.append("export").append(ImportExportCommon.ARG_SEPARATOR)
                .append(sqoopOptions).append(ImportExportCommon.ARG_SEPARATOR)
                .append(sqoopArgs).toString();
    }

    private StringBuilder buildDriverArgs(StringBuilder builder, Cluster cluster) throws FalconException {
        org.apache.falcon.entity.v0.feed.Cluster feedCluster = FeedHelper.getCluster(entity, cluster.getName());
        Datasource db = DatasourceHelper.getDatasource(FeedHelper.getExportDatasourceName(feedCluster));
        if ((db.getDriver() != null) && (db.getDriver().getClazz() != null)) {
            builder.append("--driver").append(ImportExportCommon.ARG_SEPARATOR).append(db.getDriver().getClazz());
        }
        return builder;
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

    private StringBuilder buildExportDirArg(StringBuilder builder, Cluster cluster)
        throws FalconException {
        return builder.append("--export-dir").append(ImportExportCommon.ARG_SEPARATOR)
                .append(String.format("${coord:dataIn('%s')}",
                        FeedExportCoordinatorBuilder.EXPORT_DATAIN_NAME));
    }

    private StringBuilder buildArguments(StringBuilder builder, Map<String, String> extraArgs)
        throws FalconException {
        for(Map.Entry<String, String> e : extraArgs.entrySet()) {
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
}
