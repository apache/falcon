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

package org.apache.falcon.workflow;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;


/**
 * Captures the workflow execution context.
 */
public class WorkflowExecutionContext {

    private static final Logger LOG = LoggerFactory.getLogger(WorkflowExecutionContext.class);

    public static final String INSTANCE_FORMAT = "yyyy-MM-dd-HH-mm"; // nominal time

    public static final String OUTPUT_FEED_SEPARATOR = ",";
    public static final String INPUT_FEED_SEPARATOR = "#";
    public static final String CLUSTER_NAME_SEPARATOR = ",";

    /**
     * Workflow execution status.
     */
    public enum Status {WAITING, RUNNING, SUSPENDED, SUCCEEDED, FAILED, TIMEDOUT, KILLED}

    /**
     * Workflow execution type.
     */
    public enum Type {PRE_PROCESSING, POST_PROCESSING, WORKFLOW_JOB, COORDINATOR_ACTION}

    /**
     * Entity operations supported.
     */
    public enum EntityOperations {
        GENERATE, DELETE, ARCHIVE, REPLICATE, CHMOD, IMPORT
    }

    public static final WorkflowExecutionArgs[] USER_MESSAGE_ARGS = {
        WorkflowExecutionArgs.CLUSTER_NAME,
        WorkflowExecutionArgs.ENTITY_NAME,
        WorkflowExecutionArgs.ENTITY_TYPE,
        WorkflowExecutionArgs.NOMINAL_TIME,
        WorkflowExecutionArgs.OPERATION,

        WorkflowExecutionArgs.OUTPUT_FEED_NAMES,
        WorkflowExecutionArgs.OUTPUT_FEED_PATHS,

        WorkflowExecutionArgs.WORKFLOW_ID,
        WorkflowExecutionArgs.WORKFLOW_USER,
        WorkflowExecutionArgs.RUN_ID,
        WorkflowExecutionArgs.STATUS,
        WorkflowExecutionArgs.TIMESTAMP,
        WorkflowExecutionArgs.LOG_DIR,
    };

    private final Map<WorkflowExecutionArgs, String> context;
    private final long creationTime;
    private Configuration actionJobConf;

    public WorkflowExecutionContext(Map<WorkflowExecutionArgs, String> context) {
        this.context = context;
        creationTime = System.currentTimeMillis();
    }

    public String getValue(WorkflowExecutionArgs arg) {
        return context.get(arg);
    }

    public void setValue(WorkflowExecutionArgs arg, String value) {
        context.put(arg, value);
    }

    public String getValue(WorkflowExecutionArgs arg, String defaultValue) {
        return context.containsKey(arg) ? context.get(arg) : defaultValue;
    }

    public boolean containsKey(WorkflowExecutionArgs arg) {
        return context.containsKey(arg);
    }

    public Set<Map.Entry<WorkflowExecutionArgs, String>> entrySet() {
        return context.entrySet();
    }

    // helper methods
    public boolean hasWorkflowSucceeded() {
        return Status.SUCCEEDED.name().equals(getValue(WorkflowExecutionArgs.STATUS));
    }

    public boolean hasWorkflowFailed() {
        return Status.FAILED.name().equals(getValue(WorkflowExecutionArgs.STATUS));
    }

    public boolean isWorkflowKilledManually(){
        try {
            return WorkflowEngineFactory.getWorkflowEngine().
                    isWorkflowKilledByUser(
                            getValue(WorkflowExecutionArgs.CLUSTER_NAME),
                            getValue(WorkflowExecutionArgs.WORKFLOW_ID));
        } catch (Exception e) {
            LOG.error("Got Error in getting error codes from actions: " + e);
        }
        return false;
    }

    public boolean hasWorkflowTimedOut() {
        return Status.TIMEDOUT.name().equals(getValue(WorkflowExecutionArgs.STATUS));
    }

    public boolean hasWorkflowBeenKilled() {
        return Status.KILLED.name().equals(getValue(WorkflowExecutionArgs.STATUS));
    }

    public String getContextFile() {
        return getValue(WorkflowExecutionArgs.CONTEXT_FILE);
    }

    public Status getWorkflowStatus() {
        return Status.valueOf(getValue(WorkflowExecutionArgs.STATUS));
    }

    public String getLogDir() {
        return getValue(WorkflowExecutionArgs.LOG_DIR);
    }

    public String getLogFile() {
        return getValue(WorkflowExecutionArgs.LOG_FILE);
    }

    String getNominalTime() {
        return getValue(WorkflowExecutionArgs.NOMINAL_TIME);
    }

    /**
     * Returns nominal time as a ISO8601 formatted string.
     * @return a ISO8601 formatted string
     */
    public String getNominalTimeAsISO8601() {
        return SchemaHelper.formatDateUTCToISO8601(getNominalTime(), INSTANCE_FORMAT);
    }

    String getTimestamp() {
        return getValue(WorkflowExecutionArgs.TIMESTAMP);
    }

    /**
     * Returns timestamp as a long.
     * @return Date as long (milliseconds since epoch) for the timestamp.
     */
    public long getTimeStampAsLong() {
        String dateString = getTimestamp();
        try {
            DateFormat dateFormat = new SimpleDateFormat(INSTANCE_FORMAT.substring(0, dateString.length()));
            dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
            return dateFormat.parse(dateString).getTime();
        } catch (java.text.ParseException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns timestamp as a ISO8601 formatted string.
     * @return a ISO8601 formatted string
     */
    public String getTimeStampAsISO8601() {
        return SchemaHelper.formatDateUTCToISO8601(getTimestamp(), INSTANCE_FORMAT);
    }

    public String getClusterName() {
        String value =  getValue(WorkflowExecutionArgs.CLUSTER_NAME);
        if (EntityOperations.REPLICATE != getOperation()) {
            return value;
        }

        return value.split(CLUSTER_NAME_SEPARATOR)[0];
    }

    public String getSrcClusterName() {
        String value =  getValue(WorkflowExecutionArgs.CLUSTER_NAME);
        if (EntityOperations.REPLICATE != getOperation()) {
            return value;
        }

        String[] parts = value.split(CLUSTER_NAME_SEPARATOR);
        if (parts.length != 2) {
            throw new IllegalArgumentException("Replicated cluster pair is missing in " + value);
        }

        return parts[1];
    }

    public String getEntityName() {
        return getValue(WorkflowExecutionArgs.ENTITY_NAME);
    }

    public String getEntityType() {
        return getValue(WorkflowExecutionArgs.ENTITY_TYPE).toUpperCase();
    }

    public EntityOperations getOperation() {
        if (getValue(WorkflowExecutionArgs.OPERATION) != null) {
            return EntityOperations.valueOf(getValue(WorkflowExecutionArgs.OPERATION));
        }
        return EntityOperations.valueOf(getValue(WorkflowExecutionArgs.DATA_OPERATION));
    }

    public String getOutputFeedNames() {
        return getValue(WorkflowExecutionArgs.OUTPUT_FEED_NAMES);
    }

    public String[] getOutputFeedNamesList() {
        return getOutputFeedNames().split(OUTPUT_FEED_SEPARATOR);
    }

    public String getOutputFeedInstancePaths() {
        return getValue(WorkflowExecutionArgs.OUTPUT_FEED_PATHS);
    }

    public String[] getOutputFeedInstancePathsList() {
        return getOutputFeedInstancePaths().split(OUTPUT_FEED_SEPARATOR);
    }

    public String getInputFeedNames() {
        return getValue(WorkflowExecutionArgs.INPUT_FEED_NAMES);
    }

    public String[] getInputFeedNamesList() {
        return getInputFeedNames().split(INPUT_FEED_SEPARATOR);
    }

    public String getInputFeedInstancePaths() {
        return getValue(WorkflowExecutionArgs.INPUT_FEED_PATHS);
    }

    public String[] getInputFeedInstancePathsList() {
        return getInputFeedInstancePaths().split(INPUT_FEED_SEPARATOR);
    }

    public String getWorkflowEngineUrl() {
        return getValue(WorkflowExecutionArgs.WF_ENGINE_URL);
    }

    public String getUserWorkflowEngine() {
        return getValue(WorkflowExecutionArgs.USER_WORKFLOW_ENGINE);
    }

    public String getUserWorkflowVersion() {
        return getValue(WorkflowExecutionArgs.USER_WORKFLOW_VERSION);
    }

    public String getWorkflowId() {
        return getValue(WorkflowExecutionArgs.WORKFLOW_ID);
    }

    public String getUserSubflowId() {
        return getValue(WorkflowExecutionArgs.USER_SUBFLOW_ID);
    }

    public int getWorkflowRunId() {
        return Integer.parseInt(getValue(WorkflowExecutionArgs.RUN_ID));
    }

    public String getWorkflowRunIdString() {
        return String.valueOf(Integer.parseInt(getValue(WorkflowExecutionArgs.RUN_ID)));
    }

    public String getWorkflowUser() {
        return getValue(WorkflowExecutionArgs.WORKFLOW_USER);
    }

    public long getExecutionCompletionTime() {

        return creationTime;
    }

    public String getDatasourceName() { return getValue(WorkflowExecutionArgs.DATASOURCE_NAME); }

    public long getWorkflowStartTime() {
        return Long.parseLong(getValue(WorkflowExecutionArgs.WF_START_TIME));
    }

    public long getWorkflowEndTime() {
        return Long.parseLong(getValue(WorkflowExecutionArgs.WF_END_TIME));
    }


    public Type getContextType() {
        return Type.valueOf(getValue(WorkflowExecutionArgs.CONTEXT_TYPE));
    }

    public String getCounters() {
        return getValue(WorkflowExecutionArgs.COUNTERS);
    }

    /**
     * this method is invoked from with in the workflow.
     *
     * @throws java.io.IOException
     * @throws org.apache.falcon.FalconException
     */
    public void serialize() throws IOException, FalconException {
        serialize(getContextFile());
    }

    /**
     * this method is invoked from with in the workflow.
     *
     * @param contextFile file to serialize the workflow execution metadata
     * @throws org.apache.falcon.FalconException
     */
    public void serialize(String contextFile) throws FalconException {
        LOG.info("Saving context to: [{}]", contextFile);
        OutputStream out = null;
        Path file = new Path(contextFile);
        try {
            FileSystem fs =
                    actionJobConf == null ? HadoopClientFactory.get().createProxiedFileSystem(file.toUri())
                                 : HadoopClientFactory.get().createProxiedFileSystem(file.toUri(), actionJobConf);
            out = fs.create(file);
            out.write(JSONValue.toJSONString(context).getBytes());
        } catch (IOException e) {
            throw new FalconException("Error serializing context to: " + contextFile,  e);
        } finally {
            if (out != null) {
                try {
                    out.close();
                } catch (IOException ignore) {
                    // ignore
                }
            }
        }
    }

    @Override
    public String toString() {
        return "WorkflowExecutionContext{" + context.toString() + "}";
    }

    @SuppressWarnings("unchecked")
    public static WorkflowExecutionContext deSerialize(String contextFile) throws FalconException {
        try {
            Path lineageDataPath = new Path(contextFile); // file has 777 permissions
            FileSystem fs = HadoopClientFactory.get().createProxiedFileSystem(
                    lineageDataPath.toUri());

            BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(lineageDataPath)));
            return new WorkflowExecutionContext((Map<WorkflowExecutionArgs, String>) JSONValue.parse(in));
        } catch (IOException e) {
            throw new FalconException("Error opening context file: " + contextFile, e);
        }
    }

    public static String getFilePath(String logDir, String entityName, String entityType,
                                     EntityOperations operation) {
        // needed by feed clean up
        String parentSuffix = EntityType.PROCESS.name().equals(entityType)
                || EntityOperations.REPLICATE == operation ? "" : "/context/";

        // LOG_DIR is sufficiently unique
        return new Path(logDir + parentSuffix, entityName + "-wf-post-exec-context.json").toString();
    }


    public static Path getCounterFile(String logDir) {
        return new Path(logDir, "counter.txt");
    }

    public static String readCounters(FileSystem fs, Path counterFile) throws IOException{
        StringBuilder counterBuffer = new StringBuilder();
        BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(counterFile)));
        try {
            String line;
            while ((line = in.readLine()) != null) {
                counterBuffer.append(line);
                counterBuffer.append(",");
            }
        } catch (IOException e) {
            throw e;
        } finally {
            IOUtils.closeQuietly(in);
        }

        String counterString = counterBuffer.toString();
        if (StringUtils.isNotBlank(counterString) && counterString.length() > 0) {
            return counterString.substring(0, counterString.length() - 1);
        } else {
            return null;
        }
    }

    public static WorkflowExecutionContext create(String[] args, Type type) throws FalconException {
        return create(args, type, null);
    }

    public static WorkflowExecutionContext create(String[] args, Type type, Configuration conf) throws FalconException {
        Map<WorkflowExecutionArgs, String> wfProperties = new HashMap<WorkflowExecutionArgs, String>();

        try {
            CommandLine cmd = getCommand(args);
            for (WorkflowExecutionArgs arg : WorkflowExecutionArgs.values()) {
                String optionValue = arg.getOptionValue(cmd);
                if (StringUtils.isNotEmpty(optionValue)) {
                    wfProperties.put(arg, optionValue);
                }
            }
        } catch (ParseException e) {
            throw new FalconException("Error parsing wf args", e);
        }

        WorkflowExecutionContext executionContext = new WorkflowExecutionContext(wfProperties);
        executionContext.actionJobConf = conf;
        executionContext.context.put(WorkflowExecutionArgs.CONTEXT_TYPE, type.name());
        executionContext.context.put(WorkflowExecutionArgs.CONTEXT_FILE,
                getFilePath(executionContext.getLogDir(), executionContext.getEntityName(),
                        executionContext.getEntityType(), executionContext.getOperation()));
        addCounterToWF(executionContext);

        return executionContext;
    }

    private static void addCounterToWF(WorkflowExecutionContext executionContext) throws FalconException {
        if (executionContext.hasWorkflowFailed()) {
            LOG.info("Workflow Instance failed, counter will not be added: {}",
                    executionContext.getWorkflowRunIdString());
            return;
        }

        FileSystem fs = HadoopClientFactory.get().createProxiedFileSystem(
                new Path(executionContext.getLogDir()).toUri());
        Path counterFile = getCounterFile(executionContext.getLogDir());
        try {
            if (fs.exists(counterFile)) {
                String counters = readCounters(fs, counterFile);
                if (StringUtils.isNotBlank(counters)) {
                    executionContext.context.put(WorkflowExecutionArgs.COUNTERS, counters);
                }
            }
        } catch (IOException e) {
            LOG.error("Error in accessing counter file :" + e);
        } finally {
            try {
                if (fs.exists(counterFile)) {
                    fs.delete(counterFile, false);
                }
            } catch (IOException e) {
                LOG.error("Unable to delete counter file: {}", e);
            }
        }
    }

    private static CommandLine getCommand(String[] arguments) throws ParseException {
        Options options = new Options();

        for (WorkflowExecutionArgs arg : WorkflowExecutionArgs.values()) {
            addOption(options, arg, arg.isRequired());
        }

        return new GnuParser().parse(options, arguments, false);
    }

    private static void addOption(Options options, WorkflowExecutionArgs arg, boolean isRequired) {
        Option option = arg.getOption();
        option.setRequired(isRequired);
        options.addOption(option);
    }

    public static WorkflowExecutionContext create(Map<WorkflowExecutionArgs, String> wfProperties) {
        return WorkflowExecutionContext.create(wfProperties, Type.POST_PROCESSING);
    }

    public static WorkflowExecutionContext create(Map<WorkflowExecutionArgs, String> wfProperties, Type type) {
        wfProperties.put(WorkflowExecutionArgs.CONTEXT_TYPE, type.name());
        return new WorkflowExecutionContext(wfProperties);
    }
}
