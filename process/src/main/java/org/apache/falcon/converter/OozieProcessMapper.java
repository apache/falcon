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

package org.apache.falcon.converter;

import org.apache.commons.lang.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.Tag;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.ProcessHelper;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.entity.v0.process.EngineType;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.entity.v0.process.Output;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.entity.v0.process.Property;
import org.apache.falcon.entity.v0.process.Workflow;
import org.apache.falcon.expression.ExpressionHelper;
import org.apache.falcon.messaging.EntityInstanceMessage.ARG;
import org.apache.falcon.oozie.coordinator.CONTROLS;
import org.apache.falcon.oozie.coordinator.COORDINATORAPP;
import org.apache.falcon.oozie.coordinator.DATAIN;
import org.apache.falcon.oozie.coordinator.DATAOUT;
import org.apache.falcon.oozie.coordinator.DATASETS;
import org.apache.falcon.oozie.coordinator.INPUTEVENTS;
import org.apache.falcon.oozie.coordinator.OUTPUTEVENTS;
import org.apache.falcon.oozie.coordinator.SYNCDATASET;
import org.apache.falcon.oozie.coordinator.WORKFLOW;
import org.apache.falcon.oozie.workflow.ACTION;
import org.apache.falcon.oozie.workflow.DELETE;
import org.apache.falcon.oozie.workflow.PIG;
import org.apache.falcon.oozie.workflow.PREPARE;
import org.apache.falcon.oozie.workflow.WORKFLOWAPP;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.IOException;
import java.util.*;

/**
 * This class maps the Falcon entities into Oozie artifacts.
 */
public class OozieProcessMapper extends AbstractOozieEntityMapper<Process> {

    private static final String DEFAULT_WF_TEMPLATE = "/config/workflow/process-parent-workflow.xml";
    private static final int THIRTY_MINUTES = 30 * 60 * 1000;

    public OozieProcessMapper(Process entity) {
        super(entity);
    }

    @Override
    protected List<COORDINATORAPP> getCoordinators(Cluster cluster, Path bundlePath) throws FalconException {
        List<COORDINATORAPP> apps = new ArrayList<COORDINATORAPP>();
        apps.add(createDefaultCoordinator(cluster, bundlePath));

        return apps;
    }

    /**
     * Creates default oozie coordinator.
     *
     * @param cluster    - Cluster for which the coordiantor app need to be created
     * @param bundlePath - bundle path
     * @return COORDINATORAPP
     * @throws FalconException on Error
     */
    public COORDINATORAPP createDefaultCoordinator(Cluster cluster, Path bundlePath) throws FalconException {
        Process process = getEntity();
        if (process == null) {
            return null;
        }

        COORDINATORAPP coord = new COORDINATORAPP();
        String coordName = EntityUtil.getWorkflowName(Tag.DEFAULT, process).toString();
        Path coordPath = getCoordPath(bundlePath, coordName);

        // coord attributes
        initializeCoordAttributes(cluster, process, coord, coordName);

        CONTROLS controls = initializeControls(process); // controls
        coord.setControls(controls);

        // Configuration
        Map<String, String> props = createCoordDefaultConfiguration(cluster, coordPath, coordName);

        List<String> inputFeeds = new ArrayList<String>();
        List<String> inputPaths = new ArrayList<String>();
        initializeInputPaths(cluster, process, coord, props, inputFeeds, inputPaths); // inputs
        props.put("falconInPaths", join(inputPaths.iterator(), '#'));
        props.put("falconInputFeeds", join(inputFeeds.iterator(), '#'));

        List<String> outputFeeds = new ArrayList<String>();
        List<String> outputPaths = new ArrayList<String>();
        initializeOutputPaths(cluster, process, coord, props, outputFeeds, outputPaths);  // outputs
        // Output feed name and path for parent workflow
        props.put(ARG.feedNames.getPropName(), join(outputFeeds.iterator(), ','));
        props.put(ARG.feedInstancePaths.getPropName(), join(outputPaths.iterator(), ','));

        Workflow processWorkflow = process.getWorkflow();
        props.put("userWorkflowEngine", processWorkflow.getEngine().value());

        // create parent wf
        createWorkflow(cluster, process, processWorkflow, coordName, coordPath);

        WORKFLOW wf = new WORKFLOW();
        wf.setAppPath(getStoragePath(coordPath.toString()));
        wf.setConfiguration(getCoordConfig(props));

        // set coord action to parent wf
        org.apache.falcon.oozie.coordinator.ACTION action = new org.apache.falcon.oozie.coordinator.ACTION();
        action.setWorkflow(wf);
        coord.setAction(action);

        return coord;
    }

    private void initializeCoordAttributes(Cluster cluster, Process process, COORDINATORAPP coord, String coordName) {
        coord.setName(coordName);
        org.apache.falcon.entity.v0.process.Cluster processCluster =
                ProcessHelper.getCluster(process, cluster.getName());
        coord.setStart(SchemaHelper.formatDateUTC(processCluster.getValidity().getStart()));
        coord.setEnd(SchemaHelper.formatDateUTC(processCluster.getValidity().getEnd()));
        coord.setTimezone(process.getTimezone().getID());
        coord.setFrequency("${coord:" + process.getFrequency().toString() + "}");
    }

    private CONTROLS initializeControls(Process process)
        throws FalconException {
        CONTROLS controls = new CONTROLS();
        controls.setConcurrency(String.valueOf(process.getParallel()));
        controls.setExecution(process.getOrder().name());

        Frequency timeout = process.getTimeout();
        long frequencyInMillis = ExpressionHelper.get().evaluate(process.getFrequency().toString(), Long.class);
        long timeoutInMillis;
        if (timeout != null) {
            timeoutInMillis = ExpressionHelper.get().
                    evaluate(process.getTimeout().toString(), Long.class);
        } else {
            timeoutInMillis = frequencyInMillis * 6;
            if (timeoutInMillis < THIRTY_MINUTES) {
                timeoutInMillis = THIRTY_MINUTES;
            }
        }
        controls.setTimeout(String.valueOf(timeoutInMillis / (1000 * 60)));

        if (timeoutInMillis / frequencyInMillis * 2 > 0) {
            controls.setThrottle(String.valueOf(timeoutInMillis / frequencyInMillis * 2));
        }

        return controls;
    }

    private void initializeInputPaths(Cluster cluster, Process process, COORDINATORAPP coord,
                                      Map<String, String> props, List<String> inputFeeds, List<String> inputPaths)
        throws FalconException {
        if (process.getInputs() == null) {
            return;
        }

        for (Input input : process.getInputs().getInputs()) {
            if (!input.isOptional()) {
                if (coord.getDatasets() == null) {
                    coord.setDatasets(new DATASETS());
                }
                if (coord.getInputEvents() == null) {
                    coord.setInputEvents(new INPUTEVENTS());
                }

                SYNCDATASET syncdataset = createDataSet(input.getFeed(), cluster, input.getName(),
                        LocationType.DATA);
                coord.getDatasets().getDatasetOrAsyncDataset().add(syncdataset);

                DATAIN datain = createDataIn(input);
                coord.getInputEvents().getDataIn().add(datain);
            }

            String inputExpr = getELExpression("dataIn('" + input.getName() + "', '" + input.getPartition() + "')");
            props.put(input.getName(), inputExpr);
            inputFeeds.add(input.getName());
            inputPaths.add(inputExpr);
        }
    }

    private void initializeOutputPaths(Cluster cluster, Process process, COORDINATORAPP coord,
                                       Map<String, String> props, List<String> outputFeeds, List<String> outputPaths)
        throws FalconException {
        if (process.getOutputs() == null) {
            return;
        }

        if (coord.getDatasets() == null) {
            coord.setDatasets(new DATASETS());
        }

        if (coord.getOutputEvents() == null) {
            coord.setOutputEvents(new OUTPUTEVENTS());
        }

        for (Output output : process.getOutputs().getOutputs()) {
            SYNCDATASET syncdataset = createDataSet(output.getFeed(), cluster, output.getName(), LocationType.DATA);
            coord.getDatasets().getDatasetOrAsyncDataset().add(syncdataset);

            DATAOUT dataout = createDataOut(output);
            coord.getOutputEvents().getDataOut().add(dataout);

            String outputExpr = "${coord:dataOut('" + output.getName() + "')}";
            props.put(output.getName(), outputExpr);
            outputFeeds.add(output.getName());
            outputPaths.add(outputExpr);

            // stats and meta paths
            createOutputEvent(output.getFeed(), output.getName(), cluster, "stats",
                    LocationType.STATS, coord, props, output.getInstance());
            createOutputEvent(output.getFeed(), output.getName(), cluster, "meta",
                    LocationType.META, coord, props, output.getInstance());
            createOutputEvent(output.getFeed(), output.getName(), cluster, "tmp",
                    LocationType.TMP, coord, props, output.getInstance());
        }
    }

    private DATAOUT createDataOut(Output output) {
        DATAOUT dataout = new DATAOUT();
        dataout.setName(output.getName());
        dataout.setDataset(output.getName());
        dataout.setInstance(getELExpression(output.getInstance()));
        return dataout;
    }

    private DATAIN createDataIn(Input input) {
        DATAIN datain = new DATAIN();
        datain.setName(input.getName());
        datain.setDataset(input.getName());
        datain.setStartInstance(getELExpression(input.getStart()));
        datain.setEndInstance(getELExpression(input.getEnd()));
        return datain;
    }

    //SUSPEND CHECKSTYLE CHECK VisibilityModifierCheck
    private void createOutputEvent(String feed, String name, Cluster cluster,
                                   String type, LocationType locType, COORDINATORAPP coord,
                                   Map<String, String> props, String instance) throws FalconException {
        SYNCDATASET dataset = createDataSet(feed, cluster, name + type,
                locType);
        coord.getDatasets().getDatasetOrAsyncDataset().add(dataset);
        DATAOUT dataout = new DATAOUT();
        if (coord.getOutputEvents() == null) {
            coord.setOutputEvents(new OUTPUTEVENTS());
        }
        dataout.setName(name + type);
        dataout.setDataset(name + type);
        dataout.setInstance(getELExpression(instance));
        coord.getOutputEvents().getDataOut().add(dataout);
        String outputExpr = "${coord:dataOut('" + name + type + "')}";
        props.put(name + "." + type, outputExpr);
    }
    //RESUME CHECKSTYLE CHECK VisibilityModifierCheck

    private String join(Iterator<String> itr, char sep) {
        String joinedStr = StringUtils.join(itr, sep);
        if (joinedStr.isEmpty()) {
            joinedStr = "null";
        }
        return joinedStr;
    }

    private SYNCDATASET createDataSet(String feedName, Cluster cluster, String datasetName,
                                      LocationType locationType) throws FalconException {
        Feed feed = EntityUtil.getEntity(EntityType.FEED, feedName);

        SYNCDATASET syncdataset = new SYNCDATASET();
        syncdataset.setName(datasetName);
        String locPath = FeedHelper.getLocation(feed, locationType,
                cluster.getName()).getPath();
        syncdataset.setUriTemplate(new Path(locPath).toUri().getScheme() != null ? locPath : "${nameNode}"
                + locPath);
        syncdataset.setFrequency("${coord:" + feed.getFrequency().toString() + "}");

        org.apache.falcon.entity.v0.feed.Cluster feedCluster = FeedHelper.getCluster(feed, cluster.getName());
        syncdataset.setInitialInstance(SchemaHelper.formatDateUTC(feedCluster.getValidity().getStart()));
        syncdataset.setTimezone(feed.getTimezone().getID());
        if (feed.getAvailabilityFlag() == null) {
            syncdataset.setDoneFlag("");
        } else {
            syncdataset.setDoneFlag(feed.getAvailabilityFlag());
        }
        return syncdataset;
    }

    private String getELExpression(String expr) {
        if (expr != null) {
            expr = "${" + expr + "}";
        }
        return expr;
    }

    @Override
    protected Map<String, String> getEntityProperties() {
        Process process = getEntity();
        Map<String, String> props = new HashMap<String, String>();
        if (process.getProperties() != null) {
            for (Property prop : process.getProperties().getProperties()) {
                props.put(prop.getName(), prop.getValue());
            }
        }
        return props;
    }

    private void createWorkflow(Cluster cluster, Process process, Workflow processWorkflow,
                                String wfName, Path wfPath) throws FalconException {
        WORKFLOWAPP wfApp = getWorkflowTemplate(DEFAULT_WF_TEMPLATE);
        wfApp.setName(wfName);

        EngineType engineType = processWorkflow.getEngine();
        for (Object object : wfApp.getDecisionOrForkOrJoin()) {
            if (!(object instanceof ACTION)) {
                continue;
            }

            String storagePath = getStoragePath(getEntity().getWorkflow().getPath());
            ACTION action = (ACTION) object;
            String actionName = action.getName();
            if (engineType == EngineType.OOZIE && actionName.equals("user-oozie-workflow")) {
                action.getSubWorkflow().setAppPath(storagePath);
            } else if (engineType == EngineType.PIG && actionName.equals("user-pig-job")) {
                decoratePIGAction(cluster, process, processWorkflow, storagePath, action.getPig());
            }
        }

        marshal(cluster, wfApp, wfPath);
    }

    private void decoratePIGAction(Cluster cluster, Process process, Workflow processWorkflow,
                                   String storagePath, PIG pigAction) throws FalconException {

        pigAction.setScript(storagePath);

        addPrepareDeleteOutputPath(process, pigAction);

        addInputOutputFeedsAsParams(pigAction, process);

        propagateProcessProperties(pigAction, process);

        addArchiveForCustomJars(cluster, processWorkflow, pigAction);
    }

    private void addPrepareDeleteOutputPath(Process process, PIG pigAction) {
        final PREPARE prepare = new PREPARE();
        final List<DELETE> deleteList = prepare.getDelete();
        for (Output output : process.getOutputs().getOutputs()) {
            final DELETE delete = new DELETE();
            delete.setPath("${wf:conf('" + output.getName() + "')}");
            deleteList.add(delete);
        }

        if (!deleteList.isEmpty()) {
            pigAction.setPrepare(prepare);
        }
    }

    private void addInputOutputFeedsAsParams(PIG pigAction, Process process) throws FalconException {
        final List<String> paramList = pigAction.getParam();
        for (Input input : process.getInputs().getInputs()) {
            paramList.add(input.getName() + "=${" + input.getName() + "}");
        }

        for (Output output : process.getOutputs().getOutputs()) {
            paramList.add(output.getName() + "=${" + output.getName() + "}");
        }
    }

    private void propagateProcessProperties(PIG pigAction, Process process) {
        org.apache.falcon.entity.v0.process.Properties processProperties = process.getProperties();
        if (processProperties == null) {
            return;
        }

        // Propagate user defined properties to job configuration
        final List<org.apache.falcon.oozie.workflow.CONFIGURATION.Property> configuration =
                pigAction.getConfiguration().getProperty();
        for (org.apache.falcon.entity.v0.process.Property property : processProperties.getProperties()) {
            org.apache.falcon.oozie.workflow.CONFIGURATION.Property configProperty =
                    new org.apache.falcon.oozie.workflow.CONFIGURATION.Property();
            configProperty.setName(property.getName());
            configProperty.setValue(property.getValue());
            configuration.add(configProperty);
        }

        // Propagate user defined properties to pig script as macros
        // passed as parameters -p name=value that can be accessed as $name
        final List<String> paramList = pigAction.getParam();
        for (org.apache.falcon.entity.v0.process.Property property : processProperties.getProperties()) {
            paramList.add(property.getName() + "=" + property.getValue());
        }
    }

    private void addArchiveForCustomJars(Cluster cluster, Workflow processWorkflow,
                                         PIG pigAction) throws FalconException {
        String processWorkflowLib = processWorkflow.getLib();
        if (processWorkflowLib == null) {
            return;
        }

        Path libPath = new Path(processWorkflowLib);
        try {
            final FileSystem fs = libPath.getFileSystem(ClusterHelper.getConfiguration(cluster));
            if (fs.isFile(libPath)) {  // File, not a Dir
                pigAction.getArchive().add(processWorkflowLib);
                return;
            }

            // lib path is a directory, add each file under the lib dir to archive
            final FileStatus[] fileStatuses = fs.listStatus(libPath, new PathFilter() {
                @Override
                public boolean accept(Path path) {
                    try {
                        return fs.isFile(path) && path.getName().endsWith(".jar");
                    } catch (IOException ignore) {
                        return false;
                    }
                }
            });

            for (FileStatus fileStatus : fileStatuses) {
                pigAction.getArchive().add(fileStatus.getPath().toString());
            }
        } catch (IOException e) {
            throw new FalconException("Error adding archive for custom jars under: " + libPath, e);
        }
    }
}
