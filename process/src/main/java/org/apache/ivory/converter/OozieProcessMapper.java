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

package org.apache.ivory.converter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.ClusterHelper;
import org.apache.ivory.entity.EntityUtil;
import org.apache.ivory.entity.ExternalId;
import org.apache.ivory.entity.parser.Frequency;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.entity.v0.feed.Feed;
import org.apache.ivory.entity.v0.feed.LocationType;
import org.apache.ivory.entity.v0.process.Input;
import org.apache.ivory.entity.v0.process.LateProcess;
import org.apache.ivory.entity.v0.process.Output;
import org.apache.ivory.entity.v0.process.Process;
import org.apache.ivory.latedata.LateDataUtils;
import org.apache.ivory.messaging.EntityInstanceMessage;
import org.apache.ivory.oozie.coordinator.CONTROLS;
import org.apache.ivory.oozie.coordinator.COORDINATORAPP;
import org.apache.ivory.oozie.coordinator.DATAIN;
import org.apache.ivory.oozie.coordinator.DATAOUT;
import org.apache.ivory.oozie.coordinator.DATASETS;
import org.apache.ivory.oozie.coordinator.INPUTEVENTS;
import org.apache.ivory.oozie.coordinator.OUTPUTEVENTS;
import org.apache.ivory.oozie.coordinator.SYNCDATASET;
import org.apache.ivory.oozie.coordinator.WORKFLOW;
import org.apache.ivory.oozie.workflow.ACTION;
import org.apache.ivory.oozie.workflow.SUBWORKFLOW;
import org.apache.ivory.oozie.workflow.WORKFLOWAPP;
import org.apache.ivory.util.OozieUtils;
import org.apache.log4j.Logger;
import org.apache.oozie.client.OozieClient;

public class OozieProcessMapper extends AbstractOozieEntityMapper<Process> {

    private static final String EL_PREFIX = "elext:";
    private static Logger LOG = Logger.getLogger(OozieProcessMapper.class);

    private static final String DEFAULT_WF_TEMPLATE = "/config/workflow/process-parent-workflow.xml";
    private static final String LATE_WF_TEMPLATE = "/config/workflow/process-late1-workflow.xml";

    public OozieProcessMapper(Process entity) {
        super(entity);
    }

    @Override
    protected List<COORDINATORAPP> getCoordinators(Cluster cluster, Path bundlePath) throws IvoryException {
        List<COORDINATORAPP> apps = new ArrayList<COORDINATORAPP>();
        apps.add(createDefaultCoordinator(cluster, bundlePath));
        if (getEntity().getInputs() != null) {
            COORDINATORAPP lateCoordinator = createLateCoordinator(cluster, bundlePath);
            if (lateCoordinator != null) {
                apps.add(lateCoordinator);
            }
        }
        return apps;
    }

    private void createWorkflow(Cluster cluster, String template, String wfName, Path wfPath) throws IvoryException {
        WORKFLOWAPP wfApp = getWorkflowTemplate(template);
        wfApp.setName(wfName);

        for (Object object : wfApp.getDecisionOrForkOrJoin()) {
            if (object instanceof ACTION && ((ACTION) object).getName().equals("user-workflow")) {
                SUBWORKFLOW subWf = ((ACTION) object).getSubWorkflow();
                subWf.setAppPath(getHDFSPath(getEntity().getWorkflow().getPath()));
            }
        }

        marshal(cluster, wfApp, wfPath);
    }

    /**
     * Creates default oozie coordinator
     * 
     * @param cluster
     *            - Cluster for which the coordiantor app need to be created
     * @param bundlePath
     *            - bundle path
     * @return COORDINATORAPP
     * @throws IvoryException
     *             on Error
     */
    public COORDINATORAPP createDefaultCoordinator(Cluster cluster, Path bundlePath) throws IvoryException {
        Process process = getEntity();
        if (process == null)
            return null;

        COORDINATORAPP coord = new COORDINATORAPP();
        String coordName = process.getWorkflowName("DEFAULT");
        Path coordPath = getCoordPath(bundlePath, coordName);

        // coord attributes
        coord.setName(coordName);
        coord.setStart(process.getValidity().getStart());
        coord.setEnd(process.getValidity().getEnd());
        coord.setTimezone(process.getValidity().getTimezone());
        coord.setFrequency("${coord:" + process.getFrequency() + "(" + process.getPeriodicity() + ")}");

        // controls
        CONTROLS controls = new CONTROLS();
        controls.setConcurrency(String.valueOf(process.getConcurrency()));
        controls.setExecution(process.getExecution());
        coord.setControls(controls);

        // Configuration
        Map<String, String> props = createCoordDefaultConfiguration(cluster, coordPath, coordName);

        // inputs
        if (process.getInputs() != null) {
            StringBuffer ivoryInPaths = new StringBuffer();
            for (Input input : process.getInputs().getInput()) {
                SYNCDATASET syncdataset = createDataSet(input.getFeed(), cluster);
                if (coord.getDatasets() == null)
                    coord.setDatasets(new DATASETS());
                coord.getDatasets().getDatasetOrAsyncDataset().add(syncdataset);

                DATAIN datain = new DATAIN();
                datain.setName(input.getName());
                datain.setDataset(input.getFeed());
                datain.setStartInstance(getELExpression(input.getStartInstance()));
                datain.setEndInstance(getELExpression(input.getEndInstance()));
                if (coord.getInputEvents() == null)
                    coord.setInputEvents(new INPUTEVENTS());
                coord.getInputEvents().getDataIn().add(datain);

                String inputExpr;
                if (StringUtils.isNotEmpty(input.getPartition())) {
                    inputExpr = getELExpression("dataIn('" + input.getName() + "', '" + input.getPartition() + "')");
                } else {
                    inputExpr = "${coord:dataIn('" + input.getName() + "')}";
                }
                props.put(input.getName(), inputExpr);
                ivoryInPaths.append(inputExpr).append('#');
                props.put("ivoryInPaths", ivoryInPaths.substring(0, ivoryInPaths.length() - 1));
            }
        }

        // outputs
        if (process.getOutputs() != null) {
            StringBuilder outputFeedPaths = new StringBuilder();
            StringBuilder outputFeedNames = new StringBuilder();
            for (Output output : process.getOutputs().getOutput()) {
                SYNCDATASET syncdataset = createDataSet(output.getFeed(), cluster);
                if (coord.getDatasets() == null)
                    coord.setDatasets(new DATASETS());
                coord.getDatasets().getDatasetOrAsyncDataset().add(syncdataset);

                DATAOUT dataout = new DATAOUT();
                dataout.setName(output.getName());
                dataout.setDataset(output.getFeed());
                dataout.setInstance(getELExpression(output.getInstance()));
                if (coord.getOutputEvents() == null)
                    coord.setOutputEvents(new OUTPUTEVENTS());
                coord.getOutputEvents().getDataOut().add(dataout);

                outputFeedNames.append(output.getName()).append(",");
                outputFeedPaths.append("${coord:dataOut('" + output.getName() + "')}").append(",");

                props.put(output.getName(), "${coord:dataOut('" + output.getName() + "')}");
            }
            // Output feed name and path for parent workflow
            props.put(EntityInstanceMessage.ARG.FEED_NAME.NAME(), outputFeedNames.substring(0, outputFeedNames.length() - 1));
            props.put(EntityInstanceMessage.ARG.FEED_INSTANCE_PATH.NAME(), outputFeedPaths.substring(0, outputFeedPaths.length() - 1));
        }

        String libDir = getLibDirectory(process.getWorkflow().getPath(), cluster);
        if (libDir != null)
            props.put(OozieClient.LIBPATH, libDir);

        // create parent wf
        createWorkflow(cluster, DEFAULT_WF_TEMPLATE, coordName, coordPath);

        WORKFLOW wf = new WORKFLOW();
        wf.setAppPath(getHDFSPath(coordPath.toString()));
        wf.setConfiguration(getCoordConfig(props));

        // set coord action to parent wf
        org.apache.ivory.oozie.coordinator.ACTION action = new org.apache.ivory.oozie.coordinator.ACTION();
        action.setWorkflow(wf);
        coord.setAction(action);

        return coord;
    }

    public COORDINATORAPP createLateCoordinator(Cluster cluster, Path bundlePath) throws IvoryException {
        Process process = getEntity();
        if (process == null)
            return null;

        LateProcess lateProcess = process.getLateProcess();
        if (lateProcess == null) {
            LOG.warn("Late data coordinator doesn't apply, as the late-process tag is not present in process: " + process.getName());
            return null;
        }

        String offsetExpr = "";
        long longestOffset = -1;
        for (Input input : process.getInputs().getInput()) {
            Feed feed = ConfigurationStore.get().get(EntityType.FEED, input.getFeed());
            String offsetLocal = feed.getLateArrival().getCutOff();
            long durationLocal = LateDataUtils.getDurationFromOffset(offsetLocal);
            if (durationLocal > longestOffset) {
                longestOffset = durationLocal;
                offsetExpr = offsetLocal;
            }
        }
        assert !offsetExpr.isEmpty() : "dont expect offset to be empty";

        COORDINATORAPP coord = new COORDINATORAPP();
        String coordName = process.getWorkflowName("LATE1");
        Path coordPath = getCoordPath(bundlePath, coordName);

        // coord attributes
        coord.setName(coordName);
        long endTime = EntityUtil.parseDateUTC(process.getValidity().getEnd()).getTime();
        long now = System.currentTimeMillis();
        if (endTime < now) {
            LOG.warn("Late data coordinator doesn't apply, as the end date is in past " + process.getValidity().getEnd());
            return null;
        }

        String processStartTime = process.getValidity().getStart();
        Date startTime = OozieUtils.getNextStartTime(EntityUtil.parseDateUTC(processStartTime),
                Frequency.valueOf(process.getFrequency()), process.getPeriodicity(),
                process.getValidity().getTimezone(), new Date(now));
        LOG.info("Using start time as (aligned to default) : " + EntityUtil.formatDateUTC(startTime));

        coord.setStart(LateDataUtils.addOffset(EntityUtil.formatDateUTC(startTime), offsetExpr));
        coord.setEnd(LateDataUtils.addOffset(process.getValidity().getEnd(), offsetExpr));
        coord.setTimezone(process.getValidity().getTimezone());
        coord.setFrequency("${coord:" + process.getFrequency() + "(" + process.getPeriodicity() + ")}");

        // controls
        CONTROLS controls = new CONTROLS();
        controls.setConcurrency(String.valueOf(process.getConcurrency()));
        controls.setExecution(process.getExecution());
        coord.setControls(controls);

        // Configuration
        Map<String, String> props = createCoordDefaultConfiguration(cluster, coordPath, coordName);
        Long millis = LateDataUtils.getDurationFromOffset(offsetExpr);
        long offset = -millis / (60000);
        String nominalTime = LATE_NOMINAL_TIME_EL.replace("#VAL#", String.valueOf(offset));
        props.put(EntityInstanceMessage.ARG.NOMINAL_TIME.NAME(), nominalTime);        
        props.put(OozieClient.EXTERNAL_ID, new ExternalId(process.getName(), process.getWorkflowNameTag(coordName), nominalTime).getId());

        // inputs
        if (process.getInputs() != null) {
            StringBuffer ivoryInPaths = new StringBuffer();
            for (Input input : process.getInputs().getInput()) {
                SYNCDATASET syncdataset = createDataSet(input.getFeed(), cluster);
                if (coord.getDatasets() == null)
                    coord.setDatasets(new DATASETS());
                coord.getDatasets().getDatasetOrAsyncDataset().add(syncdataset);

                DATAIN datain = new DATAIN();
                datain.setName(input.getName());
                datain.setDataset(input.getFeed());
                datain.setStartInstance(LateDataUtils.offsetTime(getELExpression(input.getStartInstance()), offsetExpr));
                datain.setEndInstance(LateDataUtils.offsetTime(getELExpression(input.getEndInstance()), offsetExpr));
                if (coord.getInputEvents() == null)
                    coord.setInputEvents(new INPUTEVENTS());
                coord.getInputEvents().getDataIn().add(datain);

                String inputExpr;
                if (StringUtils.isNotEmpty(input.getPartition())) {
                    inputExpr = getELExpression("dataIn('" + input.getName() + "', '" + input.getPartition() + "')");
                } else {
                    inputExpr = "${coord:dataIn('" + input.getName() + "')}";
                }
                props.put(input.getName(), inputExpr);
                ivoryInPaths.append(inputExpr).append('#');
                props.put("ivoryInPaths", ivoryInPaths.substring(0, ivoryInPaths.length() - 1));
            }
        }

        // outputs
        if (process.getOutputs() != null) {
            StringBuilder outputFeedPaths = new StringBuilder();
            StringBuilder outputFeedNames = new StringBuilder();
            for (Output output : process.getOutputs().getOutput()) {
                SYNCDATASET syncdataset = createDataSet(output.getFeed(), cluster);
                if (coord.getDatasets() == null)
                    coord.setDatasets(new DATASETS());
                coord.getDatasets().getDatasetOrAsyncDataset().add(syncdataset);

                DATAOUT dataout = new DATAOUT();
                dataout.setName(output.getName());
                dataout.setDataset(output.getFeed());
                dataout.setInstance(LateDataUtils.offsetTime(getELExpression(output.getInstance()), offsetExpr));
                if (coord.getOutputEvents() == null)
                    coord.setOutputEvents(new OUTPUTEVENTS());
                coord.getOutputEvents().getDataOut().add(dataout);

                outputFeedNames.append(output.getName()).append(",");
                outputFeedPaths.append("${coord:dataOut('").append(output.getName()).append("')}").append(",");

                props.put(output.getName(), "${coord:dataOut('" + output.getName() + "')}");
            }
            // Output feed name and path for parent workflow
            props.put(EntityInstanceMessage.ARG.FEED_NAME.NAME(), outputFeedNames.substring(0, outputFeedNames.length() - 1));
            props.put(EntityInstanceMessage.ARG.FEED_INSTANCE_PATH.NAME(), outputFeedPaths.substring(0, outputFeedPaths.length() - 1));
        }

        String libDir = getLibDirectory(process.getWorkflow().getPath(), cluster);
        if (libDir != null)
            props.put(OozieClient.LIBPATH, libDir);

        // create parent wf
        createWorkflow(cluster, LATE_WF_TEMPLATE, coordName, coordPath);

        WORKFLOW wf = new WORKFLOW();
        wf.setAppPath(getHDFSPath(coordPath.toString()));
        wf.setConfiguration(getCoordConfig(props));

        // set coord action to parent wf
        org.apache.ivory.oozie.coordinator.ACTION action = new org.apache.ivory.oozie.coordinator.ACTION();
        action.setWorkflow(wf);
        coord.setAction(action);

        return coord;
    }

    private String getLibDirectory(String wfpath, Cluster cluster) throws IvoryException {
        Path path = new Path(wfpath.replace("${nameNode}", ""));
        String libDir;
        try {
            FileSystem fs = FileSystem.get(ClusterHelper.getConfiguration(cluster));
            FileStatus status = fs.getFileStatus(path);
            if (status.isDir())
                libDir = path.toString() + "/lib";
            else
                libDir = path.getParent().toString() + "/lib";

            if (fs.exists(new Path(libDir)))
                return "${nameNode}" + libDir;
        } catch (IOException e) {
            throw new IvoryException(e);
        }
        return null;
    }

    private SYNCDATASET createDataSet(String feedName, Cluster cluster) throws IvoryException {
        Feed feed;
        try {
            feed = ConfigurationStore.get().get(EntityType.FEED, feedName);
        } catch (Exception e) {
            throw new IvoryException(e);
        }
        if (feed == null) // This should never happen as its checked in process
                          // validation
            throw new RuntimeException("Referenced feed " + feedName + " is not registered!");

        SYNCDATASET syncdataset = new SYNCDATASET();
        syncdataset.setName(feed.getName());
        syncdataset.setUriTemplate("${nameNode}" + feed.getLocations().get(LocationType.DATA).getPath());
        syncdataset.setFrequency("${coord:" + feed.getFrequency() + "(" + feed.getPeriodicity() + ")}");

        org.apache.ivory.entity.v0.feed.Cluster feedCluster = feed.getCluster(cluster.getName());
        syncdataset.setInitialInstance(feedCluster.getValidity().getStart());
        syncdataset.setTimezone(feedCluster.getValidity().getTimezone());
        syncdataset.setDoneFlag("");
        return syncdataset;
    }

    private String getELExpression(String expr) {
        if (expr != null) {
            expr = "${" + EL_PREFIX + expr + "}";
        }
        return expr;
    }

    @Override
    protected Map<String, String> getEntityProperties() {
        Process process = getEntity();
        Map<String, String> props = new HashMap<String, String>();
        if (process.getProperties() != null) {
            for (org.apache.ivory.entity.v0.process.Property prop : process.getProperties().getProperty())
                props.put(prop.getName(), prop.getValue());
        }
        return props;
    }
}