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
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.ProcessHelper;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.entity.v0.process.Output;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.entity.v0.process.Property;
import org.apache.falcon.expression.ExpressionHelper;
import org.apache.falcon.messaging.EntityInstanceMessage.ARG;
import org.apache.falcon.oozie.coordinator.*;
import org.apache.falcon.oozie.workflow.ACTION;
import org.apache.falcon.oozie.workflow.SUBWORKFLOW;
import org.apache.falcon.oozie.workflow.WORKFLOWAPP;
import org.apache.hadoop.fs.Path;

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

    private void createWorkflow(Cluster cluster, String template, String wfName, Path wfPath) throws FalconException {
        WORKFLOWAPP wfApp = getWorkflowTemplate(template);
        wfApp.setName(wfName);

        for (Object object : wfApp.getDecisionOrForkOrJoin()) {
            if (object instanceof ACTION && ((ACTION) object).getName().equals("user-workflow")) {
                SUBWORKFLOW subWf = ((ACTION) object).getSubWorkflow();
                subWf.setAppPath(getStoragePath(getEntity().getWorkflow().getPath()));
            }
        }

        marshal(cluster, wfApp, wfPath);
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
        coord.setName(coordName);
        org.apache.falcon.entity.v0.process.Cluster processCluster = ProcessHelper.getCluster(process,
                cluster.getName());
        coord.setStart(SchemaHelper.formatDateUTC(processCluster.getValidity().getStart()));
        coord.setEnd(SchemaHelper.formatDateUTC(processCluster.getValidity().getEnd()));
        coord.setTimezone(process.getTimezone().getID());
        coord.setFrequency("${coord:" + process.getFrequency().toString() + "}");

        // controls
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
        coord.setControls(controls);

        // Configuration
        Map<String, String> props = createCoordDefaultConfiguration(cluster, coordPath, coordName);

        List<String> inputFeeds = new ArrayList<String>();
        List<String> inputPaths = new ArrayList<String>();
        // inputs
        if (process.getInputs() != null) {
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
        props.put("falconInPaths", join(inputPaths.iterator(), '#'));
        props.put("falconInputFeeds", join(inputFeeds.iterator(), '#'));

        // outputs
        List<String> outputFeeds = new ArrayList<String>();
        List<String> outputPaths = new ArrayList<String>();
        if (process.getOutputs() != null) {
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
        // Output feed name and path for parent workflow
        props.put(ARG.feedNames.getPropName(), join(outputFeeds.iterator(), ','));
        props.put(ARG.feedInstancePaths.getPropName(), join(outputPaths.iterator(), ','));

        // create parent wf
        createWorkflow(cluster, DEFAULT_WF_TEMPLATE, coordName, coordPath);

        WORKFLOW wf = new WORKFLOW();
        wf.setAppPath(getStoragePath(coordPath.toString()));
        wf.setConfiguration(getCoordConfig(props));

        // set coord action to parent wf
        org.apache.falcon.oozie.coordinator.ACTION action = new org.apache.falcon.oozie.coordinator.ACTION();
        action.setWorkflow(wf);
        coord.setAction(action);

        return coord;
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
        Feed feed = (Feed) EntityUtil.getEntity(EntityType.FEED, feedName);

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
}
