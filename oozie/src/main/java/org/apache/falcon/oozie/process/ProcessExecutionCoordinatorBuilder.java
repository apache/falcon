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

package org.apache.falcon.oozie.process;

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.LifeCycle;
import org.apache.falcon.Tag;
import org.apache.falcon.entity.CatalogStorage;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.ProcessHelper;
import org.apache.falcon.entity.Storage;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.entity.v0.process.Output;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.expression.ExpressionHelper;
import org.apache.falcon.oozie.OozieCoordinatorBuilder;
import org.apache.falcon.oozie.OozieEntityBuilder;
import org.apache.falcon.oozie.OozieOrchestrationWorkflowBuilder;
import org.apache.falcon.oozie.coordinator.CONTROLS;
import org.apache.falcon.oozie.coordinator.COORDINATORAPP;
import org.apache.falcon.oozie.coordinator.DATAIN;
import org.apache.falcon.oozie.coordinator.DATAOUT;
import org.apache.falcon.oozie.coordinator.DATASETS;
import org.apache.falcon.oozie.coordinator.INPUTEVENTS;
import org.apache.falcon.oozie.coordinator.OUTPUTEVENTS;
import org.apache.falcon.oozie.coordinator.SYNCDATASET;
import org.apache.falcon.oozie.coordinator.WORKFLOW;
import org.apache.falcon.workflow.WorkflowExecutionArgs;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Builds oozie coordinator for process.
 */
public class ProcessExecutionCoordinatorBuilder extends OozieCoordinatorBuilder<Process> {
    private static final int THIRTY_MINUTES = 30 * 60 * 1000;

    public ProcessExecutionCoordinatorBuilder(Process entity) {
        super(entity, LifeCycle.EXECUTION);
    }

    @Override public List<Properties> buildCoords(Cluster cluster, Path buildPath) throws FalconException {
        String coordName = getEntityName();
        Path coordPath = getBuildPath(buildPath);
        copySharedLibs(cluster, new Path(coordPath, "lib"));

        COORDINATORAPP coord = new COORDINATORAPP();
        // coord attributes
        initializeCoordAttributes(cluster, coord, coordName);

        CONTROLS controls = initializeControls(); // controls
        coord.setControls(controls);

        // Configuration
        Properties props = createCoordDefaultConfiguration(coordName);

        initializeInputPaths(cluster, coord, props); // inputs
        initializeOutputPaths(cluster, coord, props);  // outputs

        // create parent wf
        Properties wfProps = OozieOrchestrationWorkflowBuilder.get(entity, cluster, Tag.DEFAULT).build(cluster,
            coordPath);

        WORKFLOW wf = new WORKFLOW();
        wf.setAppPath(getStoragePath(wfProps.getProperty(OozieEntityBuilder.ENTITY_PATH)));
        // Add the custom properties set in feed. Else, dryrun won't catch any missing props.
        props.putAll(EntityUtil.getEntityProperties(entity));
        wf.setConfiguration(getConfig(props));

        // set coord action to parent wf
        org.apache.falcon.oozie.coordinator.ACTION action = new org.apache.falcon.oozie.coordinator.ACTION();
        action.setWorkflow(wf);

        coord.setAction(action);

        Path marshalPath = marshal(cluster, coord, coordPath);
        return Arrays.asList(getProperties(marshalPath, coordName));
    }

    private void initializeCoordAttributes(Cluster cluster, COORDINATORAPP coord, String coordName) {
        coord.setName(coordName);
        org.apache.falcon.entity.v0.process.Cluster processCluster = ProcessHelper.getCluster(entity,
            cluster.getName());
        coord.setStart(SchemaHelper.formatDateUTC(processCluster.getValidity().getStart()));
        coord.setEnd(SchemaHelper.formatDateUTC(processCluster.getValidity().getEnd()));
        coord.setTimezone(entity.getTimezone().getID());
        coord.setFrequency("${coord:" + entity.getFrequency().toString() + "}");
    }

    private CONTROLS initializeControls()
        throws FalconException {
        CONTROLS controls = new CONTROLS();
        controls.setConcurrency(String.valueOf(entity.getParallel()));
        controls.setExecution(entity.getOrder().name());

        Frequency timeout = entity.getTimeout();
        long frequencyInMillis = ExpressionHelper.get().evaluate(entity.getFrequency().toString(), Long.class);
        long timeoutInMillis;
        if (timeout != null) {
            timeoutInMillis = ExpressionHelper.get().
                evaluate(entity.getTimeout().toString(), Long.class);
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

    private void initializeInputPaths(Cluster cluster, COORDINATORAPP coord, Properties props) throws FalconException {
        if (entity.getInputs() == null) {
            props.put(WorkflowExecutionArgs.INPUT_FEED_NAMES.getName(), NONE);
            props.put(WorkflowExecutionArgs.INPUT_FEED_PATHS.getName(), NONE);
            props.put(WorkflowExecutionArgs.INPUT_NAMES.getName(), NONE);
            return;
        }

        List<String> inputFeeds = new ArrayList<String>();
        List<String> inputNames = new ArrayList<String>();
        List<String> inputPaths = new ArrayList<String>();
        List<String> inputFeedStorageTypes = new ArrayList<String>();
        for (Input input : entity.getInputs().getInputs()) {
            Feed feed = EntityUtil.getEntity(EntityType.FEED, input.getFeed());
            Storage storage = FeedHelper.createStorage(cluster, feed);

            if (coord.getDatasets() == null) {
                coord.setDatasets(new DATASETS());
            }

            SYNCDATASET syncdataset = createDataSet(feed, cluster, storage, input.getName(), LocationType.DATA);
            if (syncdataset == null) {
                return;
            }
            coord.getDatasets().getDatasetOrAsyncDataset().add(syncdataset);

            if (!input.isOptional()) {
                if (coord.getInputEvents() == null) {
                    coord.setInputEvents(new INPUTEVENTS());
                }
                DATAIN datain = createDataIn(input);
                coord.getInputEvents().getDataIn().add(datain);
            }

            String inputExpr = null;
            if (storage.getType() == Storage.TYPE.FILESYSTEM) {
                inputExpr = getELExpression("dataIn('" + input.getName() + "', '" + input.getPartition() + "')");
                props.put(input.getName(), inputExpr);
            } else if (storage.getType() == Storage.TYPE.TABLE) {
                inputExpr = "${coord:dataIn('" + input.getName() + "')}";
                propagateCatalogTableProperties(input, (CatalogStorage) storage, props);
            }

            inputFeeds.add(feed.getName());
            inputPaths.add(inputExpr);
            inputNames.add(input.getName());
            inputFeedStorageTypes.add(storage.getType().name());
        }

        propagateLateDataProperties(inputFeeds, inputNames, inputPaths, inputFeedStorageTypes, props);
    }

    private void propagateLateDataProperties(List<String> inputFeeds, List<String> inputNames, List<String> inputPaths,
        List<String> inputFeedStorageTypes, Properties props) {
        // populate late data handler - should-record action
        props.put(WorkflowExecutionArgs.INPUT_FEED_NAMES.getName(), StringUtils.join(inputFeeds, '#'));
        props.put(WorkflowExecutionArgs.INPUT_NAMES.getName(), StringUtils.join(inputNames, '#'));
        props.put(WorkflowExecutionArgs.INPUT_FEED_PATHS.getName(), StringUtils.join(inputPaths, '#'));

        // storage type for each corresponding feed sent as a param to LateDataHandler
        // needed to compute usage based on storage type in LateDataHandler
        props.put(WorkflowExecutionArgs.INPUT_STORAGE_TYPES.getName(), StringUtils.join(inputFeedStorageTypes, '#'));
    }

    private SYNCDATASET createDataSet(Feed feed, Cluster cluster, Storage storage,
        String datasetName, LocationType locationType) throws FalconException {
        SYNCDATASET syncdataset = new SYNCDATASET();
        syncdataset.setName(datasetName);
        syncdataset.setFrequency("${coord:" + feed.getFrequency().toString() + "}");

        String uriTemplate = storage.getUriTemplate(locationType);
        if (uriTemplate == null) {
            return null;
        }
        if (storage.getType() == Storage.TYPE.TABLE) {
            uriTemplate = uriTemplate.replace("thrift", "hcat"); // Oozie requires this!!!
        }
        syncdataset.setUriTemplate(uriTemplate);

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

    private DATAIN createDataIn(Input input) {
        DATAIN datain = new DATAIN();
        datain.setName(input.getName());
        datain.setDataset(input.getName());
        datain.setStartInstance(getELExpression(input.getStart()));
        datain.setEndInstance(getELExpression(input.getEnd()));
        return datain;
    }

    private String getELExpression(String expr) {
        if (expr != null) {
            expr = "${" + expr + "}";
        }
        return expr;
    }

    private void initializeOutputPaths(Cluster cluster, COORDINATORAPP coord, Properties props) throws FalconException {
        if (entity.getOutputs() == null) {
            props.put(WorkflowExecutionArgs.OUTPUT_FEED_NAMES.getName(), NONE);
            props.put(WorkflowExecutionArgs.OUTPUT_FEED_PATHS.getName(), NONE);
            return;
        }

        if (coord.getDatasets() == null) {
            coord.setDatasets(new DATASETS());
        }

        if (coord.getOutputEvents() == null) {
            coord.setOutputEvents(new OUTPUTEVENTS());
        }

        List<String> outputFeeds = new ArrayList<String>();
        List<String> outputPaths = new ArrayList<String>();
        for (Output output : entity.getOutputs().getOutputs()) {
            Feed feed = EntityUtil.getEntity(EntityType.FEED, output.getFeed());
            Storage storage = FeedHelper.createStorage(cluster, feed);

            SYNCDATASET syncdataset = createDataSet(feed, cluster, storage, output.getName(), LocationType.DATA);
            if (syncdataset == null) {
                return;
            }
            coord.getDatasets().getDatasetOrAsyncDataset().add(syncdataset);

            DATAOUT dataout = createDataOut(output);
            coord.getOutputEvents().getDataOut().add(dataout);

            String outputExpr = "${coord:dataOut('" + output.getName() + "')}";
            outputFeeds.add(feed.getName());
            outputPaths.add(outputExpr);

            if (storage.getType() == Storage.TYPE.FILESYSTEM) {
                props.put(output.getName(), outputExpr);

                propagateFileSystemProperties(output, feed, cluster, coord, storage, props);
            } else if (storage.getType() == Storage.TYPE.TABLE) {
                propagateCatalogTableProperties(output, (CatalogStorage) storage, props);
            }
        }

        // Output feed name and path for parent workflow
        props.put(WorkflowExecutionArgs.OUTPUT_FEED_NAMES.getName(), StringUtils.join(outputFeeds, ','));
        props.put(WorkflowExecutionArgs.OUTPUT_FEED_PATHS.getName(), StringUtils.join(outputPaths, ','));
    }

    private DATAOUT createDataOut(Output output) {
        DATAOUT dataout = new DATAOUT();
        dataout.setName(output.getName());
        dataout.setDataset(output.getName());
        dataout.setInstance(getELExpression(output.getInstance()));
        return dataout;
    }

    private void propagateFileSystemProperties(Output output, Feed feed, Cluster cluster, COORDINATORAPP coord,
        Storage storage, Properties props) throws FalconException {
        // stats and meta paths
        createOutputEvent(output, feed, cluster, LocationType.STATS, coord, props, storage);
        createOutputEvent(output, feed, cluster, LocationType.META, coord, props, storage);
        createOutputEvent(output, feed, cluster, LocationType.TMP, coord, props, storage);
    }

    //SUSPEND CHECKSTYLE CHECK ParameterNumberCheck
    private void createOutputEvent(Output output, Feed feed, Cluster cluster, LocationType locType,
        COORDINATORAPP coord, Properties props, Storage storage) throws FalconException {
        String name = output.getName();
        String type = locType.name().toLowerCase();

        SYNCDATASET dataset = createDataSet(feed, cluster, storage, name + type, locType);
        if (dataset == null) {
            return;
        }
        coord.getDatasets().getDatasetOrAsyncDataset().add(dataset);

        DATAOUT dataout = new DATAOUT();
        dataout.setName(name + type);
        dataout.setDataset(name + type);
        dataout.setInstance(getELExpression(output.getInstance()));

        OUTPUTEVENTS outputEvents = coord.getOutputEvents();
        if (outputEvents == null) {
            outputEvents = new OUTPUTEVENTS();
            coord.setOutputEvents(outputEvents);
        }
        outputEvents.getDataOut().add(dataout);

        String outputExpr = "${coord:dataOut('" + name + type + "')}";
        props.put(name + "." + type, outputExpr);
    }
    //RESUME CHECKSTYLE CHECK ParameterNumberCheck



    protected void propagateCatalogTableProperties(Input input, CatalogStorage tableStorage, Properties props) {
        String prefix = "falcon_" + input.getName();

        propagateCommonCatalogTableProperties(tableStorage, props, prefix);

        props.put(prefix + "_partition_filter_pig",
            "${coord:dataInPartitionFilter('" + input.getName() + "', 'pig')}");
        props.put(prefix + "_partition_filter_hive",
            "${coord:dataInPartitionFilter('" + input.getName() + "', 'hive')}");
        props.put(prefix + "_partition_filter_java",
            "${coord:dataInPartitionFilter('" + input.getName() + "', 'java')}");
    }
}
