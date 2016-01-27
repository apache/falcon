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

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.LifeCycle;
import org.apache.falcon.Tag;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.Storage;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.oozie.coordinator.ACTION;
import org.apache.falcon.oozie.coordinator.COORDINATORAPP;
import org.apache.falcon.oozie.coordinator.DATAOUT;
import org.apache.falcon.oozie.coordinator.DATASETS;
import org.apache.falcon.oozie.coordinator.OUTPUTEVENTS;
import org.apache.falcon.oozie.coordinator.SYNCDATASET;
import org.apache.falcon.oozie.coordinator.WORKFLOW;
import org.apache.hadoop.fs.Path;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;

/**
 * Builds Oozie coordinator for database import.
 */

public class FeedImportCoordinatorBuilder extends OozieCoordinatorBuilder<Feed> {
    public FeedImportCoordinatorBuilder(Feed entity) {
        super(entity, LifeCycle.IMPORT);
    }

    public static final String IMPORT_DATASET_NAME = "import-dataset";

    public static final String IMPORT_DATAOUT_NAME = "import-output";

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(FeedImportCoordinatorBuilder.class);


    @Override
    public List<Properties> buildCoords(Cluster cluster, Path buildPath) throws FalconException {
        LOG.info("Generating Feed IMPORT coordinator.");

        org.apache.falcon.entity.v0.feed.Cluster feedCluster = FeedHelper.getCluster((Feed) entity, cluster.getName());
        if (!FeedHelper.isImportEnabled(feedCluster)) {
            return null;
        }

        if (feedCluster.getValidity().getEnd().before(new Date())) {
            LOG.warn("Feed IMPORT is not applicable as Feed's end time for cluster {} is not in the future",
                    cluster.getName());
            return null;
        }

        COORDINATORAPP coord = new COORDINATORAPP();
        initializeCoordAttributes(coord, (Feed) entity, cluster);
        Properties props = createCoordDefaultConfiguration(getEntityName());
        initializeOutputPath(coord, cluster, props);

        props.putAll(FeedHelper.getUserWorkflowProperties(getLifecycle()));

        WORKFLOW workflow = new WORKFLOW();
        Path coordPath = getBuildPath(buildPath);
        Properties wfProp = OozieOrchestrationWorkflowBuilder.get(entity, cluster, Tag.IMPORT).build(cluster,
                coordPath);
        workflow.setAppPath(getStoragePath(wfProp.getProperty(OozieEntityBuilder.ENTITY_PATH)));
        props.putAll(wfProp);
        workflow.setConfiguration(getConfig(props));
        ACTION action = new ACTION();
        action.setWorkflow(workflow);

        coord.setAction(action);

        Path marshalPath = marshal(cluster, coord, coordPath);
        return Arrays.asList(getProperties(marshalPath, getEntityName()));
    }

    private void initializeOutputPath(COORDINATORAPP coord, Cluster cluster, Properties props)
        throws FalconException {

        if (coord.getDatasets() == null) {
            coord.setDatasets(new DATASETS());
        }

        if (coord.getOutputEvents() == null) {
            coord.setOutputEvents(new OUTPUTEVENTS());
        }

        Storage storage = FeedHelper.createStorage(cluster, (Feed) entity);
        SYNCDATASET syncdataset = createDataSet((Feed) entity, cluster, storage,
                IMPORT_DATASET_NAME, LocationType.DATA);

        if (syncdataset == null) {
            return;
        }
        coord.getDatasets().getDatasetOrAsyncDataset().add(syncdataset);

        DATAOUT dataout = createDataOut(entity);
        coord.getOutputEvents().getDataOut().add(dataout);
    }

    private DATAOUT createDataOut(Feed feed) {
        DATAOUT dataout = new DATAOUT();
        dataout.setName(IMPORT_DATAOUT_NAME);
        dataout.setDataset(IMPORT_DATASET_NAME);
        dataout.setInstance("${coord:current(0)}");
        return dataout;
    }

    /**
     * Create DataSet. The start instance is set to current date if the merge type is snapshot.
     * Otherwise, the Feed cluster start data will be used as start instance.
     *
     * @param feed
     * @param cluster
     * @param storage
     * @param datasetName
     * @param locationType
     * @return
     * @throws FalconException
     */
    private SYNCDATASET createDataSet(Feed feed, Cluster cluster, Storage storage,
                                      String datasetName, LocationType locationType) throws FalconException {
        SYNCDATASET syncdataset = new SYNCDATASET();
        syncdataset.setName(datasetName);
        syncdataset.setFrequency("${coord:" + feed.getFrequency().toString() + "}");

        String uriTemplate = storage.getUriTemplate(locationType);
        if (StringUtils.isBlank(uriTemplate)) {
            return null;
        }
        if (storage.getType() == Storage.TYPE.TABLE) {
            uriTemplate = uriTemplate.replace("thrift", "hcat"); // Oozie requires this!!!
        }
        syncdataset.setUriTemplate(uriTemplate);

        org.apache.falcon.entity.v0.feed.Cluster feedCluster = FeedHelper.getCluster(feed, cluster.getName());
        Date initialInstance = FeedHelper.getImportInitalInstance(feedCluster);
        syncdataset.setInitialInstance(SchemaHelper.formatDateUTC(initialInstance));
        syncdataset.setTimezone(feed.getTimezone().getID());

        if (StringUtils.isNotBlank(feed.getAvailabilityFlag())) {
            syncdataset.setDoneFlag(feed.getAvailabilityFlag());
        } else {
            syncdataset.setDoneFlag("");
        }

        return syncdataset;
    }

    /**
     * Initialize the coordinator with current data as start if the merge type is snapshot.
     * Otherwise, use the feed cluster validate as the coordinator start date.
     *
     * @param coord
     * @param feed
     * @param cluster
     */

    private void initializeCoordAttributes(COORDINATORAPP coord, Feed feed, Cluster cluster) {
        coord.setName(getEntityName());
        // for feeds with snapshot layout, the start date will be the time of scheduling since it dumps whole table
        org.apache.falcon.entity.v0.feed.Cluster feedCluster = FeedHelper.getCluster(feed, cluster.getName());
        Date initialInstance = FeedHelper.getImportInitalInstance(feedCluster);
        coord.setStart(SchemaHelper.formatDateUTC(initialInstance));
        coord.setEnd(SchemaHelper.formatDateUTC(feedCluster.getValidity().getEnd()));
        coord.setTimezone(entity.getTimezone().getID());
        coord.setFrequency("${coord:" + entity.getFrequency().toString() + "}");
    }
}
