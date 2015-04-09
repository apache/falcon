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

package org.apache.falcon.catalog;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.CatalogStorage;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.Storage;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.CatalogTable;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.expression.ExpressionHelper;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.workflow.WorkflowExecutionContext;
import org.apache.falcon.workflow.WorkflowExecutionListener;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Collection;
import java.util.Arrays;
import java.util.TimeZone;
import java.util.Properties;

/**
 * Listens to workflow execution completion events.
 * It syncs HCat partitions based on the feeds created/evicted/replicated.
 */
public class CatalogPartitionHandler implements WorkflowExecutionListener{
    private static final Logger LOG = LoggerFactory.getLogger(CatalogPartitionHandler.class);

    public static final ConfigurationStore STORE = ConfigurationStore.get();
    public static final String CATALOG_TABLE = "catalog.table";
    private ExpressionHelper evaluator = ExpressionHelper.get();
    private static CatalogPartitionHandler catalogInstance = new CatalogPartitionHandler();
    private static final boolean IS_CATALOG_ENABLED = CatalogServiceFactory.isEnabled();
    public static final TimeZone UTC = TimeZone.getTimeZone("UTC");

    private static final PathFilter PATH_FILTER = new PathFilter() {
        @Override public boolean accept(Path path) {
            try {
                FileSystem fs = path.getFileSystem(new Configuration());
                return !path.getName().startsWith("_") && !path.getName().startsWith(".") && !fs.isFile(path);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    };

    public static final CatalogPartitionHandler get() {
        return catalogInstance;
    }

    @Override
    public void onSuccess(WorkflowExecutionContext context) throws FalconException {
        if (!IS_CATALOG_ENABLED) {
            //Skip if catalog service is not enabled
            return;
        }

        String[] feedNames = context.getOutputFeedNamesList();
        String[] feedPaths = context.getOutputFeedInstancePathsList();
        Cluster cluster = STORE.get(EntityType.CLUSTER, context.getClusterName());
        Configuration clusterConf = ClusterHelper.getConfiguration(cluster);

        if (StringUtils.isEmpty(ClusterHelper.getRegistryEndPoint(cluster))) {
            //Skip if registry endpoint is not defined for the cluster
            LOG.info("Catalog endpoint not defined for cluster {}. Skipping partition registration", cluster.getName());
            return;
        }

        for (int index = 0; index < feedNames.length; index++) {
            LOG.info("Partition handling for feed {} for path {}", feedNames[index], feedPaths[index]);
            Feed feed = STORE.get(EntityType.FEED, feedNames[index]);

            Storage storage = FeedHelper.createStorage(cluster, feed);
            if (storage.getType() == Storage.TYPE.TABLE) {
                //Do nothing if the feed is already table based
                LOG.info("Feed {} is already table based. Skipping partition registration", feed.getName());
                continue;
            }

            CatalogStorage catalogStorage = getCatalogStorageFromFeedProperties(feed, cluster, clusterConf);
            if (catalogStorage == null) {
                //There is no catalog defined in the feed properties. So, skip partition registration
                LOG.info("Feed {} doesn't have table defined in its properties/table doesn't exist. "
                        + "Skipping partition registration", feed.getName());
                continue;
            }

            //Generate static partition values - get the date from feed path and evaluate partitions in catalog spec
            Path feedPath = new Path(new Path(feedPaths[index]).toUri().getPath());

            String templatePath = new Path(storage.getUriTemplate(LocationType.DATA)).toUri().getPath();
            LOG.debug("Template {} catalogInstance path {}", templatePath, feedPath);
            Date date = FeedHelper.getDate(templatePath, feedPath, UTC);
            if (date == null) {
                LOG.info("Feed {} catalogInstance path {} doesn't match the template {}. "
                                + "Skipping partition registration",
                        feed.getName(), feedPath, templatePath);
                continue;
            }

            LOG.debug("Reference date from path {} is {}", feedPath, SchemaHelper.formatDateUTC(date));
            ExpressionHelper.setReferenceDate(date);
            List<String> partitionValues = new ArrayList<String>();
            for (Map.Entry<String, String> entry : catalogStorage.getPartitions().entrySet()) {
                LOG.debug("Evaluating partition {}", entry.getValue());
                partitionValues.add(evaluator.evaluateFullExpression(entry.getValue(), String.class));
            }

            LOG.debug("Static partition - {}", partitionValues);
            WorkflowExecutionContext.EntityOperations operation = context.getOperation();
            switch (operation) {
            case DELETE:
                dropPartitions(clusterConf, catalogStorage, partitionValues);
                break;

            case GENERATE:
            case REPLICATE:
                registerPartitions(clusterConf, catalogStorage, feedPath, partitionValues);
                break;

            default:
                throw new FalconException("Unhandled operation " + operation);
            }
        }
    }

    //Register additional partitions. Compare the expected partitions and the existing partitions
    //1.exist (intersection) expected --> partition already exists, so update partition
    //2.exist - expected --> partition is not required anymore, so drop partition
    //3.expected - exist --> partition doesn't exist, so add partition
    private void registerPartitions(Configuration conf, CatalogStorage storage, Path staticPath,
                                    List<String> staticPartition) throws FalconException {
        try {
            FileSystem fs = HadoopClientFactory.get().createProxiedFileSystem(conf);
            if (!fs.exists(staticPath)) {
                //Do nothing if the output path doesn't exist
                return;
            }

            List<String> partitionColumns = getPartitionColumns(conf, storage);
            int dynamicPartCols = partitionColumns.size() - staticPartition.size();
            Path searchPath = staticPath;
            if (dynamicPartCols > 0) {
                searchPath = new Path(staticPath, StringUtils.repeat("*", "/", dynamicPartCols));
            }

            //Figure out the dynamic partitions from the directories on hdfs
            FileStatus[] files = fs.globStatus(searchPath, PATH_FILTER);
            Map<List<String>, String> partitions = new HashMap<List<String>, String>();
            for (FileStatus file : files) {
                List<String> dynamicParts = getDynamicPartitions(file.getPath(), staticPath);
                List<String> partitionValues = new ArrayList<String>(staticPartition);
                partitionValues.addAll(dynamicParts);
                LOG.debug("Final partition - " + partitionValues);
                partitions.put(partitionValues, file.getPath().toString());
            }

            List<List<String>> existPartitions = listPartitions(conf, storage, staticPartition);
            Collection<List<String>> targetPartitions = partitions.keySet();

            Collection<List<String>> partitionsForDrop = CollectionUtils.subtract(existPartitions, targetPartitions);
            Collection<List<String>> partitionsForAdd = CollectionUtils.subtract(targetPartitions, existPartitions);
            Collection<List<String>> partitionsForUpdate =
                    CollectionUtils.intersection(existPartitions, targetPartitions);

            for (List<String> partition : partitionsForDrop) {
                dropPartitions(conf, storage, partition);
            }

            for (List<String> partition : partitionsForAdd) {
                addPartition(conf, storage, partition, partitions.get(partition));
            }

            for (List<String> partition : partitionsForUpdate) {
                updatePartition(conf, storage, partition, partitions.get(partition));
            }
        } catch(IOException e) {
            throw new FalconException(e);
        }
    }

    private void updatePartition(Configuration conf, CatalogStorage storage, List<String> partition, String location)
        throws FalconException {
        AbstractCatalogService catalogService = CatalogServiceFactory.getCatalogService();
        catalogService.updatePartition(conf, storage.getCatalogUrl(), storage.getDatabase(), storage.getTable(),
                partition, location);
    }

    private void addPartition(Configuration conf, CatalogStorage storage, List<String> partition, String location)
        throws FalconException {
        AbstractCatalogService catalogService = CatalogServiceFactory.getCatalogService();
        catalogService.addPartition(conf, storage.getCatalogUrl(), storage.getDatabase(), storage.getTable(), partition,
                location);
    }

    private List<List<String>> listPartitions(Configuration conf, CatalogStorage storage, List<String> staticPartitions)
        throws FalconException {
        AbstractCatalogService catalogService = CatalogServiceFactory.getCatalogService();
        List<CatalogPartition> partitions = catalogService.listPartitions(conf, storage.getCatalogUrl(),
                storage.getDatabase(), storage.getTable(), staticPartitions);
        List<List<String>> existPartitions = new ArrayList<List<String>>();
        for (CatalogPartition partition : partitions) {
            existPartitions.add(partition.getValues());
        }
        return existPartitions;
    }

    //Returns the dynamic partitions of the data path
    protected List<String> getDynamicPartitions(Path path, Path staticPath) {
        String dynPart = path.toUri().getPath().substring(staticPath.toString().length());
        dynPart = StringUtils.removeStart(dynPart, "/");
        dynPart = StringUtils.removeEnd(dynPart, "/");
        if (StringUtils.isEmpty(dynPart)) {
            return new ArrayList<String>();
        }
        return Arrays.asList(dynPart.split("/"));
    }

    private List<String> getPartitionColumns(Configuration conf, CatalogStorage storage) throws FalconException {
        AbstractCatalogService catalogService = CatalogServiceFactory.getCatalogService();
        return catalogService.getPartitionColumns(conf, storage.getCatalogUrl(), storage.getDatabase(),
                storage.getTable());
    }

    private void dropPartitions(Configuration conf, CatalogStorage storage, List<String> values)
        throws FalconException {
        AbstractCatalogService catalogService = CatalogServiceFactory.getCatalogService();
        catalogService.dropPartitions(conf, storage.getCatalogUrl(), storage.getDatabase(),
                storage.getTable(), values, false);
    }

    //Get the catalog template from feed properties as feed is filesystem based
    protected CatalogStorage getCatalogStorageFromFeedProperties(Feed feed, Cluster cluster, Configuration conf)
        throws FalconException {
        Properties properties = FeedHelper.getFeedProperties(feed);
        String tableUri = properties.getProperty(CATALOG_TABLE);
        if (tableUri == null) {
            return null;
        }

        CatalogTable table = new CatalogTable();
        table.setUri(tableUri.replace("{", "${"));
        CatalogStorage storage = null;
        try {
            storage = new CatalogStorage(cluster, table);
        } catch (URISyntaxException e) {
            throw new FalconException(e);
        }

        AbstractCatalogService catalogService = CatalogServiceFactory.getCatalogService();
        if (!catalogService.tableExists(conf, storage.getCatalogUrl(), storage.getDatabase(), storage.getTable())) {
            return null;
        }
        return storage;
    }

    @Override
    public void onFailure(WorkflowExecutionContext context) throws FalconException {
        //no-op
    }
}
