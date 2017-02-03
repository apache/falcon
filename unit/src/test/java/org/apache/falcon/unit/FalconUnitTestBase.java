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
package org.apache.falcon.unit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.LifeCycle;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.Storage;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.expression.ExpressionHelper;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.hadoop.JailedFileSystem;
import org.apache.falcon.resource.APIResult;
import org.apache.falcon.resource.ExtensionJobList;
import org.apache.falcon.resource.InstancesResult;
import org.apache.falcon.util.DateUtil;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.InputStream;
import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Test Utility for Local Falcon Unit.
 */
public class FalconUnitTestBase {

    /**
     * Perform a predicate evaluation.
     *
     * @return the boolean result of the evaluation.
     * @throws Exception thrown if the predicate evaluation could not evaluate.
     */
    public interface Predicate {
        boolean evaluate() throws Exception;
    }

    public static final ThreadLocal<SimpleDateFormat> FORMATTER = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
            return format;
        }
    };

    private static final Logger LOG = LoggerFactory.getLogger(FalconUnitTestBase.class);

    private static final String DEFAULT_CLUSTER = "local";
    private static final String DEFAULT_COLO = "local";
    protected static final String CLUSTER = "cluster";
    protected static final String COLO = "colo";
    protected static final String CLUSTER_TEMPLATE = "/local-cluster-template.xml";
    protected static final String STAGING_PATH = "/projects/falcon/staging";
    protected static final String WORKING_PATH = "/projects/falcon/working";

    public static final Pattern VAR_PATTERN = Pattern.compile("##[A-Za-z0-9_.]*##");
    protected static final int WAIT_TIME = 90000;
    protected static FalconUnitClient falconUnitClient;
    protected static JailedFileSystem fs;
    protected static ConfigurationStore configStore;

    @BeforeClass
    public void setup() throws Exception {
        FalconUnit.start(true);
        falconUnitClient = FalconUnit.getClient();
        fs = (JailedFileSystem) FalconUnit.getFileSystem();
        configStore = falconUnitClient.getConfigStore();
    }

    @AfterClass
    public void cleanup() throws Exception {
        fs.delete(new Path(STAGING_PATH), true);
        fs.delete(new Path(WORKING_PATH), true);
        FalconUnit.cleanup();
    }

    @AfterMethod
    public void cleanUpActionXml() throws IOException, FalconException {
        for (EntityType type : ConfigurationStore.ENTITY_DELETE_ORDER) {
            for (String name : ConfigurationStore.get().getEntities(type)) {
                getClient().delete(type, name, null);
            }
        }
        //Needed since oozie writes action xml to current directory.
        FileUtils.deleteQuietly(new File("action.xml"));
        FileUtils.deleteQuietly(new File(".action.xml.crc"));
    }

    protected FalconUnitClient getClient() throws FalconException {
        return FalconUnit.getClient();
    }

    protected JailedFileSystem getFileSystem() throws IOException {
        return fs;
    }

    public boolean submitCluster(String colo, String cluster,
                                 Map<String, String> props) throws IOException {
        props = updateColoAndCluster(colo, cluster, props);
        fs.mkdirs(new Path(STAGING_PATH), HadoopClientFactory.ALL_PERMISSION);
        fs.mkdirs(new Path(WORKING_PATH), HadoopClientFactory.READ_EXECUTE_PERMISSION);
        String clusterXmlPath = overlayParametersOverTemplate(CLUSTER_TEMPLATE, props);
        APIResult result = falconUnitClient.submit(CLUSTER, clusterXmlPath, "");
        return true ? APIResult.Status.SUCCEEDED.equals(result.getStatus()) : false;
    }

    public boolean submitCluster() throws IOException {
        return submitCluster(DEFAULT_COLO, DEFAULT_CLUSTER, null);
    }

    public APIResult submit(EntityType entityType, String filePath) throws IOException {
        return submit(entityType.toString(), filePath);
    }

    public APIResult submit(String entityType, String filePath) throws IOException {
        return falconUnitClient.submit(entityType, filePath, "");
    }

    public APIResult submitProcess(String filePath, String appDirectory) throws IOException {
        createDir(appDirectory);
        return submit(EntityType.PROCESS, filePath);
    }

    public APIResult scheduleProcess(String processName, String startTime, int numInstances,
                                   String cluster, String localWfPath, Boolean skipDryRun,
                                   String properties) throws FalconException, IOException {
        Process processEntity = configStore.get(EntityType.PROCESS, processName);
        if (processEntity == null) {
            throw new FalconException("Process not found " + processName);
        }
        String workflowPath = processEntity.getWorkflow().getPath();
        fs.copyFromLocalFile(new Path(localWfPath), new Path(workflowPath, "workflow.xml"));
        return falconUnitClient.schedule(EntityType.PROCESS, processName, startTime, numInstances, cluster,
                skipDryRun, properties);
    }

    public APIResult scheduleProcess(String processName, String cluster, String localWfPath) throws FalconException,
            IOException {
        Process processEntity = configStore.get(EntityType.PROCESS, processName);
        if (processEntity == null) {
            throw new FalconException("Process not found " + processName);
        }
        String workflowPath = processEntity.getWorkflow().getPath();
        fs.copyFromLocalFile(new Path(localWfPath), new Path(workflowPath, "workflow.xml"));
        return falconUnitClient.schedule(EntityType.PROCESS, processName, cluster, false, null, null);
    }

    public APIResult schedule(EntityType entityType, String entityName, String cluster) throws FalconException {
        Entity entity = configStore.get(entityType, entityName);
        if (entity == null) {
            throw new FalconException("Process not found " + entityName);
        }
        return falconUnitClient.schedule(entityType, entityName, cluster, false, null, null);
    }

    public APIResult submitAndSchedule(String type, String filePath, String localWfPath, Boolean skipDryRun,
                                       String doAsUser, String properties, String appDirectory) throws IOException,
            FalconException {
        createDir(appDirectory);
        fs.copyFromLocalFile(new Path(localWfPath), new Path(appDirectory, "workflow.xml"));
        return falconUnitClient.submitAndSchedule(type, filePath, skipDryRun, doAsUser, properties);
    }

    private Map<String, String> updateColoAndCluster(String colo, String cluster, Map<String, String> props) {
        if (props == null) {
            props = new HashMap<>();
        }
        String coloProp = StringUtils.isEmpty(colo) ? DEFAULT_COLO : colo;
        props.put(COLO, coloProp);

        String clusterProp = StringUtils.isEmpty(cluster) ? DEFAULT_CLUSTER : cluster;
        props.put(CLUSTER, clusterProp);

        return props;
    }

    APIResult registerExtension(String extensionName, String packagePath, String description)
        throws IOException, FalconException {

        return falconUnitClient.registerExtension(extensionName, packagePath, description);
    }

    String disableExtension(String extensionName) {
        return falconUnitClient.disableExtension(extensionName).getMessage();
    }

    String enableExtension(String extensionName) {
        return falconUnitClient.enableExtension(extensionName).getMessage();
    }

    APIResult getExtensionJobDetails(String jobName) {
        return falconUnitClient.getExtensionJobDetails(jobName);
    }

    APIResult unregisterExtension(String extensionName) {
        return falconUnitClient.unregisterExtension(extensionName);
    }

    APIResult submitExtensionJob(String extensionName, String jobName, String configPath, String doAsUser) {
        return falconUnitClient.submitExtensionJob(extensionName, jobName, configPath, doAsUser);
    }

    ExtensionJobList getExtensionJobs(String extensionName, String sortOrder, String doAsUser) {
        return falconUnitClient.getExtensionJobs(extensionName, sortOrder, doAsUser);
    }

    public APIResult submitAndScheduleExtensionJob(String extensionName, String jobName, String configPath,
                                                   String doAsUser) {
        return falconUnitClient.submitAndScheduleExtensionJob(extensionName, jobName, configPath, doAsUser);
    }

    APIResult updateExtensionJob(String jobName, String configPath, String doAsUser) {
        return falconUnitClient.updateExtensionJob(jobName, configPath, doAsUser);
    }

    APIResult deleteExtensionJob(String jobName, String doAsUser) {
        return falconUnitClient.deleteExtensionJob(jobName, doAsUser);
    }

    public static String overlayParametersOverTemplate(String template,
                                                       Map<String, String> overlay) throws IOException {
        File tmpFile = getTempFile();
        OutputStream out = new FileOutputStream(tmpFile);

        InputStreamReader in;
        InputStream resourceAsStream = FalconUnitTestBase.class.getResourceAsStream(template);
        if (resourceAsStream == null) {
            in = new FileReader(template);
        } else {
            in = new InputStreamReader(resourceAsStream);
        }
        BufferedReader reader = new BufferedReader(in);
        String line;
        while ((line = reader.readLine()) != null) {
            Matcher matcher = VAR_PATTERN.matcher(line);
            while (matcher.find()) {
                String variable = line.substring(matcher.start(), matcher.end());
                line = line.replace(variable, overlay.get(variable.substring(2, variable.length() - 2)));
                matcher = VAR_PATTERN.matcher(line);
            }
            out.write(line.getBytes());
            out.write("\n".getBytes());
        }
        reader.close();
        out.close();
        return tmpFile.getAbsolutePath();
    }


    public static File getTempFile() throws IOException {
        return getTempFile("test", ".xml");
    }

    public static File getTempFile(String prefix, String suffix) throws IOException {
        return getTempFile("target", prefix, suffix);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static File getTempFile(String path, String prefix, String suffix) throws IOException {
        File f = new File(path);
        if (!f.exists()) {
            f.mkdirs();
        }
        return File.createTempFile(prefix, suffix, f);
    }

    /**
     * Creates data in the feed path with the given timestamp.
     *
     * @param feedName
     * @param cluster
     * @param time
     * @param inputFile
     * @throws FalconException
     * @throws ParseException
     * @throws IOException
     */
    public void createData(String feedName, String cluster, String time,
                           String inputFile) throws FalconException, ParseException, IOException {
        String feedPath = getFeedPathForTS(cluster, feedName, time);
        fs.mkdirs(new Path(feedPath));
        fs.copyFromLocalFile(new Path(getAbsolutePath(inputFile)), new Path(feedPath));
    }

    public void deleteData(String feedName, String cluster) throws FalconException, ParseException, IOException {
        Entity existingEntity = configStore.get(EntityType.FEED, feedName);
        if (existingEntity == null) {
            throw new FalconException("Feed Not Found  " + feedName);
        }
        Feed feed = (Feed) existingEntity;
        Storage rawStorage = FeedHelper.createStorage(cluster, feed);
        String feedPathTemplate = rawStorage.getUriTemplate(LocationType.DATA);

        Path feedBasePath = FeedHelper.getFeedBasePath(feedPathTemplate);
        fs.delete(feedBasePath, true);
    }

    protected String getFeedPathForTS(String cluster, String feedName,
                                      String timeStamp) throws FalconException, ParseException {
        Entity existingEntity = configStore.get(EntityType.FEED, feedName);
        if (existingEntity == null) {
            throw new FalconException("Feed Not Found  " + feedName);
        }
        Feed feed = (Feed) existingEntity;
        Storage rawStorage = FeedHelper.createStorage(cluster, feed);
        String feedPathTemplate = rawStorage.getUriTemplate(LocationType.DATA);
        Properties properties = ExpressionHelper.getTimeVariables(ExpressionHelper.FORMATTER.get().parse(timeStamp),
                TimeZone.getTimeZone("UTC"));
        String feedPath = ExpressionHelper.substitute(feedPathTemplate, properties);
        return feedPath;
    }


    public String getAbsolutePath(String fileName) {
        return this.getClass().getResource("/" + fileName).getPath();
    }

    public void createDir(String path) throws IOException {
        fs.mkdirs(new Path(path));
    }

    /**
     * Wait for a condition, expressed via a {@link Predicate} to become true.
     *
     * @param timeout   maximum time in milliseconds to wait for the predicate to become true.
     * @param predicate predicate waiting on.
     * @return the waited time.
     */
    protected long waitFor(int timeout, Predicate predicate) {
        long started = System.currentTimeMillis();
        long mustEnd = System.currentTimeMillis() + timeout;
        long lastEcho = 0;
        try {
            long waiting = mustEnd - System.currentTimeMillis();
            LOG.info("Waiting up to [{}] msec", waiting);
            while (!(predicate.evaluate()) && System.currentTimeMillis() < mustEnd) {
                if ((System.currentTimeMillis() - lastEcho) > 5000) {
                    waiting = mustEnd - System.currentTimeMillis();
                    LOG.info("Waiting up to [{}] msec", waiting);
                    lastEcho = System.currentTimeMillis();
                }
                Thread.sleep(7000);
            }
            if (!predicate.evaluate()) {
                LOG.info("Waiting timed out after [{}] msec", timeout);
            }
            return System.currentTimeMillis() - started;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    protected long waitForStatus(final String entityType, final String entityName, final String instanceTime,
                                 final InstancesResult.WorkflowStatus instanceStatus) {
        return waitFor(WAIT_TIME, new Predicate() {
            public boolean evaluate() throws Exception {
                InstancesResult.WorkflowStatus status = falconUnitClient.getInstanceStatus(entityType,
                        entityName, instanceTime);
                return instanceStatus.equals(status);
            }
        });
    }

    public void assertStatus(APIResult apiResult) {
        Assert.assertEquals(APIResult.Status.SUCCEEDED, apiResult.getStatus());
    }

    public InstancesResult.WorkflowStatus getRetentionStatus(String feedName, String cluster) throws FalconException {
        Feed feedEntity = EntityUtil.getEntity(EntityType.FEED, feedName);

        Frequency feedFrequency = feedEntity.getFrequency();
        Frequency defaultFrequency = new Frequency("hours(24)");
        long endTimeInMillis = System.currentTimeMillis() + 30000;
        String endTime = DateUtil.getDateFormatFromTime(endTimeInMillis);
        long startTimeInMillis;
        if (DateUtil.getFrequencyInMillis(feedFrequency) < DateUtil.getFrequencyInMillis(defaultFrequency)) {
            startTimeInMillis = endTimeInMillis - (6 * DateUtil.HOUR_IN_MILLIS);
        } else {
            startTimeInMillis = endTimeInMillis - (24 * DateUtil.HOUR_IN_MILLIS);
        }
        String startTime = DateUtil.getDateFormatFromTime(startTimeInMillis);
        List<LifeCycle> lifecycles = new ArrayList<>();
        lifecycles.add(LifeCycle.EVICTION);
        InstancesResult result = falconUnitClient.getStatusOfInstances("feed", feedName, startTime, endTime, cluster,
                lifecycles, null, "status", "asc", 0, 1, null, null);
        if (result.getInstances() != null && result.getInstances().length > 0) {
            return result.getInstances()[0].getStatus();
        }
        return null;
    }

}
