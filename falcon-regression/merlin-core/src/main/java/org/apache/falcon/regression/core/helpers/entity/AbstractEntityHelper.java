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

package org.apache.falcon.regression.core.helpers.entity;

import com.jcraft.jsch.JSchException;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.falcon.regression.core.helpers.FalconClientBuilder;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.supportClasses.ExecResult;
import org.apache.falcon.regression.core.util.Config;
import org.apache.falcon.regression.core.util.ExecUtil;
import org.apache.falcon.regression.core.util.FileUtil;
import org.apache.falcon.regression.core.util.HCatUtil;
import org.apache.falcon.regression.core.util.HiveUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OozieUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.resource.FeedInstanceResult;
import org.apache.falcon.resource.InstancesResult;
import org.apache.falcon.resource.InstancesSummaryResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.log4j.Logger;
import org.apache.oozie.client.OozieClient;
import org.testng.Assert;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/** Abstract class for helper classes. */
public abstract class AbstractEntityHelper {

    public static final boolean AUTHENTICATE = setAuthenticate();

    private static final Logger LOGGER = Logger.getLogger(AbstractEntityHelper.class);

    private static boolean setAuthenticate() {
        String value = Config.getProperty("isAuthenticationSet");
        value = (null == value) ? "true" : value;
        return !value.equalsIgnoreCase("false");
    }

    public String getActiveMQ() {
        return activeMQ;
    }

    public String getHadoopLocation() {
        return hadoopLocation;
    }

    public String getHadoopURL() {
        return hadoopURL;
    }

    public String getClusterReadonly() {
        return clusterReadonly;
    }

    public String getClusterWrite() {
        return clusterWrite;
    }

    public String getHostname() {
        return hostname;
    }

    public String getPassword() {
        return password;
    }

    public String getStoreLocation() {
        return storeLocation;
    }

    public String getUsername() {
        return username;
    }

    public String getHCatEndpoint() {
        return hcatEndpoint;
    }

    protected HCatClient hCatClient;

    public HCatClient getHCatClient() {
        if (null == this.hCatClient) {
            try {
                this.hCatClient = HCatUtil.getHCatClient(hcatEndpoint, hiveMetaStorePrincipal);
            } catch (HCatException e) {
                Assert.fail("Unable to create hCatClient because of exception:\n"
                    + ExceptionUtils.getStackTrace(e));
            }
        }
        return this.hCatClient;
    }

    protected Connection hiveJdbcConnection;

    public Connection getHiveJdbcConnection() {
        if (null == hiveJdbcConnection) {
            try {
                hiveJdbcConnection =
                    HiveUtil.getHiveJdbcConnection(hiveJdbcUrl, hiveJdbcUser, hiveJdbcPassword, hiveMetaStorePrincipal);
            } catch (ClassNotFoundException | SQLException | InterruptedException | IOException e) {
                Assert.fail("Unable to create hive jdbc connection because of exception:\n"
                    + ExceptionUtils.getStackTrace(e));
            }
        }
        return hiveJdbcConnection;
    }

    //basic properties
    protected String qaHost;

    public String getQaHost() {
        return qaHost;
    }

    protected String hostname = "";
    protected String username = "";
    protected String password = "";
    protected String hadoopLocation = "";
    protected String hadoopURL = "";
    protected String clusterReadonly = "";
    protected String clusterWrite = "";
    private String oozieURL = "";
    protected String activeMQ = "";
    protected String storeLocation = "";
    protected String colo;
    protected String allColo;
    protected String coloName;
    protected String serviceStartCmd;
    protected String serviceStopCmd;
    protected String serviceStatusCmd;
    protected String hcatEndpoint = "";
    protected String hiveJdbcUrl = "";
    protected String hiveJdbcUser = "";
    protected String hiveJdbcPassword = "";

    public String getNamenodePrincipal() {
        return namenodePrincipal;
    }

    public String getHiveMetaStorePrincipal() {
        return hiveMetaStorePrincipal;
    }

    protected String namenodePrincipal;
    protected String hiveMetaStorePrincipal;

    public OozieClient getOozieClient() {
        if (null == this.oozieClient) {
            this.oozieClient = OozieUtil.getClient(this.oozieURL);
        }
        return this.oozieClient;
    }

    protected OozieClient oozieClient;

    public FileSystem getHadoopFS() throws IOException {
        if (null == this.hadoopFS) {
            Configuration conf = new Configuration();
            conf.setBoolean("fs.hdfs.impl.disable.cache", true);
            conf.set("fs.default.name", "hdfs://" + this.hadoopURL);
            this.hadoopFS = FileSystem.get(conf);
        }
        return this.hadoopFS;
    }

    protected FileSystem hadoopFS;

    public String getIdentityFile() {
        return identityFile;
    }

    protected String identityFile;

    protected String serviceStatusMsg;

    public String getServiceUser() {
        return serviceUser;
    }

    public String getServiceStopCmd() {
        return serviceStopCmd;
    }

    public String getServiceStartCmd() {
        return serviceStartCmd;
    }

    protected String serviceUser;

    public String getColo() {
        return colo;
    }

    public String getColoName() {
        return coloName;
    }

    public AbstractEntityHelper(String prefix) {
        if ((null == prefix) || prefix.isEmpty()) {
            prefix = "";
        } else {
            prefix += ".";
        }
        this.qaHost = Config.getProperty(prefix + "qa_host");
        this.hostname = Config.getProperty(prefix + "hostname");
        this.username = Config.getProperty(prefix + "username", System.getProperty("user.name"));
        this.password = Config.getProperty(prefix + "password", "");
        this.hadoopLocation = Config.getProperty(prefix + "hadoop_location");
        this.hadoopURL = Config.getProperty(prefix + "hadoop_url");
        this.hcatEndpoint = Config.getProperty(prefix + "hcat_endpoint");
        this.clusterReadonly = Config.getProperty(prefix + "cluster_readonly");
        this.clusterWrite = Config.getProperty(prefix + "cluster_write");
        this.oozieURL = Config.getProperty(prefix + "oozie_url");
        this.activeMQ = Config.getProperty(prefix + "activemq_url");
        this.storeLocation = Config.getProperty(prefix + "storeLocation");
        this.allColo = "?colo=" + Config.getProperty(prefix + "colo", "*");
        this.colo = (!Config.getProperty(prefix + "colo", "").isEmpty()) ? "?colo=" + Config
            .getProperty(prefix + "colo") : "";
        this.coloName = this.colo.contains("=") ? this.colo.split("=")[1] : "";
        this.serviceStartCmd =
            Config.getProperty(prefix + "service_start_cmd", "/etc/init.d/tomcat6 start");
        this.serviceStopCmd = Config.getProperty(prefix + "service_stop_cmd",
            "/etc/init.d/tomcat6 stop");
        this.serviceUser = Config.getProperty(prefix + "service_user", null);
        this.serviceStatusMsg = Config.getProperty(prefix + "service_status_msg",
            "Tomcat servlet engine is running with pid");
        this.serviceStatusCmd =
            Config.getProperty(prefix + "service_status_cmd", "/etc/init.d/tomcat6 status");
        this.identityFile = Config.getProperty(prefix + "identityFile",
            System.getProperty("user.home") + "/.ssh/id_rsa");
        this.hadoopFS = null;
        this.oozieClient = null;
        this.namenodePrincipal = Config.getProperty(prefix + "namenode.kerberos.principal", "none");
        this.hiveMetaStorePrincipal = Config.getProperty(
                prefix + "hive.metastore.kerberos.principal", "none");
        this.hiveJdbcUrl = Config.getProperty(prefix + "hive.jdbc.url", "none");
        this.hiveJdbcUser =
            Config.getProperty(prefix + "hive.jdbc.user", System.getProperty("user.name"));
        this.hiveJdbcPassword = Config.getProperty(prefix + "hive.jdbc.password", "");
    }

    public abstract String getEntityType();

    public abstract String getEntityName(String entity);

    protected String createUrl(String... parts) {
        return StringUtils.join(parts, "/");
    }

    public ServiceResponse listEntities(String entityType, String params, String user)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        if (StringUtils.isEmpty(entityType)) {
            entityType = getEntityType();
        }
        LOGGER.info("fetching " + entityType + " list");
        String url = createUrl(this.hostname + URLS.LIST_URL.getValue(), entityType + colo);
        if (StringUtils.isNotEmpty(params)){
            url += colo.isEmpty() ? "?" + params : "&" + params;
        }
        return Util.sendRequest(createUrl(url), "get", null, user);
    }

    public ServiceResponse listAllEntities()
        throws AuthenticationException, IOException, URISyntaxException, InterruptedException {
        return listAllEntities(null, null);
    }

    public ServiceResponse listAllEntities(String params, String user)
        throws AuthenticationException, IOException, URISyntaxException, InterruptedException {
        return listEntities(null, (params == null ? "" : params + '&')
            + "numResults=" + Integer.MAX_VALUE, user);
    }

    public ServiceResponse submitEntity(String data)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return submitEntity(data, null);
    }

    public ServiceResponse validateEntity(String data)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return validateEntity(data, null);
    }

    public ServiceResponse submitEntity(String data, String user)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        LOGGER.info("Submitting " + getEntityType() + ": \n" + Util.prettyPrintXml(data));
        return Util.sendRequest(createUrl(this.hostname + URLS.SUBMIT_URL.getValue(),
            getEntityType() + colo), "post", data, user);
    }

    public ServiceResponse validateEntity(String data, String user)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        LOGGER.info("Validating " + getEntityType() + ": \n" + Util.prettyPrintXml(data));
        return Util.sendRequest(createUrl(this.hostname + URLS.VALIDATE_URL.getValue(),
            getEntityType() + colo), "post", data, user);
    }

    public ServiceResponse schedule(String processData)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return schedule(processData, null);
    }

    public ServiceResponse schedule(String processData, String user)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return Util.sendRequest(createUrl(this.hostname + URLS.SCHEDULE_URL.getValue(),
            getEntityType(), getEntityName(processData) + colo), "post", user);
    }

    public ServiceResponse submitAndSchedule(String data)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return submitAndSchedule(data, null);
    }

    public ServiceResponse submitAndSchedule(String data, String user)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        LOGGER.info("Submitting " + getEntityType() + ": \n" + Util.prettyPrintXml(data));
        return Util.sendRequest(createUrl(this.hostname + URLS.SUBMIT_AND_SCHEDULE_URL.getValue(),
            getEntityType()), "post", data, user);
    }

    public ServiceResponse deleteByName(String entityName, String user)
        throws AuthenticationException, IOException, URISyntaxException, InterruptedException {
        return Util.sendRequest(createUrl(this.hostname + URLS.DELETE_URL.getValue(),
            getEntityType(), entityName + colo), "delete", user);
    }

    public ServiceResponse delete(String data)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return delete(data, null);
    }

    public ServiceResponse delete(String data, String user)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return Util.sendRequest(createUrl(this.hostname + URLS.DELETE_URL.getValue(),
            getEntityType(), getEntityName(data) + colo), "delete", user);
    }

    public ServiceResponse suspend(String data)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return suspend(data, null);
    }

    public ServiceResponse suspend(String data, String user)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return Util.sendRequest(createUrl(this.hostname + URLS.SUSPEND_URL.getValue(),
            getEntityType(), getEntityName(data) + colo), "post", user);
    }

    public ServiceResponse resume(String data)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return resume(data, null);
    }

    public ServiceResponse resume(String data, String user)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return Util.sendRequest(createUrl(this.hostname + URLS.RESUME_URL.getValue(),
            getEntityType(), getEntityName(data) + colo), "post", user);
    }

    public ServiceResponse getStatus(String data)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return getStatus(data, null);
    }

    public ServiceResponse getStatus(String data, String user)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return Util.sendRequest(createUrl(this.hostname + URLS.STATUS_URL.getValue(),
            getEntityType(), getEntityName(data) + colo), "get", user);
    }

    public ServiceResponse getEntityDefinition(String data)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return getEntityDefinition(data, null);
    }

    public ServiceResponse getEntityDefinition(String data, String user)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return Util.sendRequest(createUrl(this.hostname + URLS.GET_ENTITY_DEFINITION.getValue(),
            getEntityType(), getEntityName(data) + colo), "get", user);
    }

    public ServiceResponse getEntityDependencies(String data, String user)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return Util.sendRequest(createUrl(this.hostname + URLS.DEPENDENCIES.getValue(),
            getEntityType(), getEntityName(data) + colo), "get", user);
    }

    public InstancesResult getRunningInstance(String name)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return getRunningInstance(name, null);
    }

    public InstancesResult getRunningInstance(String name, String user)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        String url = createUrl(this.hostname + URLS.INSTANCE_RUNNING.getValue(), getEntityType(),
            name + allColo);
        return (InstancesResult) InstanceUtil.sendRequestProcessInstance(url, user);
    }

    public InstancesResult getProcessInstanceStatus(String entityName, String params)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return getProcessInstanceStatus(entityName, params, null);
    }

    public InstancesResult getProcessInstanceStatus(
        String entityName, String params, String user)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        String url = createUrl(this.hostname + URLS.INSTANCE_STATUS.getValue(), getEntityType(),
            entityName, "");
        return (InstancesResult) InstanceUtil
            .createAndSendRequestProcessInstance(url, params, allColo, user);
    }

    public InstancesResult getProcessInstanceLogs(String entityName, String params)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return getProcessInstanceLogs(entityName, params, null);
    }

    public InstancesResult getProcessInstanceLogs(String entityName, String params,
                                                  String user)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        String url = createUrl(this.hostname + URLS.INSTANCE_LOGS.getValue(), getEntityType(),
            entityName);
        if (StringUtils.isNotEmpty(params)) {
            url += "?";
        }
        return (InstancesResult) InstanceUtil
            .createAndSendRequestProcessInstance(url, params, allColo, user);
    }

    public InstancesResult getProcessInstanceSuspend(
        String readEntityName, String params)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return getProcessInstanceSuspend(readEntityName, params, null);
    }

    public InstancesResult getProcessInstanceSuspend(
        String entityName, String params, String user)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        String url = createUrl(this.hostname + URLS.INSTANCE_SUSPEND.getValue(), getEntityType(),
            entityName, "");
        return (InstancesResult) InstanceUtil
            .createAndSendRequestProcessInstance(url, params, allColo, user);
    }

    public ServiceResponse update(String oldEntity, String newEntity)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return update(oldEntity, newEntity, null);
    }

    public ServiceResponse update(String oldEntity, String newEntity, String user)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        LOGGER.info("Updating " + getEntityType() + ": \n" + Util.prettyPrintXml(oldEntity));
        LOGGER.info("To " + getEntityType() + ": \n" + Util.prettyPrintXml(newEntity));
        String url = createUrl(this.hostname + URLS.UPDATE.getValue(), getEntityType(),
            getEntityName(oldEntity));
        return Util.sendRequest(url + colo, "post", newEntity, user);
    }

    public InstancesResult getProcessInstanceKill(String readEntityName, String params)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return getProcessInstanceKill(readEntityName, params, null);
    }

    public InstancesResult getProcessInstanceKill(String entityName, String params,
                                                         String user)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        String url = createUrl(this.hostname + URLS.INSTANCE_KILL.getValue(), getEntityType(),
            entityName, "");
        return (InstancesResult) InstanceUtil
            .createAndSendRequestProcessInstance(url, params, allColo, user);
    }

    public InstancesResult getProcessInstanceRerun(String entityName, String params)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return getProcessInstanceRerun(entityName, params, null);
    }

    public InstancesResult getProcessInstanceRerun(String entityName, String params,
                                                          String user)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        String url = createUrl(this.hostname + URLS.INSTANCE_RERUN.getValue(), getEntityType(),
            entityName, "");
        return (InstancesResult) InstanceUtil
            .createAndSendRequestProcessInstance(url, params, allColo, user);
    }

    public InstancesResult getProcessInstanceResume(String entityName, String params)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return getProcessInstanceResume(entityName, params, null);
    }

    public InstancesResult getProcessInstanceResume(String entityName, String params,
                                                           String user)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        String url = createUrl(this.hostname + URLS.INSTANCE_RESUME.getValue(), getEntityType(),
            entityName, "");
        return (InstancesResult) InstanceUtil
            .createAndSendRequestProcessInstance(url, params, allColo, user);
    }

    public FeedInstanceResult getFeedInstanceListing(String entityName, String params)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return getFeedInstanceListing(entityName, params, null);
    }

    public FeedInstanceResult getFeedInstanceListing(String entityName, String params,
                                                     String user)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        String url = createUrl(this.hostname + URLS.INSTANCE_LISTING.getValue(), getEntityType(),
                entityName, "");
        return (FeedInstanceResult) InstanceUtil
                .createAndSendRequestProcessInstance(url, params, allColo, user);
    }

    public InstancesSummaryResult getInstanceSummary(String entityName, String params)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        String url = createUrl(this.hostname + URLS.INSTANCE_SUMMARY.getValue(), getEntityType(),
            entityName, "");
        return (InstancesSummaryResult) InstanceUtil
            .createAndSendRequestProcessInstance(url, params, allColo, null);
    }

    public List<String> getArchiveInfo() throws IOException, JSchException {
        return Util.getStoreInfo(this, "/archive/" + getEntityType().toUpperCase());
    }

    public List<String> getStoreInfo() throws IOException, JSchException {
        return Util.getStoreInfo(this, "/" + getEntityType().toUpperCase());
    }

    public InstancesResult getInstanceParams(String entityName, String params)
        throws AuthenticationException, IOException, URISyntaxException, InterruptedException {
        String url = createUrl(this.hostname + URLS.INSTANCE_PARAMS.getValue(), getEntityType(),
            entityName, "");
        return (InstancesResult) InstanceUtil
            .createAndSendRequestProcessInstance(url, params, allColo, null);
    }

    /**
     * Lists all entities which are tagged by a given pipeline.
     * @param pipeline filter
     * @return service response
     * @throws AuthenticationException
     * @throws IOException
     * @throws URISyntaxException
     */
    public ServiceResponse getListByPipeline(String pipeline)
        throws AuthenticationException, IOException, URISyntaxException, InterruptedException {
        String url = createUrl(this.hostname + URLS.LIST_URL.getValue() + "/" + getEntityType());
        url += "?filterBy=PIPELINES:" + pipeline;
        return Util.sendRequest(url, "get", null, null);
    }

    /**
     * Submit an entity through falcon client.
     * @param entityStr string of the entity to be submitted
     * @throws IOException
     */
    public ExecResult clientSubmit(final String entityStr) throws IOException {
        LOGGER.info("Submitting " + getEntityType() + " through falcon client: \n"
            + Util.prettyPrintXml(entityStr));
        final String fileName = FileUtil.writeEntityToFile(entityStr);
        final CommandLine commandLine = FalconClientBuilder.getBuilder()
                .getSubmitCommand(getEntityType(), fileName).build();
        return ExecUtil.executeCommand(commandLine);
    }

    /**
     * Delete an entity through falcon client.
     * @param entityStr string of the entity to be submitted
     * @throws IOException
     */
    public ExecResult clientDelete(final String entityStr, String user) throws IOException {
        final String entityName = getEntityName(entityStr);
        LOGGER.info("Deleting " + getEntityType() + ": " + entityName);
        final CommandLine commandLine = FalconClientBuilder.getBuilder(user)
                .getDeleteCommand(getEntityType(), entityName).build();
        return ExecUtil.executeCommand(commandLine);
    }


    /**
     * Retrieves entities summary.
     * @param clusterName compulsory parameter for request
     * @param params list of optional parameters
     * @return entity summary along with its instances.
     */
    public ServiceResponse getEntitySummary(String clusterName, String params)
        throws AuthenticationException, IOException, URISyntaxException, InterruptedException {
        String url = createUrl(this.hostname + URLS.ENTITY_SUMMARY.getValue(),
            getEntityType()) +"?cluster=" + clusterName;
        if (StringUtils.isNotEmpty(params)) {
            url += "&" + params;
        }
        return Util.sendRequest(url, "get", null, null);
    }

    /**
     * Get list of all instances of a given entity.
     * @param entityName entity name
     * @param params list of optional parameters
     * @param user user name
     * @return response
     */
    public InstancesResult listInstances(String entityName, String params, String user)
        throws AuthenticationException, IOException, URISyntaxException, InterruptedException {
        String url = createUrl(this.hostname + URLS.INSTANCE_LIST.getValue(), getEntityType(),
            entityName + colo);
        if (StringUtils.isNotEmpty(params)) {
            url += colo.isEmpty() ? "?" + params : "&" + params;
        }
        return (InstancesResult) InstanceUtil.sendRequestProcessInstance(url, user);
    }

    /**
     * Get list of all dependencies of a given entity.
     * @param entityName entity name
     * @return response
     * @throws URISyntaxException
     * @throws AuthenticationException
     * @throws InterruptedException
     * @throws IOException
     */
    public ServiceResponse getDependencies(String entityName)
        throws URISyntaxException, AuthenticationException, InterruptedException, IOException {
        String url = createUrl(this.hostname + URLS.DEPENDENCIES.getValue(), getEntityType(),
            entityName + colo);
        return Util.sendRequest(url, "get", null, null);
    }

    public ServiceResponse touchEntity(String data)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return touchEntity(Util.readEntityName(data), data, null);
    }

    public ServiceResponse touchEntity(String entityName, String data, String user)
        throws AuthenticationException, IOException, URISyntaxException, InterruptedException {
        String url = createUrl(this.hostname + URLS.TOUCH_URL.getValue(), getEntityType(),
                entityName + colo);
        return Util.sendRequest(url, "post", data, user);
    }
}
