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

package org.apache.falcon.regression.core.interfaces;

import com.jcraft.jsch.JSchException;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.falcon.regression.core.response.InstancesSummaryResult;
import org.apache.falcon.regression.core.response.InstancesResult;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.Config;
import org.apache.falcon.regression.core.util.ExecUtil;
import org.apache.falcon.regression.core.util.HCatUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.OozieUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.commons.lang.StringUtils;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.log4j.Logger;
import org.apache.oozie.client.AuthOozieClient;
import org.testng.Assert;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

/** Abstract class for helper classes. */
public abstract class IEntityManagerHelper {

    public static final boolean AUTHENTICATE = setAuthenticate();

    private static final Logger LOGGER = Logger.getLogger(IEntityManagerHelper.class);

    protected static final String CLIENT_LOCATION = OSUtil.RESOURCES
        + OSUtil.getPath("IvoryClient", "IvoryCLI.jar");
    protected static final String BASE_COMMAND = "java -jar " + CLIENT_LOCATION;

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
    protected String hadoopGetCommand = "";
    protected String colo;
    protected String allColo;
    protected String coloName;
    protected String serviceStartCmd;
    protected String serviceStopCmd;
    protected String serviceStatusCmd;
    protected String hcatEndpoint = "";

    public String getNamenodePrincipal() {
        return namenodePrincipal;
    }

    public String getHiveMetaStorePrincipal() {
        return hiveMetaStorePrincipal;
    }

    protected String namenodePrincipal;
    protected String hiveMetaStorePrincipal;

    public AuthOozieClient getOozieClient() {
        if (null == this.oozieClient) {
            this.oozieClient = OozieUtil.getClient(this.oozieURL);
        }
        return this.oozieClient;
    }

    protected AuthOozieClient oozieClient;

    public FileSystem getHadoopFS() throws IOException {
        if (null == this.hadoopFS) {
            Configuration conf = new Configuration();
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

    public IEntityManagerHelper(String prefix) {
        if ((null == prefix) || prefix.isEmpty()) {
            prefix = "";
        } else {
            prefix += ".";
        }
        this.qaHost = Config.getProperty(prefix + "qa_host");
        this.hostname = Config.getProperty(prefix + "ivory_hostname");
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
        this.hadoopGetCommand = hadoopLocation + "  fs -cat hdfs://" + hadoopURL
                + "/projects/ivory/staging/ivory/workflows/process";
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
    }

    public abstract String getEntityType();

    public abstract String getEntityName(String entity);

    protected String createUrl(String... parts) {
        return StringUtils.join(parts, "/");
    }

    public ServiceResponse listEntities(URLS url)
        throws IOException, URISyntaxException, AuthenticationException {
        return listEntities(url, null);
    }

    public ServiceResponse listEntities(Util.URLS url, String user)
        throws IOException, URISyntaxException, AuthenticationException {
        LOGGER.info("fetching " + getEntityType() + " list");
        return Util.sendRequest(createUrl(this.hostname + url.getValue(), getEntityType() + colo),
            "get", null, user);
    }

    public ServiceResponse submitEntity(URLS url, String data)
        throws IOException, URISyntaxException, AuthenticationException {
        return submitEntity(url, data, null);
    }

    public ServiceResponse submitEntity(URLS url, String data, String user)
        throws IOException, URISyntaxException, AuthenticationException {
        LOGGER.info("Submitting " + getEntityType() + ": \n" + Util.prettyPrintXml(data));
        return Util.sendRequest(createUrl(this.hostname + url.getValue(), getEntityType() + colo),
            "post", data, user);
    }

    public ServiceResponse schedule(URLS scheduleUrl, String processData)
        throws IOException, URISyntaxException, AuthenticationException {
        return schedule(scheduleUrl, processData, null);
    }

    public ServiceResponse schedule(URLS scheduleUrl, String processData, String user)
        throws IOException, URISyntaxException, AuthenticationException {
        return Util.sendRequest(createUrl(this.hostname + scheduleUrl.getValue(), getEntityType(),
            getEntityName(processData) + colo), "post", user);
    }

    public ServiceResponse submitAndSchedule(URLS url, String data)
        throws IOException, URISyntaxException, AuthenticationException {
        return submitAndSchedule(url, data, null);
    }

    public ServiceResponse submitAndSchedule(URLS url, String data, String user)
        throws IOException, URISyntaxException, AuthenticationException {
        LOGGER.info("Submitting " + getEntityType() + ": \n" + Util.prettyPrintXml(data));
        return Util.sendRequest(createUrl(this.hostname + url.getValue(), getEntityType()), "post",
            data, user);
    }

    public ServiceResponse deleteByName(URLS deleteUrl, String entityName, String user)
        throws AuthenticationException, IOException, URISyntaxException {
        return Util.sendRequest(
            createUrl(this.hostname + deleteUrl.getValue(), getEntityType(), entityName + colo),
            "delete", user);
    }

    public ServiceResponse delete(URLS deleteUrl, String data)
        throws IOException, URISyntaxException, AuthenticationException {
        return delete(deleteUrl, data, null);
    }

    public ServiceResponse delete(URLS deleteUrl, String data, String user)
        throws IOException, URISyntaxException, AuthenticationException {
        return Util.sendRequest(
            createUrl(this.hostname + deleteUrl.getValue(), getEntityType(),
                getEntityName(data) + colo),
            "delete", user);
    }

    public ServiceResponse suspend(URLS suspendUrl, String data)
        throws IOException, URISyntaxException, AuthenticationException {
        return suspend(suspendUrl, data, null);
    }

    public ServiceResponse suspend(URLS url, String data, String user)
        throws IOException, URISyntaxException, AuthenticationException {
        return Util.sendRequest(
            createUrl(this.hostname + url.getValue(), getEntityType(), getEntityName(data) + colo),
            "post", user);
    }

    public ServiceResponse resume(URLS url, String data)
        throws IOException, URISyntaxException, AuthenticationException {
        return resume(url, data, null);
    }

    public ServiceResponse resume(URLS url, String data, String user)
        throws IOException, URISyntaxException, AuthenticationException {
        return Util.sendRequest(
            createUrl(this.hostname + url.getValue(), getEntityType(), getEntityName(data) + colo),
            "post", user);
    }

    public ServiceResponse getStatus(URLS url, String data)
        throws IOException, URISyntaxException, AuthenticationException {
        return getStatus(url, data, null);
    }

    public ServiceResponse getStatus(Util.URLS url, String data, String user)
        throws IOException, URISyntaxException, AuthenticationException {
        return Util.sendRequest(
            createUrl(this.hostname + url.getValue(), getEntityType(), getEntityName(data) + colo),
            "get", user);
    }

    public ServiceResponse getEntityDefinition(URLS url, String data)
        throws IOException, URISyntaxException, AuthenticationException {
        return getEntityDefinition(url, data, null);
    }

    public ServiceResponse getEntityDefinition(URLS url, String data, String user)
        throws IOException, URISyntaxException, AuthenticationException {
        return Util.sendRequest(
            createUrl(this.hostname + url.getValue(), getEntityType(), getEntityName(data) + colo),
            "get", user);
    }

    public InstancesResult getRunningInstance(URLS processRunningInstance, String name)
        throws IOException, URISyntaxException, AuthenticationException {
        return getRunningInstance(processRunningInstance, name, null);
    }

    public InstancesResult getRunningInstance(
        URLS processRunningInstance, String name, String user)
        throws IOException, URISyntaxException, AuthenticationException {
        String url = createUrl(this.hostname + processRunningInstance.getValue(), getEntityType(),
            name + allColo);
        return (InstancesResult) InstanceUtil.sendRequestProcessInstance(url, user);
    }

    public InstancesResult getProcessInstanceStatus(String entityName, String params)
        throws IOException, URISyntaxException, AuthenticationException {
        return getProcessInstanceStatus(entityName, params, null);
    }

    public InstancesResult getProcessInstanceStatus(
        String entityName, String params, String user)
        throws IOException, URISyntaxException, AuthenticationException {
        String url = createUrl(this.hostname + Util.URLS.INSTANCE_STATUS.getValue(),
            getEntityType(), entityName, "");
        return (InstancesResult) InstanceUtil
            .createAndSendRequestProcessInstance(url, params, allColo, user);
    }

    public InstancesResult getProcessInstanceSuspend(
        String readEntityName, String params)
        throws IOException, URISyntaxException, AuthenticationException {
        return getProcessInstanceSuspend(readEntityName, params, null);
    }

    public InstancesResult getProcessInstanceSuspend(
        String entityName, String params, String user)
        throws IOException, URISyntaxException, AuthenticationException {
        String url = createUrl(this.hostname + Util.URLS.INSTANCE_SUSPEND.getValue(),
            getEntityType(), entityName, "");
        return (InstancesResult) InstanceUtil
            .createAndSendRequestProcessInstance(url, params, allColo, user);
    }

    public ServiceResponse update(String oldEntity, String newEntity)
        throws IOException, URISyntaxException, AuthenticationException {
        return update(oldEntity, newEntity, null);
    }

    public ServiceResponse update(String oldEntity, String newEntity, String user)
        throws IOException, URISyntaxException, AuthenticationException {
        String url = createUrl(this.hostname + Util.URLS.UPDATE.getValue(), getEntityType(),
            getEntityName(oldEntity));
        return Util.sendRequest(url + colo, "post", newEntity, user);
    }

    public ServiceResponse update(String oldEntity, String newEntity, String updateTime,
                                  String user)
        throws IOException, URISyntaxException, AuthenticationException {
        String url = this.hostname + URLS.UPDATE.getValue() + "/" + getEntityType() + "/"
            + Util.readEntityName(oldEntity);
        String urlPart = colo == null || colo.isEmpty() ? "?" : colo + "&";
        return Util.sendRequest(url + urlPart + "effective=" + updateTime, "post",
            newEntity, user);
    }

    public InstancesResult getProcessInstanceKill(String readEntityName, String params)
        throws IOException, URISyntaxException, AuthenticationException {
        return getProcessInstanceKill(readEntityName, params, null);
    }

    public InstancesResult getProcessInstanceKill(String entityName, String params,
                                                         String user)
        throws IOException, URISyntaxException, AuthenticationException {
        String url = createUrl(this.hostname + URLS.INSTANCE_KILL.getValue(), getEntityType(),
            entityName, "");
        return (InstancesResult) InstanceUtil
            .createAndSendRequestProcessInstance(url, params, allColo, user);
    }

    public InstancesResult getProcessInstanceRerun(String entityName, String params)
        throws IOException, URISyntaxException, AuthenticationException {
        return getProcessInstanceRerun(entityName, params, null);
    }

    public InstancesResult getProcessInstanceRerun(String entityName, String params,
                                                          String user)
        throws IOException, URISyntaxException, AuthenticationException {
        String url = createUrl(this.hostname + URLS.INSTANCE_RERUN.getValue(), getEntityType(),
            entityName, "");
        return (InstancesResult) InstanceUtil
            .createAndSendRequestProcessInstance(url, params, allColo, user);
    }

    public InstancesResult getProcessInstanceResume(String entityName, String params)
        throws IOException, URISyntaxException, AuthenticationException {
        return getProcessInstanceResume(entityName, params, null);
    }

    public InstancesResult getProcessInstanceResume(String entityName, String params,
                                                           String user)
        throws IOException, URISyntaxException, AuthenticationException {
        String url = createUrl(this.hostname + Util.URLS.INSTANCE_RESUME.getValue(),
            getEntityType(), entityName, "");
        return (InstancesResult) InstanceUtil
            .createAndSendRequestProcessInstance(url, params, allColo, user);
    }

    public InstancesSummaryResult getInstanceSummary(String entityName,
                                                     String params
    ) throws IOException, URISyntaxException, AuthenticationException {
        String url = createUrl(this.hostname + URLS.INSTANCE_SUMMARY.getValue(), getEntityType(),
            entityName, "");
        return (InstancesSummaryResult) InstanceUtil
            .createAndSendRequestProcessInstance(url, params, allColo, null);
    }

    public String list() {
        return ExecUtil.executeCommandGetOutput(
            BASE_COMMAND + " entity -list -url " + this.hostname + " -type " + getEntityType());
    }

    public String getDependencies(String entityName) {
        return ExecUtil.executeCommandGetOutput(
            BASE_COMMAND + " entity -dependency -url " + this.hostname + " -type "
                + getEntityType() + " -name " + entityName);
    }

    public List<String> getArchiveInfo() throws IOException, JSchException {
        return Util.getStoreInfo(this, "/archive/" + getEntityType().toUpperCase());
    }

    public List<String> getStoreInfo() throws IOException, JSchException {
        return Util.getStoreInfo(this, "/" + getEntityType().toUpperCase());
    }

    public InstancesResult getInstanceParams(String entityName, String params)
        throws AuthenticationException, IOException, URISyntaxException {
        String url = createUrl(this.hostname + Util.URLS.INSTANCE_PARAMS.getValue(),
            getEntityType(), entityName, "");
        return (InstancesResult) InstanceUtil
            .createAndSendRequestProcessInstance(url, params, allColo, null);
    }
}
