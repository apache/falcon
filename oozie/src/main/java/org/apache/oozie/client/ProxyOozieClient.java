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

package org.apache.oozie.client;

import org.apache.falcon.security.CurrentUser;
import org.apache.falcon.security.SecurityUtil;
import org.apache.falcon.util.RuntimeProperties;
import org.apache.hadoop.hdfs.web.KerberosUgiAuthenticator;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.Authenticator;
import org.apache.log4j.Logger;
import org.apache.oozie.client.rest.RestConstants;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;

/**
 * Wrapped Oozie Client that does proxy the requests.
 */
public class ProxyOozieClient extends AuthOozieClient {

    private static final Logger LOG = Logger.getLogger(ProxyOozieClient.class);
    private static final Map<String, String> NONE = new HashMap<String, String>();

    private final Authenticator authenticator = new KerberosUgiAuthenticator();

    public ProxyOozieClient(String oozieUrl) {
        super(oozieUrl, SecurityUtil.getAuthenticationType());

        if (org.apache.log4j.Logger.getLogger(getClass()).isDebugEnabled()) {
            setDebugMode(1);
        }
    }

    public Properties getConfiguration() throws OozieClientException {
        return (new OozieConfiguration(RestConstants.ADMIN_CONFIG_RESOURCE)).call();
    }

    public Properties getProperties() throws OozieClientException {
        return (new OozieConfiguration(RestConstants.ADMIN_JAVA_SYS_PROPS_RESOURCE)).call();
    }

    @Override
    protected Authenticator getAuthenticator() throws OozieClientException {
        return authenticator;
    }

    @Override
    protected HttpURLConnection createConnection(URL url, final String method)
        throws IOException, OozieClientException {

        final URL decoratedUrl = decorateUrlWithUser(url);
        if (LOG.isDebugEnabled()) {
            LOG.debug("ProxyOozieClient.createConnection: u=" + url + ", m=" + method);
        }

        UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
        try {
            return currentUser.doAs(new PrivilegedExceptionAction<HttpURLConnection>() {
                public HttpURLConnection run() throws Exception {
                    HttpURLConnection conn = ProxyOozieClient.super.createConnection(decoratedUrl, method);

                    int connectTimeout = Integer.valueOf(
                            RuntimeProperties.get().getProperty("oozie.connect.timeout", "1000"));
                    conn.setConnectTimeout(connectTimeout);

                    int readTimeout = Integer.valueOf(
                            RuntimeProperties.get().getProperty("oozie.read.timeout", "45000"));
                    conn.setReadTimeout(readTimeout);

                    return conn;
                }
            });
        } catch (InterruptedException e) {
            throw new IOException("Could not connect to oozie: " + e.getMessage(), e);
        }
    }

    protected URL decorateUrlWithUser(URL url) throws IOException {
        String strUrl = url.toString();

        if (!strUrl.contains(OozieClient.USER_NAME)) {
            // decorate the url with the proxy user in request
            String paramSeparator = (strUrl.contains("?")) ? "&" : "?";
            strUrl += paramSeparator + OozieClient.USER_NAME + "="
                    + UserGroupInformation.getLoginUser().getUserName();
            // strUrl += "&" + RestConstants.DO_AS_PARAM + "=" + CurrentUser.getUser();

            url = new URL(strUrl);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Decorated url with user info: " + url);
            }
        }

        return url;
    }

    private class OozieConfiguration extends ClientCallable<Properties> {

        public OozieConfiguration(String resource) {
            super("GET", RestConstants.ADMIN, resource, NONE);
        }

        @Override
        protected Properties call(HttpURLConnection conn)
            throws IOException, OozieClientException {
            conn.setRequestProperty("content-type", RestConstants.XML_CONTENT_TYPE);
            if ((conn.getResponseCode() == HttpURLConnection.HTTP_OK)) {
                Reader reader = new InputStreamReader(conn.getInputStream(), "UTF_8");
                JSONObject json = (JSONObject) JSONValue.parse(reader);
                Properties props = new Properties();
                props.putAll(json);
                return props;
            } else {
                handleError(conn);
                return null;
            }
        }
    }

    @Override
    public SYSTEM_MODE getSystemMode() throws OozieClientException {
        try {
            return doAs(CurrentUser.getUser(), new Callable<SYSTEM_MODE>() {

                public SYSTEM_MODE call() throws Exception {
                    return ProxyOozieClient.super.getSystemMode();
                }
            });
        } catch (Exception e) {
            throw new OozieClientException(OozieClientException.AUTHENTICATION, e);
        }
    }

    @Override
    public String submit(final Properties conf) throws OozieClientException {
        try {
            return doAs(CurrentUser.getUser(), new Callable<String>() {

                public String call() throws Exception {
                    return ProxyOozieClient.super.submit(conf);
                }
            });
        } catch (Exception e) {
            throw new OozieClientException(OozieClientException.AUTHENTICATION, e);
        }
    }

    @Override
    public String dryrun(final Properties conf) throws OozieClientException {
        try {
            return doAs(CurrentUser.getUser(), new Callable<String>() {

                public String call() throws Exception {
                    return ProxyOozieClient.super.dryrun(conf);
                }
            });
        } catch (Exception e) {
            throw new OozieClientException(OozieClientException.AUTHENTICATION, e);
        }

    }

    @Override
    public void start(final String jobId) throws OozieClientException {
        try {
            doAs(CurrentUser.getUser(), new Callable<Object>() {

                public String call() throws Exception {
                    ProxyOozieClient.super.start(jobId);
                    return null;
                }
            });
        } catch (Exception e) {
            throw new OozieClientException(OozieClientException.AUTHENTICATION, e);
        }
    }

    @Override
    public String run(final Properties conf) throws OozieClientException {
        try {
            return doAs(CurrentUser.getUser(), new Callable<String>() {

                public String call() throws Exception {
                    return ProxyOozieClient.super.run(conf);
                }
            });
        } catch (Exception e) {
            throw new OozieClientException(OozieClientException.AUTHENTICATION, e);
        }
    }

    @Override
    public void reRun(final String jobId, final Properties conf) throws OozieClientException {
        try {
            doAs(CurrentUser.getUser(), new Callable<Object>() {

                public Object call() throws Exception {
                    ProxyOozieClient.super.reRun(jobId, conf);
                    return null;
                }
            });
        } catch (Exception e) {
            throw new OozieClientException(OozieClientException.AUTHENTICATION, e);
        }
    }

    @Override
    public void suspend(final String jobId) throws OozieClientException {
        try {
            doAs(CurrentUser.getUser(), new Callable<Object>() {

                public Object call() throws Exception {
                    ProxyOozieClient.super.suspend(jobId);
                    return null;
                }
            });
        } catch (Exception e) {
            throw new OozieClientException(OozieClientException.AUTHENTICATION, e);
        }
    }

    @Override
    public void resume(final String jobId) throws OozieClientException {
        try {
            doAs(CurrentUser.getUser(), new Callable<Object>() {

                public Object call() throws Exception {
                    ProxyOozieClient.super.resume(jobId);
                    return null;
                }
            });
        } catch (Exception e) {
            throw new OozieClientException(OozieClientException.AUTHENTICATION, e);
        }
    }

    @Override
    public void kill(final String jobId) throws OozieClientException {
        try {
            doAs(CurrentUser.getUser(), new Callable<Object>() {

                public Object call() throws Exception {
                    ProxyOozieClient.super.kill(jobId);
                    return null;
                }
            });
        } catch (Exception e) {
            throw new OozieClientException(OozieClientException.AUTHENTICATION, e);
        }
    }

    @Override
    public void change(final String jobId, final String changeValue) throws OozieClientException {
        try {
            doAs(CurrentUser.getUser(), new Callable<Object>() {

                public Object call() throws Exception {
                    ProxyOozieClient.super.change(jobId, changeValue);
                    return null;
                }
            });
        } catch (Exception e) {
            throw new OozieClientException(OozieClientException.AUTHENTICATION, e);
        }
    }

    @Override
    public WorkflowJob getJobInfo(final String jobId) throws OozieClientException {
        try {
            return doAs(CurrentUser.getUser(), new Callable<WorkflowJob>() {

                public WorkflowJob call() throws Exception {
                    return ProxyOozieClient.super.getJobInfo(jobId);
                }
            });
        } catch (Exception e) {
            throw new OozieClientException(OozieClientException.AUTHENTICATION, e);
        }
    }

    @Override
    public WorkflowJob getJobInfo(final String jobId, final int start, final int len)
        throws OozieClientException {
        try {
            return doAs(CurrentUser.getUser(), new Callable<WorkflowJob>() {

                public WorkflowJob call() throws Exception {
                    return ProxyOozieClient.super.getJobInfo(jobId, start, len);
                }
            });
        } catch (Exception e) {
            throw new OozieClientException(OozieClientException.AUTHENTICATION, e);
        }
    }

    @Override
    public WorkflowAction getWorkflowActionInfo(final String actionId)
        throws OozieClientException {
        try {
            return doAs(CurrentUser.getUser(), new Callable<WorkflowAction>() {

                public WorkflowAction call() throws Exception {
                    return ProxyOozieClient.super.getWorkflowActionInfo(actionId);
                }
            });
        } catch (Exception e) {
            throw new OozieClientException(OozieClientException.AUTHENTICATION, e);
        }
    }

    @Override
    public String getJobLog(final String jobId) throws OozieClientException {
        try {
            return doAs(CurrentUser.getUser(), new Callable<String>() {

                public String call() throws Exception {
                    return ProxyOozieClient.super.getJobLog(jobId);
                }
            });
        } catch (Exception e) {
            throw new OozieClientException(OozieClientException.AUTHENTICATION, e);
        }
    }

    @Override
    public void getJobLog(final String jobId, final String logRetrievalType,
                          final String logRetrievalScope, final PrintStream ps)
        throws OozieClientException {
        try {
            doAs(CurrentUser.getUser(), new Callable<Object>() {

                public Object call() throws Exception {
                    ProxyOozieClient.super.getJobLog(jobId, logRetrievalType, logRetrievalScope, ps);
                    return null;
                }
            });
        } catch (Exception e) {
            throw new OozieClientException(OozieClientException.AUTHENTICATION, e);
        }
    }

    @Override
    public String getJobDefinition(final String jobId) throws OozieClientException {
        try {
            return doAs(CurrentUser.getUser(), new Callable<String>() {

                public String call() throws Exception {
                    return ProxyOozieClient.super.getJobDefinition(jobId);
                }
            });
        } catch (Exception e) {
            throw new OozieClientException(OozieClientException.AUTHENTICATION, e);
        }
    }

    @Override
    public BundleJob getBundleJobInfo(final String jobId) throws OozieClientException {
        try {
            return doAs(CurrentUser.getUser(), new Callable<BundleJob>() {

                public BundleJob call() throws Exception {
                    return ProxyOozieClient.super.getBundleJobInfo(jobId);
                }
            });
        } catch (Exception e) {
            throw new OozieClientException(OozieClientException.AUTHENTICATION, e);
        }
    }

    @Override
    public CoordinatorJob getCoordJobInfo(final String jobId) throws OozieClientException {
        try {
            return doAs(CurrentUser.getUser(), new Callable<CoordinatorJob>() {

                public CoordinatorJob call() throws Exception {
                    return ProxyOozieClient.super.getCoordJobInfo(jobId);
                }
            });
        } catch (Exception e) {
            throw new OozieClientException(OozieClientException.AUTHENTICATION, e);
        }
    }

    @Override
    public CoordinatorJob getCoordJobInfo(final String jobId, final String filter,
                                          final int start, final int len)
        throws OozieClientException {
        try {
            return doAs(CurrentUser.getUser(), new Callable<CoordinatorJob>() {

                public CoordinatorJob call() throws Exception {
                    return ProxyOozieClient.super.getCoordJobInfo(jobId, filter, start, len);
                }
            });
        } catch (Exception e) {
            throw new OozieClientException(OozieClientException.AUTHENTICATION, e);
        }
    }

    @Override
    public CoordinatorAction getCoordActionInfo(final String actionId) throws OozieClientException {
        try {
            return doAs(CurrentUser.getUser(), new Callable<CoordinatorAction>() {

                public CoordinatorAction call() throws Exception {
                    return ProxyOozieClient.super.getCoordActionInfo(actionId);
                }
            });
        } catch (Exception e) {
            throw new OozieClientException(OozieClientException.AUTHENTICATION, e);
        }
    }

    @Override
    public List<CoordinatorAction> reRunCoord(final String jobId, final String rerunType,
                                              final String scope, final boolean refresh,
                                              final boolean noCleanup)
        throws OozieClientException {
        try {
            return doAs(CurrentUser.getUser(), new Callable<List<CoordinatorAction>>() {

                public List<CoordinatorAction> call() throws Exception {
                    return ProxyOozieClient.super.reRunCoord(jobId, rerunType, scope, refresh, noCleanup);
                }
            });
        } catch (Exception e) {
            throw new OozieClientException(OozieClientException.AUTHENTICATION, e);
        }
    }

    @Override
    public Void reRunBundle(final String jobId, final String coordScope, final String dateScope,
                            final boolean refresh, final boolean noCleanup)
        throws OozieClientException {
        try {
            return doAs(CurrentUser.getUser(), new Callable<Void>() {

                public Void call() throws Exception {
                    return ProxyOozieClient.super.reRunBundle(jobId, coordScope, dateScope, refresh, noCleanup);
                }
            });
        } catch (Exception e) {
            throw new OozieClientException(OozieClientException.AUTHENTICATION, e);
        }
    }

    @Override
    public List<WorkflowJob> getJobsInfo(final String filter, final int start, final int len)
        throws OozieClientException {
        try {
            return doAs(CurrentUser.getUser(), new Callable<List<WorkflowJob>>() {

                public List<WorkflowJob> call() throws Exception {
                    return ProxyOozieClient.super.getJobsInfo(filter, start, len);
                }
            });
        } catch (Exception e) {
            throw new OozieClientException(OozieClientException.AUTHENTICATION, e);
        }
    }

    @Override
    public List<WorkflowJob> getJobsInfo(final String filter) throws OozieClientException {
        try {
            return doAs(CurrentUser.getUser(), new Callable<List<WorkflowJob>>() {

                public List<WorkflowJob> call() throws Exception {
                    return ProxyOozieClient.super.getJobsInfo(filter);
                }
            });
        } catch (Exception e) {
            throw new OozieClientException(OozieClientException.AUTHENTICATION, e);
        }
    }

    @Override
    public void getSlaInfo(final int start, final int len, final String filter) throws OozieClientException {
        try {
            doAs(CurrentUser.getUser(), new Callable<Object>() {

                public Object call() throws Exception {
                    ProxyOozieClient.super.getSlaInfo(start, len, filter);
                    return null;
                }
            });
        } catch (Exception e) {
            throw new OozieClientException(OozieClientException.AUTHENTICATION, e);
        }
    }

    @Override
    public String getJobId(final String externalId) throws OozieClientException {
        try {
            return doAs(CurrentUser.getUser(), new Callable<String>() {

                public String call() throws Exception {
                    return ProxyOozieClient.super.getJobId(externalId);
                }
            });
        } catch (Exception e) {
            throw new OozieClientException(OozieClientException.AUTHENTICATION, e);
        }
    }

    @Override
    public List<CoordinatorJob> getCoordJobsInfo(final String filter, final int start,
                                                 final int len) throws OozieClientException {
        try {
            return doAs(CurrentUser.getUser(), new Callable<List<CoordinatorJob>>() {

                public List<CoordinatorJob> call() throws Exception {
                    return ProxyOozieClient.super.getCoordJobsInfo(filter, start, len);
                }
            });
        } catch (Exception e) {
            throw new OozieClientException(OozieClientException.AUTHENTICATION, e);
        }
    }

    @Override
    public List<BundleJob> getBundleJobsInfo(final String filter, final int start,
                                             final int len) throws OozieClientException {
        try {
            return doAs(CurrentUser.getUser(), new Callable<List<BundleJob>>() {
                public List<BundleJob> call() throws Exception {
                    return ProxyOozieClient.super.getBundleJobsInfo(filter, start, len);
                }
            });
        } catch (Exception e) {
            throw new OozieClientException(OozieClientException.AUTHENTICATION, e);
        }
    }
}
