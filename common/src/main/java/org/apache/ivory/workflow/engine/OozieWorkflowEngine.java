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

package org.apache.ivory.workflow.engine;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethodBase;
import org.apache.commons.httpclient.methods.ByteArrayRequestEntity;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.RequestEntity;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.ivory.IvoryException;
import org.apache.ivory.IvoryRuntimException;
import org.apache.ivory.util.StartupProperties;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 * Workflow engine which uses oozies APIs
 * 
 */
public class OozieWorkflowEngine implements WorkflowEngine {

    private static final Logger LOG = Logger.getLogger(OozieWorkflowEngine.class);

    private static final String OOZIE_MIME_TYPE = "application/xml;charset=UTF-8";
    private static final String JOBS_RESOURCE = "jobs";
    private static final String JOB_RESOURCE = "job";

    private static final String URI_SEPERATOR = "/";


    @Override
    public String schedule(Path path) throws IvoryException {
        OozieClient client = new OozieClient();
        Properties conf = client.createConfiguration();
        conf.setProperty(OozieClient.COORDINATOR_APP_PATH, path.toString());
        return client.schedule(conf);
    }

    @Override
    public String dryRun(Path path) throws IvoryException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String suspend(String entityName) throws IvoryException {
        //TODO migrate to oozie client
        // GET /oozie/v1/jobs?filter=user%3Dbansalm&offset=1&len=50

        String oozieUrl = StartupProperties.get().getProperty("oozie.url");
        OozieJobFilter oozieJobFilter = new OozieJobFilter(OozieJobFilter.JobTypes.coord);

        oozieJobFilter.names.add(entityName);
        oozieJobFilter.statuss.add(OozieJobFilter.JobStatus.PREP);
        oozieJobFilter.statuss.add(OozieJobFilter.JobStatus.RUNNING);
        oozieJobFilter.statuss.add(OozieJobFilter.JobStatus.PAUSED);

        GetMethod getMethod = new GetMethod(oozieUrl + URI_SEPERATOR + JOBS_RESOURCE + "?" + oozieJobFilter);
        String jobsResponse = executeHTTPmethod(getMethod);

        LOG.info(jobsResponse);

        String coordinatorId = getCoordinatorId(jobsResponse, entityName);
        // PUT /oozie/v1/job/job-3?action=suspend
        PutMethod putMethod = new PutMethod(oozieUrl + URI_SEPERATOR + JOB_RESOURCE + URI_SEPERATOR + coordinatorId
                + "?action=suspend");

        String suspendResponse = executeHTTPmethod(putMethod);

        return suspendResponse;
    }

    @Override
    public String resume(String entityName) throws IvoryException {
        //TODO migrate to oozie client
        // GET /oozie/v1/jobs?filter=user%3Dbansalm&offset=1&len=50

        String oozieUrl = StartupProperties.get().getProperty("oozie.url");
        OozieJobFilter oozieJobFilter = new OozieJobFilter(OozieJobFilter.JobTypes.coord);

        oozieJobFilter.names.add(entityName);
        oozieJobFilter.statuss.add(OozieJobFilter.JobStatus.PREPSUSPENDED);
        oozieJobFilter.statuss.add(OozieJobFilter.JobStatus.SUSPENDED);

        GetMethod getMethod = new GetMethod(oozieUrl + URI_SEPERATOR + JOBS_RESOURCE + "?" + oozieJobFilter);
        String jobsResponse = executeHTTPmethod(getMethod);

        LOG.info(jobsResponse);

        String coordinatorId = getCoordinatorId(jobsResponse, entityName);
        // PUT /oozie/v1/job/job-3?action=resume
        PutMethod putMethod = new PutMethod(oozieUrl + URI_SEPERATOR + JOB_RESOURCE + URI_SEPERATOR + coordinatorId
                + "?action=resume");

        String suspendResponse = executeHTTPmethod(putMethod);

        return suspendResponse;
    }

    @Override
    public String delete(String entityName) throws IvoryException {
        //TODO migrate to oozie client
        // GET /oozie/v1/jobs?filter=user%3Dbansalm&offset=1&len=50

        String oozieUrl = StartupProperties.get().getProperty("oozie.url");
        OozieJobFilter oozieJobFilter = new OozieJobFilter(OozieJobFilter.JobTypes.coord);

        oozieJobFilter.names.add(entityName);
        oozieJobFilter.statuss.add(OozieJobFilter.JobStatus.PREP);
        oozieJobFilter.statuss.add(OozieJobFilter.JobStatus.RUNNING);
        oozieJobFilter.statuss.add(OozieJobFilter.JobStatus.PREPSUSPENDED);
        oozieJobFilter.statuss.add(OozieJobFilter.JobStatus.SUSPENDED);
        oozieJobFilter.statuss.add(OozieJobFilter.JobStatus.PREPPAUSED);
        oozieJobFilter.statuss.add(OozieJobFilter.JobStatus.PAUSED);

        GetMethod getMethod = new GetMethod(oozieUrl + URI_SEPERATOR + JOBS_RESOURCE + "?" + oozieJobFilter);
        String jobsResponse = executeHTTPmethod(getMethod);

        LOG.info(jobsResponse);

        String coordinatorId = getCoordinatorId(jobsResponse, entityName);
        // PUT /oozie/v1/job/job-3?action=kill
        PutMethod putMethod = new PutMethod(oozieUrl + URI_SEPERATOR + JOB_RESOURCE + URI_SEPERATOR + coordinatorId + "?action=kill");

        String killReponse = executeHTTPmethod(putMethod);

        return killReponse;

    }

    private String getCoordinatorId(String response, String entityName) {
        try {
            JSONObject jobsResponse = new JSONObject(response);
            Integer total = (Integer) jobsResponse.get("total");
            if (total.intValue() == 0) {
                LOG.error("No live process found with name: " + entityName);
                throw new IvoryRuntimException("No live process found with name: " + entityName);
            }
            if (total.intValue() > 1) {
                LOG.warn("Found " + total + " running coordinators with name: " + entityName);
            }
            JSONArray coordinators = jobsResponse.getJSONArray("coordinatorjobs");
            return (String) coordinators.getJSONObject(0).get("coordJobId");
        } catch (JSONException e) {
            LOG.error(e);
            throw new IvoryRuntimException(e);
        }

    }

    private RequestEntity getConfigurationRequestEntity(Configuration configuration) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(2048);
        try {
            configuration.writeXml(out);
            out.close();
        } catch (IOException e) {
            LOG.error(e);
            throw new IvoryRuntimException(e);
        }

        RequestEntity reqEntity = new ByteArrayRequestEntity(out.toByteArray(), OOZIE_MIME_TYPE);
        return reqEntity;

    }

    private static class OozieJobFilter {
        enum JobTypes {
            wf, coord, bundle
        };

        enum JobStatus {
            PREP, RUNNING, PREPSUSPENDED, SUSPENDED, PREPPAUSED, PAUSED, SUCCEEDED, DONWITHERROR, KILLED, FAILED
        }

        OozieJobFilter(JobTypes jobType) {
            this.jobType = jobType;
        }

        JobTypes jobType;
        List<String> names = new ArrayList<String>();
        List<String> users = new ArrayList<String>();
        List<String> groups = new ArrayList<String>();
        List<JobStatus> statuss = new ArrayList<JobStatus>();

        @Override
        public String toString() {
            StringBuffer urlFilter = new StringBuffer();
            for (String name : names) {
                urlFilter.append("name=");
                urlFilter.append(name + ";");
            }
            for (String user : users) {
                urlFilter.append("user=");
                urlFilter.append(user + ";");
            }
            for (String group : groups) {
                urlFilter.append("group=");
                urlFilter.append(group + ";");
            }
            for (JobStatus status : statuss) {
                urlFilter.append("status=");
                urlFilter.append(status.name() + ";");
            }

            try {
                return "jobtype=" + this.jobType.name() + "&filter=" + URLEncoder.encode(urlFilter.toString(), "UTF-8");
            } catch (UnsupportedEncodingException e) {
                return e.getMessage();
            }

        }
    }

    private String executeHTTPmethod(HttpMethodBase method) throws IvoryException {
        HttpClient client = new HttpClient();
        try {
            client.executeMethod(method);
            int statusCode = method.getStatusCode();
            if ((statusCode == HttpServletResponse.SC_OK) || (statusCode == HttpServletResponse.SC_CREATED)) {
                return method.getResponseBodyAsString();
            } else {
                throw new IvoryException(method.getResponseHeader("oozie-error-message").getValue());
            }
        } catch (HttpException e) {
            LOG.error(e.getMessage());
            throw new IvoryRuntimException(e);
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw new IvoryRuntimException(e);
        }

    }

}
