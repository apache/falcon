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

import org.apache.oozie.BaseEngineException;
import org.apache.oozie.BundleEngine;
import org.apache.oozie.BundleEngineException;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.BundleJobInfo;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.util.XConfiguration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;


/**
 * Client API to submit and manage Oozie bundle jobs against an Oozie
 * intance.
 */
public class LocalOozieClientBundle extends OozieClient {

    private final BundleEngine bundleEngine;

    /**
     * Create a bundle client for Oozie local use.
     * <p/>
     *
     * @param bundleEngine the engine instance to use.
     */
    public LocalOozieClientBundle(BundleEngine bundleEngine) {
        this.bundleEngine = bundleEngine;
    }

    /**
     * Return the Oozie URL of the bundle client instance.
     * <p/>
     * This URL is the base URL for the Oozie system, without protocol
     * versioning.
     *
     * @return the Oozie URL of the bundle client instance.
     */
    @Override
    public String getOozieUrl() {
        return "localoozie";
    }

    /**
     * Return the Oozie URL used by the client and server for WS communications.
     * <p/>
     * This URL is the original URL plus the versioning element path.
     *
     * @return the Oozie URL used by the client and server for communication.
     * @throws OozieClientException thrown in the client
     *                              and the server are not protocol compatible.
     */
    @Override
    public String getProtocolUrl() throws OozieClientException {
        return "localoozie";
    }

    /**
     * Validate that the Oozie client and server instances are protocol
     * compatible.
     *
     * @throws OozieClientException thrown in the client
     *                              and the server are not protocol compatible.
     */
    @Override
    public synchronized void validateWSVersion() throws OozieClientException {
    }

    /**
     * Create an empty configuration with just the {@link #USER_NAME} set to the
     * JVM user name and the {@link #GROUP_NAME} set to 'other'.
     *
     * @return an empty configuration.
     */
    @Override
    public Properties createConfiguration() {
        Properties conf = new Properties();
        if (bundleEngine != null) {
            conf.setProperty(USER_NAME, bundleEngine.getUser());
        }
        return conf;
    }

    /**
     * Set a HTTP header to be used in the WS requests by the bundle
     * instance.
     *
     * @param name  header name.
     * @param value header value.
     */
    @Override
    public void setHeader(String name, String value) {
    }

    /**
     * Get the value of a set HTTP header from the bundle instance.
     *
     * @param name header name.
     * @return header value, <code>null</code> if not set.
     */
    @Override
    public String getHeader(String name) {
        return null;
    }

    /**
     * Remove a HTTP header from the bundle client instance.
     *
     * @param name header name.
     */
    @Override
    public void removeHeader(String name) {
    }

    /**
     * Return an iterator with all the header names set in the bundle
     * instance.
     *
     * @return header names.
     */
    @Override
    @SuppressWarnings("unchecked")
    public Iterator<String> getHeaderNames() {
        return Collections.EMPTY_SET.iterator();
    }

    /**
     * Submit a bundle job.
     *
     * @param conf job configuration.
     * @return the job Id.
     * @throws OozieClientException thrown if the job
     *                              could not be submitted.
     */
    @Override
    public String submit(Properties conf) throws OozieClientException {
        try {
            return bundleEngine.submitJob(new XConfiguration(conf), false);
        } catch (BundleEngineException ex) {
            throw new OozieClientException(ex.getErrorCode().toString(), ex);
        }
    }

    /**
     * Start a bundle job.
     *
     * @param jobId job Id.
     * @throws OozieClientException thrown if the job
     *                              could not be started.
     */
    @Override
    @Deprecated
    public void start(String jobId) throws OozieClientException {
        try {
            bundleEngine.start(jobId);
        } catch (BundleEngineException ex) {
            throw new OozieClientException(ex.getErrorCode().toString(), ex);
        } catch (BaseEngineException bex) {
            throw new OozieClientException(bex.getErrorCode().toString(), bex);
        }
    }

    /**
     * Submit and start a bundle job.
     *
     * @param conf job configuration.
     * @return the job Id.
     * @throws OozieClientException thrown if the job
     *                              could not be submitted.
     */
    @Override
    public String run(Properties conf) throws OozieClientException {
        try {
            return bundleEngine.submitJob(new XConfiguration(conf), true);
        } catch (BundleEngineException ex) {
            throw new OozieClientException(ex.getErrorCode().toString(), ex);
        }
    }

    /**
     * Rerun a workflow job.
     *
     * @param jobId job Id to rerun.
     * @param conf  configuration information for the rerun.
     * @throws OozieClientException thrown if the job
     *                              could not be started.
     */
    @Override
    @Deprecated
    public void reRun(String jobId, Properties conf) throws OozieClientException {
        throw new OozieClientException(ErrorCode.E0301.toString(), "no-op");
    }

    /**
     * Rerun bundle coordinators.
     *
     * @param jobId      bundle jobId
     * @param coordScope rerun scope for coordinator jobs
     * @param dateScope  rerun scope for date
     * @param refresh    true if -refresh is given in command option
     * @param noCleanup  true if -nocleanup is given in command option
     * @throws OozieClientException
     */
    @Override
    public Void reRunBundle(String jobId, String coordScope, String dateScope, boolean refresh,
                            boolean noCleanup) throws OozieClientException {
        try {
            new BundleEngine().reRun(jobId, coordScope, dateScope, refresh, noCleanup);
        } catch (BaseEngineException e) {
            throw new OozieClientException(e.getErrorCode().toString(), e);
        }
        return null;
    }

    /**
     * Suspend a bundle job.
     *
     * @param jobId job Id.
     * @throws OozieClientException thrown if the job
     *                              could not be suspended.
     */
    @Override
    public void suspend(String jobId) throws OozieClientException {
        try {
            bundleEngine.suspend(jobId);
        } catch (BundleEngineException ex) {
            throw new OozieClientException(ex.getErrorCode().toString(), ex);
        }
    }

    /**
     * Resume a bundle job.
     *
     * @param jobId job Id.
     * @throws OozieClientException thrown if the job
     *                              could not be resume.
     */
    @Override
    public void resume(String jobId) throws OozieClientException {
        try {
            bundleEngine.resume(jobId);
        } catch (BundleEngineException ex) {
            throw new OozieClientException(ex.getErrorCode().toString(), ex);
        }
    }

    /**
     * Kill a bundle job.
     *
     * @param jobId job Id.
     * @throws OozieClientException thrown if the job
     *                              could not be killed.
     */
    @Override
    public void kill(String jobId) throws OozieClientException {
        try {
            bundleEngine.kill(jobId);
        } catch (BundleEngineException ex) {
            throw new OozieClientException(ex.getErrorCode().toString(), ex);
        }
    }

    /**
     * Get the info of a workflow job.
     *
     * @param jobId job Id.
     * @return the job info.
     * @throws OozieClientException thrown if the job
     *                              info could not be retrieved.
     */
    @Override
    @Deprecated
    public WorkflowJob getJobInfo(String jobId) throws OozieClientException {
        throw new OozieClientException(ErrorCode.E0301.toString(), "no-op");
    }

    /**
     * Get the info of a bundle job.
     *
     * @param jobId job Id.
     * @return the job info.
     * @throws OozieClientException thrown if the job
     *                              info could not be retrieved.
     */
    @Override
    public BundleJob getBundleJobInfo(String jobId) throws OozieClientException {
        try {
            return bundleEngine.getBundleJob(jobId);
        } catch (BundleEngineException ex) {
            throw new OozieClientException(ex.getErrorCode().toString(), ex);
        } catch (BaseEngineException bex) {
            throw new OozieClientException(bex.getErrorCode().toString(), bex);
        }
    }

    /**
     * Return the info of the workflow jobs that match the filter.
     *
     * @param filter job filter. Refer to the {@link OozieClient} for the filter
     *               syntax.
     * @param start  jobs offset, base 1.
     * @param len    number of jobs to return.
     * @return a list with the workflow jobs info, without node details.
     * @throws OozieClientException thrown if the jobs info could not be
     *                              retrieved.
     */
    @Override
    @Deprecated
    public List<WorkflowJob> getJobsInfo(String filter, int start, int len) throws OozieClientException {
        throw new OozieClientException(ErrorCode.E0301.toString(), "no-op");
    }

    /**
     * Return the info of the bundle jobs that match the filter.
     *
     * @param filter job filter. Refer to the {@link OozieClient} for the filter
     *               syntax.
     * @param start  jobs offset, base 1.
     * @param len    number of jobs to return.
     * @return a list with the coordinator jobs info
     * @throws OozieClientException thrown if the jobs info could not be
     *                              retrieved.
     */
    @Override
    public List<BundleJob> getBundleJobsInfo(String filter, int start, int len) throws OozieClientException {
        try {
            start = (start < 1) ? 1 : start; // taken from oozie API
            len = (len < 1) ? 50 : len;
            BundleJobInfo info = bundleEngine.getBundleJobs(filter, start, len);
            List<BundleJob> jobs = new ArrayList<BundleJob>();
            List<BundleJobBean> jobBeans = info.getBundleJobs();
            for (BundleJobBean jobBean : jobBeans) {
                jobs.add(jobBean);
            }
            return jobs;

        } catch (BundleEngineException ex) {
            throw new OozieClientException(ex.getErrorCode().toString(), ex);
        }
    }

    /**
     * Return the info of the workflow jobs that match the filter.
     * <p/>
     * It returns the first 100 jobs that match the filter.
     *
     * @param filter job filter. Refer to the {@link org.apache.oozie.LocalOozieClient} for the
     *               filter syntax.
     * @return a list with the workflow jobs info, without node details.
     * @throws OozieClientException thrown if the jobs
     *                              info could not be retrieved.
     */
    @Override
    @Deprecated
    public List<WorkflowJob> getJobsInfo(String filter) throws OozieClientException {
        throw new OozieClientException(ErrorCode.E0301.toString(), "no-op");
    }

}
