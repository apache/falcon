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

package org.apache.falcon.hadoop;

import org.apache.commons.lang.Validate;
import org.apache.falcon.FalconException;
import org.apache.falcon.security.CurrentUser;
import org.apache.falcon.security.SecurityUtil;
import org.apache.falcon.util.StartupProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;

/**
 * A factory implementation to dole out FileSystem handles based on the logged in user.
 */
public final class HadoopClientFactory {

    public static final String FS_DEFAULT_NAME_KEY = CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
    public static final String MR_JT_ADDRESS_KEY = "mapreduce.jobtracker.address";
    public static final String YARN_RM_ADDRESS_KEY = "yarn.resourcemanager.address";

    private static final HadoopClientFactory INSTANCE = new HadoopClientFactory();

    private HadoopClientFactory() {
    }

    public static HadoopClientFactory get() {
        return INSTANCE;
    }

    /**
     * This method is only used by Falcon internally to talk to the config store on HDFS.
     *
     * @param uri file system URI for config store.
     * @return FileSystem created with the provided proxyUser/group.
     * @throws org.apache.falcon.FalconException
     *          if the filesystem could not be created.
     */
    public FileSystem createFileSystem(final URI uri) throws FalconException {
        Validate.notNull(uri, "uri cannot be null");

        try {
            Configuration conf = new Configuration();
            if (UserGroupInformation.isSecurityEnabled()) {
                conf.set(SecurityUtil.NN_PRINCIPAL, StartupProperties.get().getProperty(SecurityUtil.NN_PRINCIPAL));
            }

            return createFileSystem(UserGroupInformation.getLoginUser(), uri, conf);
        } catch (IOException e) {
            throw new FalconException("Exception while getting FileSystem for: " + uri, e);
        }
    }

    public FileSystem createFileSystem(final Configuration conf)
        throws FalconException {
        Validate.notNull(conf, "configuration cannot be null");

        String nameNode = conf.get(FS_DEFAULT_NAME_KEY);
        try {
            return createFileSystem(UserGroupInformation.getLoginUser(), new URI(nameNode), conf);
        } catch (URISyntaxException e) {
            throw new FalconException("Exception while getting FileSystem for: " + nameNode, e);
        } catch (IOException e) {
            throw new FalconException("Exception while getting FileSystem for: " + nameNode, e);
        }
    }

    public FileSystem createFileSystem(final URI uri, final Configuration conf)
        throws FalconException {
        Validate.notNull(uri, "uri cannot be null");

        try {
            return createFileSystem(UserGroupInformation.getLoginUser(), uri, conf);
        } catch (IOException e) {
            throw new FalconException("Exception while getting FileSystem for: " + uri, e);
        }
    }

    /**
     * Return a FileSystem created with the authenticated proxy user for the specified conf.
     *
     * @param conf Configuration with all necessary information to create the FileSystem.
     * @return FileSystem created with the provided proxyUser/group.
     * @throws org.apache.falcon.FalconException
     *          if the filesystem could not be created.
     */
    public FileSystem createProxiedFileSystem(final Configuration conf)
        throws FalconException {
        Validate.notNull(conf, "configuration cannot be null");

        String nameNode = conf.get(FS_DEFAULT_NAME_KEY);
        try {
            return createFileSystem(CurrentUser.getProxyUgi(), new URI(nameNode), conf);
        } catch (URISyntaxException e) {
            throw new FalconException("Exception while getting FileSystem for proxy: "
                    + CurrentUser.getUser(), e);
        } catch (IOException e) {
            throw new FalconException("Exception while getting FileSystem for proxy: "
                    + CurrentUser.getUser(), e);
        }
    }

    /**
     * Return a FileSystem created with the provided user for the specified URI.
     *
     * @param ugi user group information
     * @param uri  file system URI.
     * @param conf Configuration with all necessary information to create the FileSystem.
     * @return FileSystem created with the provided user/group.
     * @throws org.apache.falcon.FalconException
     *          if the filesystem could not be created.
     */
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public FileSystem createFileSystem(UserGroupInformation ugi, final URI uri, final Configuration conf)
        throws FalconException {
        Validate.notNull(ugi, "ugi cannot be null");
        Validate.notNull(conf, "configuration cannot be null");

        String nameNode = uri.getAuthority();
        if (nameNode == null) {
            nameNode = conf.get(FS_DEFAULT_NAME_KEY);
            if (nameNode != null) {
                try {
                    new URI(nameNode).getAuthority();
                } catch (URISyntaxException ex) {
                    throw new FalconException("Exception while getting FileSystem", ex);
                }
            }
        }

        try {
            return ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
                public FileSystem run() throws Exception {
                    return FileSystem.get(uri, conf);
                }
            });
        } catch (InterruptedException ex) {
            throw new FalconException("Exception creating FileSystem:" + ex.getMessage(), ex);
        } catch (IOException ex) {
            throw new FalconException("Exception creating FileSystem:" + ex.getMessage(), ex);
        }
    }

    /**
     * This method validates if the execute url is able to reach the MR endpoint.
     *
     * @param executeUrl jt url or RM url
     * @throws IOException
     */
    public static void validateJobClient(String executeUrl) throws IOException {
        final JobConf jobConf = new JobConf();
        jobConf.set(MR_JT_ADDRESS_KEY, executeUrl);
        jobConf.set(YARN_RM_ADDRESS_KEY, executeUrl);

        UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
        try {
            JobClient jobClient = loginUser.doAs(new PrivilegedExceptionAction<JobClient>() {
                public JobClient run() throws Exception {
                    return new JobClient(jobConf);
                }
            });

            jobClient.getClusterStatus().getMapTasks();
        } catch (InterruptedException e) {
            throw new IOException("Exception creating job client:" + e.getMessage(), e);
        }
    }
}
