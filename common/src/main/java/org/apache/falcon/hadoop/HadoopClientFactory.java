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
import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.security.AuthenticationInitializationService;
import org.apache.falcon.security.CurrentUser;
import org.apache.falcon.security.SecurityUtil;
import org.apache.falcon.util.StartupProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A factory implementation to dole out FileSystem handles based on the logged in user.
 */
public final class HadoopClientFactory {

    private static final Logger LOG = LoggerFactory.getLogger(HadoopClientFactory.class);

    public static final String FS_DEFAULT_NAME_KEY = CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
    public static final String MR_JT_ADDRESS_KEY = "mapreduce.jobtracker.address";
    public static final String YARN_RM_ADDRESS_KEY = "yarn.resourcemanager.address";

    public static final FsPermission READ_EXECUTE_PERMISSION =
            new FsPermission(FsAction.ALL, FsAction.READ_EXECUTE, FsAction.READ_EXECUTE);
    public static final FsPermission ALL_PERMISSION =
            new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);

    private static final HadoopClientFactory INSTANCE = new HadoopClientFactory();
    public static final FsPermission READ_ONLY_PERMISSION =
            new FsPermission(FsAction.READ, FsAction.READ, FsAction.READ);

    private ConcurrentMap<String, UserGroupInformation> cache = new ConcurrentHashMap<String, UserGroupInformation>();

    private HadoopClientFactory() {
    }

    public static HadoopClientFactory get() {
        return INSTANCE;
    }

    public UserGroupInformation getProxyUser(String user) throws IOException {
        // Due to the token refresh that happens in AuthenticationInitializationService, need to ensure the new
        // credentials are picked up.
        UserGroupInformation loginUser = AuthenticationInitializationService.getLoginUser();
        if (loginUser == null) {
            loginUser = UserGroupInformation.getLoginUser();
        }
        if (! cache.containsKey(user) || loginUser != cache.get(user).getRealUser()) {
                cache.put(user, UserGroupInformation.createProxyUser(user, loginUser));
                LOG.debug("Adding {} to proxy user cache with real user, {}", user, cache.get(user).getRealUser());
        }
        return cache.get(user);
    }


    /**
     * This method is only used by Falcon internally to talk to the config store on HDFS.
     *
     * @param uri file system URI for config store.
     * @return FileSystem created with the provided proxyUser/group.
     * @throws org.apache.falcon.FalconException
     *          if the filesystem could not be created.
     */
    public FileSystem createFalconFileSystem(final URI uri) throws FalconException {
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

    /**
     * This method is only used by Falcon internally to talk to the config store on HDFS.
     *
     * @param conf configuration.
     * @return FileSystem created with the provided proxyUser/group.
     * @throws org.apache.falcon.FalconException
     *          if the filesystem could not be created.
     */
    public FileSystem createFalconFileSystem(final Configuration conf)
        throws FalconException {
        Validate.notNull(conf, "configuration cannot be null");

        String nameNode = getNameNode(conf);
        try {
            return createFileSystem(UserGroupInformation.getLoginUser(), new URI(nameNode), conf);
        } catch (URISyntaxException e) {
            throw new FalconException("Exception while getting FileSystem for: " + nameNode, e);
        } catch (IOException e) {
            throw new FalconException("Exception while getting FileSystem for: " + nameNode, e);
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

        String nameNode = getNameNode(conf);
        try {
            return createProxiedFileSystem(new URI(nameNode), conf);
        } catch (URISyntaxException e) {
            throw new FalconException("Exception while getting FileSystem for: " + nameNode, e);
        }
    }

    /**
     * Return a DistributedFileSystem created with the authenticated proxy user for the specified conf.
     *
     * @param conf Configuration with all necessary information to create the FileSystem.
     * @return DistributedFileSystem created with the provided proxyUser/group.
     * @throws org.apache.falcon.FalconException
     *          if the filesystem could not be created.
     */
    public DistributedFileSystem createDistributedProxiedFileSystem(final Configuration conf) throws FalconException {
        Validate.notNull(conf, "configuration cannot be null");

        String nameNode = getNameNode(conf);
        try {
            UserGroupInformation ugi = CurrentUser.isAuthenticated()
                    ? getProxyUser(CurrentUser.getUser()) : UserGroupInformation.getCurrentUser();
            return createDistributedFileSystem(ugi, new URI(nameNode), conf);
        } catch (Exception e) {
            throw new FalconException("Exception while getting Distributed FileSystem for: " + nameNode, e);
        }
    }

    private static String getNameNode(Configuration conf) {
        return conf.get(FS_DEFAULT_NAME_KEY);
    }

    /**
     * This method is called from with in a workflow execution context.
     *
     * @param uri uri
     * @return file system handle
     * @throws FalconException
     */
    public FileSystem createProxiedFileSystem(final URI uri) throws FalconException {
        return createProxiedFileSystem(uri, new Configuration());
    }

    public FileSystem createProxiedFileSystem(final URI uri,
                                              final Configuration conf) throws FalconException {
        Validate.notNull(uri, "uri cannot be null");
        try {
            UserGroupInformation ugi = CurrentUser.isAuthenticated()
                    ? getProxyUser(CurrentUser.getUser()) : UserGroupInformation.getCurrentUser();
            return createFileSystem(ugi, uri, conf);
        } catch (IOException e) {
            throw new FalconException("Exception while getting Proxied FileSystem for: " + uri, e);
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
    public FileSystem createFileSystem(UserGroupInformation ugi, final URI uri,
                                       final Configuration conf) throws FalconException {
        validateInputs(ugi, uri, conf);

        try {
            // prevent falcon impersonating falcon, no need to use doas
            final String proxyUserName = ugi.getShortUserName();
            if (proxyUserName.equals(UserGroupInformation.getLoginUser().getShortUserName())) {
                LOG.trace("Creating FS for the login user {}, impersonation not required",
                    proxyUserName);
                return FileSystem.get(uri, conf);
            }

            LOG.trace("Creating FS impersonating user {} using auth method {}, real user {}",
                    ugi.getShortUserName(), ugi.getAuthenticationMethod(), ugi.getRealUser());
            return ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
                public FileSystem run() throws Exception {
                    return FileSystem.get(uri, conf);
                }
            });
        } catch (InterruptedException | IOException ex) {
            throw new FalconException("Exception creating FileSystem:" + ex.getMessage(), ex);
        }
    }

    /**
     * Return a DistributedFileSystem created with the provided user for the specified URI.
     *
     * @param ugi user group information
     * @param uri  file system URI.
     * @param conf Configuration with all necessary information to create the FileSystem.
     * @return DistributedFileSystem created with the provided user/group.
     * @throws org.apache.falcon.FalconException
     *          if the filesystem could not be created.
     */
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public DistributedFileSystem createDistributedFileSystem(UserGroupInformation ugi, final URI uri,
                                                             final Configuration conf) throws FalconException {
        validateInputs(ugi, uri, conf);
        FileSystem returnFs;
        try {
            // prevent falcon impersonating falcon, no need to use doas
            final String proxyUserName = ugi.getShortUserName();
            if (proxyUserName.equals(UserGroupInformation.getLoginUser().getShortUserName())) {
                LOG.trace("Creating Distributed FS for the login user {}, impersonation not required",
                        proxyUserName);
                returnFs = DistributedFileSystem.get(uri, conf);
            } else {
                LOG.trace("Creating FS impersonating user {} using auth method {}, real user {}",
                        ugi.getShortUserName(), ugi.getAuthenticationMethod(), ugi.getRealUser());
                returnFs = ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
                    public FileSystem run() throws Exception {
                        return DistributedFileSystem.get(uri, conf);
                    }
                });
            }

            return (DistributedFileSystem) returnFs;
        } catch (InterruptedException | IOException ex) {
            throw new FalconException("Exception creating FileSystem:" + ex.getMessage(), ex);
        }
    }

    private void validateInputs(UserGroupInformation ugi, final URI uri,
                                final Configuration conf) throws FalconException {
        Validate.notNull(ugi, "ugi cannot be null");
        Validate.notNull(conf, "configuration cannot be null");
        validateNameNode(uri, conf);
    }

    /**
     * This method validates if the execute url is able to reach the MR endpoint.
     *
     * @param executeUrl jt url or RM url
     * @throws IOException
     */
    public void validateJobClient(String executeUrl, String rmPrincipal) throws IOException {
        final JobConf jobConf = new JobConf();
        jobConf.set(MR_JT_ADDRESS_KEY, executeUrl);
        jobConf.set(YARN_RM_ADDRESS_KEY, executeUrl);
        /**
         * It is possible that the RM/JT principal can be different between clusters,
         * for example, the cluster is using a different KDC with cross-domain trust
         * with the Falcon KDC.   in that case, we want to allow the user to provide
         * the RM principal similar to NN principal.
         */
        if (UserGroupInformation.isSecurityEnabled() && StringUtils.isNotEmpty(rmPrincipal)) {
            jobConf.set(SecurityUtil.RM_PRINCIPAL, rmPrincipal);
        }
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

    public static FsPermission getDirDefaultPermission(Configuration conf) {
        return getDirDefault().applyUMask(FsPermission.getUMask(conf));
    }

    public static FsPermission getFileDefaultPermission(Configuration conf) {
        return getFileDefault().applyUMask(FsPermission.getUMask(conf));
    }

    public static FsPermission getDirDefault() {
        return new FsPermission((short)511);
    }

    public static FsPermission getFileDefault() {
        return new FsPermission((short)438);
    }

    public static void mkdirsWithDefaultPerms(FileSystem fs, Path path) throws IOException {
        mkdirs(fs, path, getDirDefaultPermission(fs.getConf()));
    }

    public static void mkdirs(FileSystem fs, Path path,
                              FsPermission permission) throws IOException {
        if (!FileSystem.mkdirs(fs, path, permission)) {
            throw new IOException("mkdir failed for " + path);
        }
    }

    private void validateNameNode(URI uri, Configuration conf) throws FalconException {
        String nameNode = uri.getAuthority();
        if (nameNode == null) {
            nameNode = getNameNode(conf);
            if (nameNode != null) {
                try {
                    new URI(nameNode).getAuthority();
                } catch (URISyntaxException ex) {
                    throw new FalconException("Exception while getting FileSystem", ex);
                }
            }
        }
    }
}
