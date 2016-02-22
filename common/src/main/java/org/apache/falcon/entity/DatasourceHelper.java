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

package org.apache.falcon.entity;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.datasource.Credential;
import org.apache.falcon.entity.v0.datasource.Credentialtype;
import org.apache.falcon.entity.v0.datasource.Datasource;
import org.apache.falcon.entity.v0.datasource.DatasourceType;
import org.apache.falcon.entity.v0.datasource.Interface;
import org.apache.falcon.entity.v0.datasource.Interfaces;
import org.apache.falcon.entity.v0.datasource.Interfacetype;
import org.apache.falcon.entity.v0.datasource.PasswordAliasType;
import org.apache.falcon.security.CurrentUser;
import org.apache.hadoop.conf.Configuration;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.security.CredentialProviderHelper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URI;
import java.security.PrivilegedExceptionAction;

/**
 * DataSource entity helper methods.
 */

public final class DatasourceHelper {

    public static final String HADOOP_CREDENTIAL_PROVIDER_FILEPATH = "hadoop.security.credential.provider.path";

    private static final Logger LOG = LoggerFactory.getLogger(DatasourceHelper.class);

    private static final ConfigurationStore STORE = ConfigurationStore.get();

    public static DatasourceType getDatasourceType(String datasourceName) throws FalconException {
        return getDatasource(datasourceName).getType();
    }

    private DatasourceHelper() {}

    public static Datasource getDatasource(String datasourceName) throws FalconException {
        return STORE.get(EntityType.DATASOURCE, datasourceName);
    }

    public static String getReadOnlyEndpoint(Datasource datasource) {
        return getInterface(datasource, Interfacetype.READONLY);
    }

    public static String getWriteEndpoint(Datasource datasource) {
        return getInterface(datasource, Interfacetype.WRITE);
    }

    /**
     * Returns user name and password pair as it is specified in the XML. If the credential type is
     * password-file, the path name is returned.
     *
     * @param db
     * @return Credential
     * @throws FalconException
     */

    public static Credential getReadPasswordInfo(Datasource db) throws FalconException {
        for (Interface ifs : db.getInterfaces().getInterfaces()) {
            if ((ifs.getType() == Interfacetype.READONLY) && (ifs.getCredential() != null)) {
                return ifs.getCredential();
            }
        }
        return getDefaultPasswordInfo(db.getInterfaces());
    }

    public static Credential getWritePasswordInfo(Datasource db) throws FalconException {
        for (Interface ifs : db.getInterfaces().getInterfaces()) {
            if ((ifs.getType() == Interfacetype.WRITE) && (ifs.getCredential() != null)) {
                return ifs.getCredential();
            }
        }
        return getDefaultPasswordInfo(db.getInterfaces());
    }

    /**
     * Returns user name and actual password pair. If the credential type is password-file, then the
     * password is read from the HDFS file. If the credential type is password-text, the clear text
     * password is returned.
     *
     * @param db
     * @return
     * @throws FalconException
     */
    public static java.util.Properties fetchReadPasswordInfo(Datasource db) throws FalconException {
        Credential cred = getReadPasswordInfo(db);
        return fetchPasswordInfo(cred);
    }

    public static java.util.Properties fetchWritePasswordInfo(Datasource db) throws FalconException {
        Credential cred = getWritePasswordInfo(db);
        return fetchPasswordInfo(cred);
    }

    public static java.util.Properties fetchPasswordInfo(Credential cred) throws FalconException {
        java.util.Properties p = new java.util.Properties();
        p.put("user", cred.getUserName());
        if (cred.getType() == Credentialtype.PASSWORD_TEXT) {
            p.put("password", cred.getPasswordText());
        } else if (cred.getType() == Credentialtype.PASSWORD_FILE) {
            String actualPasswd = fetchPasswordInfoFromFile(cred.getPasswordFile());
            p.put("password", actualPasswd);
        } else if (cred.getType() == Credentialtype.PASSWORD_ALIAS) {
            String actualPasswd = fetchPasswordInfoFromCredentialStore(cred.getPasswordAlias());
            p.put("password", actualPasswd);
        }
        return p;
    }

    public static String buildJceksProviderPath(URI credURI) {
        StringBuilder sb = new StringBuilder();
        final String credProviderPath = sb.append("jceks:").append("//")
                .append(credURI.getScheme()).append("@")
                .append(credURI.getHost())
                .append(credURI.getPath()).toString();
        return credProviderPath;
    }

    /**
     * Return the Interface endpoint for the interface type specified in the argument.
     *
     * @param db
     * @param type - can be read-only or write
     * @return
     */
    private static String getInterface(Datasource db, Interfacetype type) {
        for(Interface ifs : db.getInterfaces().getInterfaces()) {
            if (ifs.getType() == type) {
                return ifs.getEndpoint();
            }
        }
        return null;
    }

    private static Credential getDefaultPasswordInfo(Interfaces ifs) throws FalconException {

        if (ifs.getCredential() != null) {
            return ifs.getCredential();
        } else {
            throw new FalconException("Missing Interfaces default credential");
        }
    }

    private static String fetchPasswordInfoFromCredentialStore(final PasswordAliasType c) throws FalconException {
        try {
            final String credPath = c.getProviderPath();
            final URI credURI = new URI(credPath);
            if (StringUtils.isBlank(credURI.getScheme())
                || StringUtils.isBlank(credURI.getHost())
                || StringUtils.isBlank(credURI.getPath())) {
                throw new FalconException("Password alias jceks provider HDFS path is incorrect.");
            }
            final String alias = c.getAlias();
            if (StringUtils.isBlank(alias)) {
                throw new FalconException("Password alias is empty.");
            }

            final String credProviderPath = buildJceksProviderPath(credURI);
            LOG.info("Credential provider HDFS path : " + credProviderPath);

            if (CredentialProviderHelper.isProviderAvailable()) {
                UserGroupInformation ugi = CurrentUser.getProxyUGI();
                String password = ugi.doAs(new PrivilegedExceptionAction<String>() {
                    public String run() throws Exception {
                        final Configuration conf = new Configuration();
                        conf.set(HadoopClientFactory.FS_DEFAULT_NAME_KEY, credPath);
                        conf.set(CredentialProviderHelper.CREDENTIAL_PROVIDER_PATH, credProviderPath);
                        FileSystem fs = FileSystem.get(credURI, conf);
                        if (!fs.exists(new Path(credPath))) {
                            String msg = String.format("Credential provider hdfs path [%s] does not "
                                   + "exist or access denied!", credPath);
                            LOG.error(msg);
                            throw new FalconException(msg);
                        }
                        return CredentialProviderHelper.resolveAlias(conf, alias);
                    }
                });
                return password;
            } else {
                throw new FalconException("Credential Provider is not initialized");
            }
        } catch (Exception ioe) {
            String msg = "Exception while trying to fetch credential alias";
            LOG.error(msg, ioe);
            throw new FalconException(msg, ioe);
        }
    }
    private static String fetchPasswordInfoFromFile(String passwordFilePath) throws FalconException {
        try {
            Path path = new Path(passwordFilePath);
            FileSystem fs = HadoopClientFactory.get().createProxiedFileSystem(path.toUri());
            if (!fs.exists(path)) {
                throw new IOException("The password file does not exist! "
                        + passwordFilePath);
            }

            if (!fs.isFile(path)) {
                throw new IOException("The password file cannot be a directory! "
                        + passwordFilePath);
            }

            InputStream is = fs.open(path);
            StringWriter writer = new StringWriter();
            try {
                IOUtils.copy(is, writer);
                return writer.toString();
            } finally {
                IOUtils.closeQuietly(is);
                IOUtils.closeQuietly(writer);
                fs.close();
            }
        } catch (IOException ioe) {
            LOG.error("Error reading password file from HDFS : " + ioe);
            throw new FalconException(ioe);
        }
    }
}
