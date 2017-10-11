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
import org.apache.falcon.entity.v0.datasource.Driver;
import org.apache.falcon.entity.v0.datasource.Interface;
import org.apache.falcon.entity.v0.datasource.Interfaces;
import org.apache.falcon.entity.v0.datasource.Interfacetype;
import org.apache.falcon.entity.v0.datasource.PasswordAliasType;
import org.apache.falcon.entity.v0.datasource.Property;
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
import java.util.HashMap;
import java.util.Map;

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
        return getInterfaceEndpoint(datasource, Interfacetype.READONLY);
    }

    public static String getWriteEndpoint(Datasource datasource) {
        return getInterfaceEndpoint(datasource, Interfacetype.WRITE);
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
     * checks if two datasource interfaces are same.
     *
     * @param oldEntity old datasource entity
     * @param newEntity new datasource entity
     * @param ifacetype type of interface
     * @return true if same else false
     */
    public static boolean isSameInterface(Datasource oldEntity, Datasource newEntity, Interfacetype ifacetype) {
        LOG.debug("Verifying if Interfaces match for Datasource {} : Old - {}, New - {}", oldEntity, newEntity);
        Interface oIface = getInterface(oldEntity, ifacetype);
        Interface nIface = getInterface(newEntity, ifacetype);
        if ((oIface == null) && (nIface == null)) {
            return true;
        }
        if ((oIface == null) || (nIface == null)) {
            return false;
        }

        return (StringUtils.equals(oIface.getEndpoint(), nIface.getEndpoint())
            && isSameDriverClazz(oIface.getDriver(), nIface.getDriver())
            && isSameCredentials(oIface.getCredential(), nIface.getCredential()));

    }

    /**
     * check if datasource driver is same.
     * @param oldEntity
     * @param newEntity
     * @return true if same or false
     */

    public static boolean isSameDriverClazz(Driver oldEntity, Driver newEntity) {
        if ((oldEntity == null) && (newEntity == null)) {
            return true;
        }
        if ((oldEntity == null) || (newEntity == null)) {
            return false;
        }
        return StringUtils.equals(oldEntity.getClazz(), newEntity.getClazz());
    }

    /**
     * checks if data source properties are same.
     * @param oldEntity
     * @param newEntity
     * @return true if same else false
     */

    public static boolean isSameProperties(Datasource oldEntity, Datasource newEntity) {
        Map<String, String> oldProps = getDatasourceProperties(oldEntity);
        Map<String, String> newProps = getDatasourceProperties(newEntity);
        return oldProps.equals(newProps);
    }

    /**
     * checks if data source credentials are same.
     * @param oCred
     * @param nCred
     * @return true true
     */
    public static boolean isSameCredentials(Credential oCred, Credential nCred) {
        if ((oCred == null) && (nCred == null)) {
            return true;
        }
        if ((oCred == null) || (nCred == null)) {
            return true;
        }
        if (StringUtils.equals(oCred.getUserName(), nCred.getUserName())) {
            if (oCred.getType() == nCred.getType()) {
                if (oCred.getType() == Credentialtype.PASSWORD_TEXT) {
                    return StringUtils.equals(oCred.getPasswordText(), nCred.getPasswordText());
                } else if (oCred.getType() == Credentialtype.PASSWORD_FILE) {
                    return StringUtils.equals(oCred.getPasswordFile(), nCred.getPasswordFile());
                } else if (oCred.getType() == Credentialtype.PASSWORD_ALIAS) {
                    return (StringUtils.equals(oCred.getPasswordAlias().getAlias(),
                            nCred.getPasswordAlias().getAlias())
                            && StringUtils.equals(oCred.getPasswordAlias().getProviderPath(),
                                    nCred.getPasswordAlias().getProviderPath()));
                }
            } else {
                return false;
            }
        }
        return false;
    }

    public static Credential getCredential(Datasource db) {
        return getCredential(db, null);
    }

    public static Credential getCredential(Datasource db, Interfacetype interfaceType) {
        if (interfaceType == null) {
            return db.getInterfaces().getCredential();
        } else {
            for(Interface iface : db.getInterfaces().getInterfaces()) {
                if (iface.getType() == interfaceType) {
                    return iface.getCredential();
                }
            }
        }
        return null;
    }

    public static void validateCredential(Credential cred) throws FalconException {
        if (cred == null) {
            return;
        }
        switch (cred.getType()) {
        case PASSWORD_TEXT:
            if (StringUtils.isBlank(cred.getUserName()) || StringUtils.isBlank(cred.getPasswordText())) {
                throw new FalconException(String.format("Credential type '%s' missing tags '%s' or '%s'",
                    cred.getType().value(), "userName", "passwordText"));
            }
            break;
        case PASSWORD_FILE:
            if (StringUtils.isBlank(cred.getUserName()) || StringUtils.isBlank(cred.getPasswordFile())) {
                throw new FalconException(String.format("Credential type '%s' missing tags '%s' or '%s'",
                    cred.getType().value(), "userName", "passwordFile"));
            }
            break;
        case PASSWORD_ALIAS:
            if (StringUtils.isBlank(cred.getUserName()) || (cred.getPasswordAlias() == null)
                || StringUtils.isBlank(cred.getPasswordAlias().getAlias())
                || StringUtils.isBlank(cred.getPasswordAlias().getProviderPath())) {
                throw new FalconException(String.format("Credential type '%s' missing tags '%s' or '%s' or %s'",
                    cred.getType().value(), "userName", "alias", "providerPath"));
            }
            break;
        default:
            throw new FalconException(String.format("Unknown Credential type '%s'", cred.getType().value()));
        }
    }

    /**
     * Return the Interface endpoint for the interface type specified in the argument.
     *
     * @param db
     * @param type - can be read-only or write
     * @return
     */
    private static String getInterfaceEndpoint(Datasource db, Interfacetype type) {
        if (getInterface(db, type) != null) {
            return getInterface(db, type).getEndpoint();
        } else {
            return null;
        }
    }

    private static Interface getInterface(Datasource db, Interfacetype type) {
        for(Interface ifs : db.getInterfaces().getInterfaces()) {
            if (ifs.getType() == type) {
                return ifs;
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

    /**
     * fetch password from the corresponding store.
     * @param c
     * @return actual password
     * @throws FalconException
     */
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

    /**
     * fetch the password from file.
     *
     * @param passwordFilePath
     * @return
     * @throws FalconException
     */

    private static String fetchPasswordInfoFromFile(String passwordFilePath) throws FalconException {
        try {
            Path path = new Path(passwordFilePath);
            FileSystem fs = HadoopClientFactory.get().createProxiedFileSystem(path.toUri());
            if (!fs.exists(path)) {
                throw new IOException("The password file does not exist! ");
            }

            if (!fs.isFile(path)) {
                throw new IOException("The password file cannot be a directory! ");
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


    /*
     returns data store properties
     */

    public static Map<String, String> getDatasourceProperties(final Datasource datasource) {
        Map<String, String> returnProps = new HashMap<String, String>();
        if (datasource.getProperties() != null) {
            for (Property prop : datasource.getProperties().getProperties()) {
                returnProps.put(prop.getName(), prop.getValue());
            }
        }
        return returnProps;
    }

}
