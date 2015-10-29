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
import org.apache.falcon.FalconException;
import org.apache.falcon.Pair;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.datasource.Credential;
import org.apache.falcon.entity.v0.datasource.Credentialtype;
import org.apache.falcon.entity.v0.datasource.Datasource;
import org.apache.falcon.entity.v0.datasource.DatasourceType;
import org.apache.falcon.entity.v0.datasource.Interface;
import org.apache.falcon.entity.v0.datasource.Interfaces;
import org.apache.falcon.entity.v0.datasource.Interfacetype;
import org.apache.falcon.entity.v0.feed.Cluster;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;

/**
 * DataSource entity helper methods.
 */

public final class DatasourceHelper {

    private static final Logger LOG = LoggerFactory.getLogger(DatasourceHelper.class);

    private static final ConfigurationStore STORE = ConfigurationStore.get();

    public static DatasourceType getImportSourceType(Cluster feedCluster) throws FalconException {
        Datasource ds = STORE.get(EntityType.DATASOURCE, feedCluster.getImport().getSource().getName());
        return ds.getType();
    }

    private DatasourceHelper() {}

    public static Datasource getDatasource(Cluster feedCluster) throws FalconException {
        return STORE.get(EntityType.DATASOURCE, feedCluster.getImport().getSource().getName());
    }
    public static String getReadOnlyEndpoint(Datasource db) {
        return getInterface(db, Interfacetype.READONLY);
    }

    /**
     * Returns user name and password pair as it is specified in the XML. If the credential type is
     * password-file, the path name is returned.
     *
     * @param db
     * @return user name and password pair
     * @throws FalconException
     */
    public static Pair<String, String> getReadPasswordInfo(Datasource db) throws FalconException {
        for (Interface ifs : db.getInterfaces().getInterfaces()) {
            if ((ifs.getType() == Interfacetype.READONLY) && (ifs.getCredential() != null)) {
                return getPasswordInfo(ifs.getCredential());
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
        Pair<String, String> passwdInfo = getReadPasswordInfo(db);
        java.util.Properties p = new java.util.Properties();
        p.put("user", passwdInfo.first);
        p.put("password", passwdInfo.second);
        if (getReadPasswordType(db) == Credentialtype.PASSWORD_FILE) {
            String actualPasswd = readPasswordInfoFromFile(passwdInfo.second);
            p.put("password", actualPasswd);
        }
        return p;
    }

    /**
     * Given Datasource, return the read-only credential type. If read-only credential is missing,
     * use interface's default credential.
     *
     * @param db
     * @return Credentialtype
     * @throws FalconException
     */
    public static Credentialtype getReadPasswordType(Datasource db) throws FalconException {
        for (Interface ifs : db.getInterfaces().getInterfaces()) {
            if ((ifs.getType() == Interfacetype.READONLY) && (ifs.getCredential() != null)) {
                return getPasswordType(ifs.getCredential());
            }
        }
        return getDefaultPasswordType(db.getInterfaces());
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
    private static Credentialtype getPasswordType(Credential c) {
        return c.getType();
    }

    private static Credentialtype getDefaultPasswordType(Interfaces ifs) throws FalconException {

        if (ifs.getCredential() != null) {
            return ifs.getCredential().getType();
        } else {
            throw new FalconException("Missing Interfaces default credential");
        }
    }

    private static Pair<String, String> getDefaultPasswordInfo(Interfaces ifs) throws FalconException {

        if (ifs.getCredential() != null) {
            return getPasswordInfo(ifs.getCredential());
        } else {
            throw new FalconException("Missing Interfaces default credential");
        }
    }

    private static Pair<String, String> getPasswordInfo(Credential c) throws FalconException {
        String passwd = null;
        if (c.getType() == Credentialtype.PASSWORD_FILE) {
            passwd = c.getPasswordFile();
        } else {
            passwd = c.getPasswordText();
        }
        return new Pair<String, String>(c.getUserName(), passwd);
    }

    private static String readPasswordInfoFromFile(String passwordFilePath) throws FalconException {
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
