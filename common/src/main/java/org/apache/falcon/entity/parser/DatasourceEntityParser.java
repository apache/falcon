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

package org.apache.falcon.entity.parser;

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.DatasourceHelper;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.datasource.ACL;
import org.apache.falcon.entity.v0.datasource.Datasource;
import org.apache.falcon.entity.v0.datasource.Interfacetype;
import org.apache.falcon.util.HdfsClassLoader;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.util.Arrays;
import java.util.Properties;

/**
 * Parser for DataSource entity definition.
 */

public class DatasourceEntityParser extends EntityParser<Datasource> {

    private static final Logger LOG = LoggerFactory.getLogger(DatasourceEntityParser.class);

    public DatasourceEntityParser() {
        super(EntityType.DATASOURCE);
    }

    @Override
    public void validate(Datasource db) throws FalconException {
        ClassLoader previousClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            ClassLoader hdfsClassLoader = HdfsClassLoader.load(db.getName(), db.getDriver().getJars());
            Thread.currentThread().setContextClassLoader(hdfsClassLoader);
            validateInterface(db, Interfacetype.READONLY, hdfsClassLoader);
            validateInterface(db, Interfacetype.WRITE, hdfsClassLoader);
            validateACL(db);
        } catch(IOException io) {
            throw new ValidationException("Unable to copy driver jars to local dir: "
                    + Arrays.toString(db.getDriver().getJars().toArray()));
        } finally {
            Thread.currentThread().setContextClassLoader(previousClassLoader);
        }
    }

    private static void validateInterface(Datasource db, Interfacetype interfacetype, ClassLoader hdfsClassLoader)
        throws ValidationException {
        String endpoint = null;
        try {
            endpoint = DatasourceHelper.getReadOnlyEndpoint(db);
            if (StringUtils.isNotBlank(endpoint)) {
                LOG.info("Validating {0} endpoint {1} connection.", interfacetype.value(), endpoint);
                Properties userPasswdInfo = DatasourceHelper.fetchReadPasswordInfo(db);
                validateConnection(hdfsClassLoader, db.getDriver().getClazz(), endpoint, userPasswdInfo);
            }
        } catch(FalconException fe) {
            throw new ValidationException(String.format("Cannot validate '%s' "
                            + "interface '%s' " + "of database entity '%s' due to '%s' ",
                   interfacetype, endpoint,
                   db.getName(), fe.getMessage()));
        }
    }

    private static void validateConnection(ClassLoader hdfsClassLoader, String driverClass,
                                    String connectUrl, Properties userPasswdInfo)
        throws FalconException {
        try {
            java.sql.Driver driver = (java.sql.Driver) hdfsClassLoader.loadClass(driverClass).newInstance();
            LOG.info("Validating connection URL: {0} using driver: {1}", connectUrl, driver.getClass().toString());
            Connection con = driver.connect(connectUrl, userPasswdInfo);
            if (con == null) {
                throw new FalconException("DriverManager.getConnection() return "
                       + "null for URL : " + connectUrl);
            }
        } catch (Exception ex) {
            LOG.error("Exception while validating connection : ", ex);
            throw new FalconException(ex);
        }
    }

    /**
     * Validate ACL if authorization is enabled.
     *
     * @param  db database entity
     * @throws ValidationException
     */
    private void validateACL(Datasource db) throws ValidationException {
        if (isAuthorizationDisabled) {
            return;
        }

        // Validate the entity owner is logged-in, authenticated user if authorization is enabled
        final ACL dbACL = db.getACL();
        if (dbACL == null) {
            throw new ValidationException("Datasource ACL cannot be empty for:  " + db.getName());
        }

        validateACLOwnerAndGroup(dbACL);

        try {
            authorize(db.getName(), dbACL);
        } catch (AuthorizationException e) {
            throw new ValidationException(e);
        }
    }
}
