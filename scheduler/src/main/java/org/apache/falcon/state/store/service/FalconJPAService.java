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
package org.apache.falcon.state.store.service;

import org.apache.commons.lang.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.service.FalconService;
import org.apache.falcon.state.store.jdbc.EntityBean;
import org.apache.falcon.state.store.jdbc.InstanceBean;
import org.apache.falcon.util.StartupProperties;
import org.apache.openjpa.persistence.OpenJPAEntityManagerFactorySPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import java.text.MessageFormat;
import java.util.Properties;

/**
 * Service that manages JPA.
 */
public final class FalconJPAService implements FalconService {

    private static final Logger LOG = LoggerFactory.getLogger(FalconJPAService.class);
    public static final String PREFIX = "falcon.statestore.";

    public static final String DB_SCHEMA = PREFIX + "schema.name";
    public static final String URL = PREFIX + "jdbc.url";
    public static final String DRIVER = PREFIX + "jdbc.driver";
    public static final String USERNAME = PREFIX + "jdbc.username";
    public static final String PASSWORD = PREFIX + "jdbc.password";
    public static final String CONN_DATA_SOURCE = PREFIX + "connection.data.source";
    public static final String CONN_PROPERTIES = PREFIX + "connection.properties";
    public static final String MAX_ACTIVE_CONN = PREFIX + "pool.max.active.conn";
    public static final String CREATE_DB_SCHEMA = PREFIX + "create.db.schema";
    public static final String VALIDATE_DB_CONN = PREFIX + "validate.db.connection";
    public static final String VALIDATE_DB_CONN_EVICTION_INTERVAL = PREFIX + "validate.db.connection.eviction.interval";
    public static final String VALIDATE_DB_CONN_EVICTION_NUM = PREFIX + "validate.db.connection.eviction.num";

    private EntityManagerFactory entityManagerFactory;
    // Persistent Unit which is defined in persistence.xml
    private String persistenceUnit;
    private static final FalconJPAService FALCON_JPA_SERVICE = new FalconJPAService();

    private FalconJPAService() {
    }

    public static FalconJPAService get() {
        return FALCON_JPA_SERVICE;
    }

    public EntityManagerFactory getEntityManagerFactory() {
        return entityManagerFactory;
    }

    public void setPersistenceUnit(String dbType) {
        if (StringUtils.isEmpty(dbType)) {
            throw new IllegalArgumentException(" DB type cannot be null or empty");
        }
        dbType = dbType.split(":")[0];
        this.persistenceUnit = "falcon-" + dbType;
    }

    @Override
    public String getName() {
        return this.getClass().getSimpleName();
    }

    @Override
    public void init() throws FalconException {
        Properties props = getPropsforStore();
        entityManagerFactory = Persistence.
                createEntityManagerFactory(persistenceUnit, props);
        EntityManager entityManager = getEntityManager();
        entityManager.find(EntityBean.class, 1);
        entityManager.find(InstanceBean.class, 1);
        LOG.info("All entities initialized");

        // need to use a pseudo no-op transaction so all entities, datasource
        // and connection pool are initialized one time only
        entityManager.getTransaction().begin();
        OpenJPAEntityManagerFactorySPI spi = (OpenJPAEntityManagerFactorySPI) entityManagerFactory;
        // Mask the password with '***'
        String logMsg = spi.getConfiguration().getConnectionProperties().replaceAll("Password=.*?,", "Password=***,");
        LOG.info("JPA configuration: {0}", logMsg);
        entityManager.getTransaction().commit();
        entityManager.close();
    }

    private Properties getPropsforStore() throws FalconException {
        String dbSchema = StartupProperties.get().getProperty(DB_SCHEMA);
        String url = StartupProperties.get().getProperty(URL);
        String driver = StartupProperties.get().getProperty(DRIVER);
        String user = StartupProperties.get().getProperty(USERNAME);
        String password = StartupProperties.get().getProperty(PASSWORD).trim();
        String maxConn = StartupProperties.get().getProperty(MAX_ACTIVE_CONN).trim();
        String dataSource = StartupProperties.get().getProperty(CONN_DATA_SOURCE);
        String connPropsConfig = StartupProperties.get().getProperty(CONN_PROPERTIES);
        boolean autoSchemaCreation = Boolean.parseBoolean(StartupProperties.get().getProperty(CREATE_DB_SCHEMA,
                "false"));
        boolean validateDbConn = Boolean.parseBoolean(StartupProperties.get().getProperty(VALIDATE_DB_CONN, "true"));
        String evictionInterval = StartupProperties.get().getProperty(VALIDATE_DB_CONN_EVICTION_INTERVAL).trim();
        String evictionNum = StartupProperties.get().getProperty(VALIDATE_DB_CONN_EVICTION_NUM).trim();

        if (!url.startsWith("jdbc:")) {
            throw new FalconException("invalid JDBC URL, must start with 'jdbc:'" + url);
        }
        String dbType = url.substring("jdbc:".length());
        if (dbType.indexOf(":") <= 0) {
            throw new FalconException("invalid JDBC URL, missing vendor 'jdbc:[VENDOR]:...'" + url);
        }
        setPersistenceUnit(dbType);
        String connProps = "DriverClassName={0},Url={1},Username={2},Password={3},MaxActive={4}";
        connProps = MessageFormat.format(connProps, driver, url, user, password, maxConn);
        Properties props = new Properties();
        if (autoSchemaCreation) {
            connProps += ",TestOnBorrow=false,TestOnReturn=false,TestWhileIdle=false";
            props.setProperty("openjpa.jdbc.SynchronizeMappings", "buildSchema(ForeignKeys=true)");
        } else if (validateDbConn) {
            // validation can be done only if the schema already exist, else a
            // connection cannot be obtained to create the schema.
            String interval = "timeBetweenEvictionRunsMillis=" + evictionInterval;
            String num = "numTestsPerEvictionRun=" + evictionNum;
            connProps += ",TestOnBorrow=true,TestOnReturn=true,TestWhileIdle=true," + interval + "," + num;
            connProps += ",ValidationQuery=select count(*) from ENTITIES";
            connProps = MessageFormat.format(connProps, dbSchema);
        } else {
            connProps += ",TestOnBorrow=false,TestOnReturn=false,TestWhileIdle=false";
        }
        if (connPropsConfig != null) {
            connProps += "," + connPropsConfig;
        }
        props.setProperty("openjpa.ConnectionProperties", connProps);
        props.setProperty("openjpa.ConnectionDriverName", dataSource);
        return props;
    }

    @Override
    public void destroy() throws FalconException {
        if (entityManagerFactory.isOpen()) {
            entityManagerFactory.close();
        }
    }


    /**
     * Return an EntityManager. Used by the StoreService.
     *
     * @return an entity manager
     */
    public EntityManager getEntityManager() {
        return getEntityManagerFactory().createEntityManager();
    }
}
