/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.falcon.regression.security;

import org.apache.falcon.regression.core.enumsAndConstants.MerlinConstants;
import org.apache.falcon.regression.core.interfaces.IEntityManagerHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.CleanupUtil;
import org.apache.falcon.regression.core.util.KerberosHelper;
import org.apache.falcon.regression.core.util.Util;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

/**
 * All the falcon operation are implemented as enum. The benefit of this is that these operations
 * can now be passed as parameters.
 */
enum EntityOp {
    status() {
        @Override
        public boolean executeAs(String user, IEntityManagerHelper helper, String data) {
            final ServiceResponse response;
            try {
                response = helper.getStatus(data, user);
            } catch (IOException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (URISyntaxException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (AuthenticationException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (InterruptedException e) {
                logger.error("Caught Exception: " + e);
                return false;
            }
            return AssertUtil.checkSucceeded(response);
        }
    },
    dependency() {
        @Override
        public boolean executeAs(String user, IEntityManagerHelper helper, String data) {
            final ServiceResponse response;
            try {
                response = helper.getEntityDependencies(data, user);
            } catch (IOException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (URISyntaxException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (AuthenticationException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (InterruptedException e) {
                logger.error("Caught exception: " + e);
                return false;
            }
            return AssertUtil.checkSucceeded(response);
        }
    },
    listing() {
        @Override
        public boolean executeAs(String user, IEntityManagerHelper helper, String data) {
            final String entityName = Util.readEntityName(data);
            final List<String> entities;
            try {
                entities = CleanupUtil.getAllEntitiesOfOneType(helper, user);
            } catch (IOException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (URISyntaxException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (AuthenticationException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (JAXBException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (InterruptedException e) {
                logger.error("Caught exception: " + e);
                return false;
            }
            logger.info("Checking for presence of " + entityName + " in " + entities);
            return entities.contains(entityName);
        }
    },
    definition() {
        @Override
        public boolean executeAs(String user, IEntityManagerHelper helper, String data) {
            final ServiceResponse response;
            try {
                response = helper.getEntityDefinition(data, user);
            } catch (IOException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (URISyntaxException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (AuthenticationException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (InterruptedException e) {
                logger.error("Caught exception: " + e);
                return false;
            }
            return AssertUtil.checkSucceeded(response);
        }
    },
    delete() {
        @Override
        public boolean executeAs(String user, IEntityManagerHelper helper, String data) {
            final ServiceResponse response;
            try {
                response = helper.delete(data, user);
            } catch (IOException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (URISyntaxException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (AuthenticationException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (InterruptedException e) {
                logger.error("Caught exception: " + e);
                return false;
            }
            return AssertUtil.checkSucceeded(response);
        }
    },
    update() {
        @Override
        public boolean executeAs(String user, IEntityManagerHelper helper, String data) {
            final ServiceResponse response;
            try {
                response = helper.update(data, data, user);
            } catch (IOException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (URISyntaxException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (AuthenticationException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (InterruptedException e) {
                logger.error("Caught exception: " + e);
                return false;
            }
            return AssertUtil.checkSucceeded(response);
        }
    },
    schedule() {
        @Override
        public boolean executeAs(String user, IEntityManagerHelper helper, String data) {
            final ServiceResponse response;
            try {
                response = helper.schedule(data, user);
            } catch (IOException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (URISyntaxException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (AuthenticationException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (InterruptedException e) {
                logger.error("Caught exception: " + e);
                return false;
            }
            return AssertUtil.checkSucceeded(response);
        }
    },
    submit() {
        @Override
        public boolean executeAs(String user, IEntityManagerHelper helper, String data) {
            final ServiceResponse response;
            try {
                response = helper.submitEntity(data, user);
            } catch (IOException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (URISyntaxException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (AuthenticationException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (InterruptedException e) {
                logger.error("Caught exception: " + e);
                return false;
            }
            return AssertUtil.checkSucceeded(response);
        }
    },
    submitAndSchedule() {
        @Override
        public boolean executeAs(String user, IEntityManagerHelper helper, String data) {
            final ServiceResponse response;
            try {
                response = helper.submitAndSchedule(data, user);
            } catch (IOException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (URISyntaxException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (AuthenticationException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (InterruptedException e) {
                logger.error("Caught exception: " + e);
                return false;
            }
            return AssertUtil.checkSucceeded(response);
        }
    },
    suspend() {
        @Override
        public boolean executeAs(String user, IEntityManagerHelper helper, String data) {
            final ServiceResponse response;
            try {
                response = helper.suspend(data, user);
            } catch (IOException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (URISyntaxException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (AuthenticationException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (InterruptedException e) {
                logger.error("Caught exception: " + e);
                return false;
            }
            return AssertUtil.checkSucceeded(response);
        }
    },
    resume() {
        @Override
        public boolean executeAs(String user, IEntityManagerHelper helper, String data) {
            final ServiceResponse response;
            try {
                response = helper.resume(data, user);
            } catch (IOException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (URISyntaxException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (AuthenticationException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (InterruptedException e) {
                logger.error("Caught exception: " + e);
                return false;
            }
            return AssertUtil.checkSucceeded(response);
        }
    },
    ;

    private static Logger logger = Logger.getLogger(EntityOp.class);
    public abstract boolean executeAs(String user, IEntityManagerHelper helper, String data);
}
