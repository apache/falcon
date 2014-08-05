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

package org.apache.falcon.regression.core.supportClasses;

import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.interfaces.IEntityManagerHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.testng.TestNGException;
import org.apache.log4j.Logger;

/** Class for running a rest request in a parallel thread. */
public class Brother extends Thread {
    private String operation;
    private String data;
    private URLS url;
    private ServiceResponse output;
    private static final Logger LOGGER = Logger.getLogger(Brother.class);

    public ServiceResponse getOutput() {
        return output;
    }

    private IEntityManagerHelper entityManagerHelper;

    public Brother(String threadName, String operation, EntityType entityType, ThreadGroup tGroup,
                   Bundle b, ColoHelper p, URLS url) {
        super(tGroup, threadName);
        this.operation = operation;
        switch (entityType) {
        case PROCESS:
            this.data = b.getProcessData();
            this.entityManagerHelper = p.getProcessHelper();
            break;
        case CLUSTER:
            this.entityManagerHelper = p.getClusterHelper();
            this.data = b.getClusters().get(0);
            break;
        case FEED:
            this.entityManagerHelper = p.getFeedHelper();
            this.data = b.getDataSets().get(0);
            break;
        default:
            LOGGER.error("Unexpected entityType=" + entityType);
        }
        this.url = url;
        this.output = new ServiceResponse();
    }

    public void run() {
        try {
            sleep(50L);
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        }
        LOGGER.info("Brother " + this.getName() + " will be executing " + operation);
        try {
            switch (url) {
            case SUBMIT_URL:
                output = entityManagerHelper.submitEntity(url, data);
                break;
            case GET_ENTITY_DEFINITION:
                output = entityManagerHelper.getEntityDefinition(url, data);
                break;
            case DELETE_URL:
                output = entityManagerHelper.delete(url, data);
                break;
            case SUSPEND_URL:
                output = entityManagerHelper.suspend(url, data);
                break;
            case SCHEDULE_URL:
                output = entityManagerHelper.schedule(url, data);
                break;
            case RESUME_URL:
                output = entityManagerHelper.resume(url, data);
                break;
            case SUBMIT_AND_SCHEDULE_URL:
                output = entityManagerHelper.submitAndSchedule(url, data);
                break;
            case STATUS_URL:
                output = entityManagerHelper.getStatus(url, data);
                break;
            default:
                LOGGER.error("Unexpected url: " + url);
                break;
            }
            LOGGER.info("Brother " + getName() + "'s response to the "
                + operation + " is: " + output);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
