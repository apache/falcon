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

package org.apache.falcon.regression.core.helpers.entity;

import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.resource.InstancesResult;
import org.apache.falcon.resource.InstancesSummaryResult;

import java.io.IOException;
import java.net.URISyntaxException;

/** Helper class to work with cluster endpoints of a colo. */
public class ClusterEntityHelper extends AbstractEntityHelper {


    private static final String INVALID_ERR = "Not Valid for Cluster Entity";

    public ClusterEntityHelper(String prefix) {
        super(prefix);
    }

    public String getEntityType() {
        return "cluster";
    }

    public String getEntityName(String entity) {
        return Util.readEntityName(entity);
    }

    public ServiceResponse getStatus(String data, String user) {
        throw new UnsupportedOperationException(INVALID_ERR);
    }

    public ServiceResponse resume(String data, String user) {
        throw new UnsupportedOperationException(INVALID_ERR);
    }

    public ServiceResponse schedule(String data, String user) {
        throw new UnsupportedOperationException(INVALID_ERR);
    }

    public ServiceResponse submitAndSchedule(String data, String user) {
        throw new UnsupportedOperationException(INVALID_ERR);
    }

    public ServiceResponse suspend(String data, String user) {
        throw new UnsupportedOperationException(INVALID_ERR);
    }

    @Override
    public InstancesResult getRunningInstance(String name, String user) {
        throw new UnsupportedOperationException(INVALID_ERR);
    }

    @Override
    public InstancesResult getProcessInstanceStatus(
        String readEntityName, String params, String user) {
        throw new UnsupportedOperationException(INVALID_ERR);
    }


    public InstancesResult getProcessInstanceSuspend(
        String readEntityName, String params, String user) {
        throw new UnsupportedOperationException(INVALID_ERR);
    }

    @Override
    public ServiceResponse update(String oldEntity, String newEntity, String user) {
        throw new UnsupportedOperationException(INVALID_ERR);
    }

    @Override
    public InstancesResult getProcessInstanceKill(String readEntityName,
                                                         String string, String user) {
        throw new UnsupportedOperationException(INVALID_ERR);
    }

    @Override
    public InstancesResult getProcessInstanceRerun(
        String readEntityName, String string, String user) {
        throw new UnsupportedOperationException(INVALID_ERR);
    }

    @Override
    public InstancesResult getProcessInstanceResume(
        String readEntityName, String string, String user) {
        throw new UnsupportedOperationException(INVALID_ERR);
    }

    @Override
    public InstancesSummaryResult getInstanceSummary(String readEntityName,
                                                     String string
    ) throws
        IOException, URISyntaxException {
        throw new UnsupportedOperationException(INVALID_ERR);
    }

    @Override
    public ServiceResponse getListByPipeline(String pipeline){
        throw new UnsupportedOperationException(INVALID_ERR);
    }
}
