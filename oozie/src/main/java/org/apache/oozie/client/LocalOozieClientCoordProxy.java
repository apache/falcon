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

package org.apache.oozie.client;

import org.apache.oozie.BaseEngineException;
import org.apache.oozie.CoordinatorEngine;
import org.apache.oozie.CoordinatorEngineException;
import org.apache.oozie.LocalOozieClientCoord;

/**
 * Client API to submit and manage Oozie Coord jobs against an Oozie
 * intance.
 */
public class LocalOozieClientCoordProxy extends LocalOozieClientCoord {

    private final CoordinatorEngine coordEngine;

    /**
     * Create a coordinator client for Oozie local use.
     * <p/>
     *
     * @param coordEngine the engine instance to use.
     */
    public LocalOozieClientCoordProxy(CoordinatorEngine coordEngine) {
        super(coordEngine);
        this.coordEngine = coordEngine;
    }

    /**
     * Get the info of a coordinator job and subset actions.
     *
     * @param jobId job Id.
     * @param filter filter the status filter
     * @param start starting index in the list of actions belonging to the job
     * @param len number of actions to be returned
     * @return the job info.
     * @throws OozieClientException thrown if the job info could not be retrieved.
     */
    @Override
    public CoordinatorJob getCoordJobInfo(String jobId, String filter, int start, int len) throws OozieClientException {
        try {
            return coordEngine.getCoordJob(jobId, filter, start, len, false);
        } catch (BaseEngineException bex) {
            throw new OozieClientException(bex.getErrorCode().toString(), bex);
        }
    }

    /**
     * Change a coordinator job.
     *
     * @param jobId job Id.
     * @param changeValue change value.
     * @throws OozieClientException thrown if the job could not be changed.
     */
    public void change(String jobId, String changeValue) throws OozieClientException {
        try {
            coordEngine.change(jobId, changeValue);
        } catch (CoordinatorEngineException e) {
            throw new OozieClientException(e.getErrorCode().toString(), e);
        }
    }
}
