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

package org.apache.falcon.regression.core.response;

import com.google.gson.GsonBuilder;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.resource.EntityList;
import org.apache.falcon.resource.EntitySummaryResult;
import org.apache.falcon.resource.LineageGraphResult;
import org.apache.falcon.resource.SchedulableEntityInstanceResult;
import org.apache.http.HttpResponse;
import org.apache.log4j.Logger;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.IOException;
import java.io.StringReader;

/** Class to represent falcon's response to a rest request. */
public class ServiceResponse {
    private static final Logger LOGGER = Logger.getLogger(ServiceResponse.class);

    private String message;
    private int code;
    private HttpResponse response;

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public HttpResponse getResponse() {
        return response;
    }

    public void setResponse(HttpResponse response) {
        this.response = response;
    }

    public ServiceResponse(String message, int code) {
        this.message = message;
        this.code = code;
    }

    public ServiceResponse(HttpResponse response) throws IOException {
        this.message = IOUtils.toString(response.getEntity().getContent());
        this.code = response.getStatusLine().getStatusCode();
        this.response = response;

        LOGGER.info("The web service response is:\n" + Util.prettyPrintXmlOrJson(message));
    }

    public ServiceResponse() {
    }

    /**
     * Retrieves EntitiesResult from a message if possible.
     * @return EntitiesResult
     */
    public EntityList getEntityList(){
        try {
            JAXBContext jc = JAXBContext.newInstance(EntityList.class);
            Unmarshaller u = jc.createUnmarshaller();
            return  (EntityList) u.unmarshal(new StringReader(message));
        } catch (JAXBException e) {
            LOGGER.info("getEntityList() failed:\n" + ExceptionUtils.getStackTrace(e));
            return null;
        }
    }

    /**
     * Retrieves EntitySummaryResult from a message if possible.
     * @return EntitiesResult
     */
    public EntitySummaryResult getEntitySummaryResult() {
        try {
            JAXBContext jc = JAXBContext.newInstance(EntitySummaryResult.class);
            Unmarshaller u = jc.createUnmarshaller();
            return  (EntitySummaryResult) u.unmarshal(new StringReader(message));
        } catch (JAXBException e) {
            LOGGER.info("getEntitySummaryResult() failed:\n" + ExceptionUtils.getStackTrace(e));
            return null;
        }
    }

    /**
     * Retrieves LineageGraphResult from a message if possible.
     * @return LineageGraphResult
     */
    public LineageGraphResult getLineageGraphResult() {
        LineageGraphResult lineageGraphResult = new GsonBuilder().create().fromJson(message, LineageGraphResult.class);
        return lineageGraphResult;
    }

    /**
     * Retrieves SchedulableEntityInstanceResult from a message if possible.
     * @return SchedulableEntityInstanceResult
     */
    public SchedulableEntityInstanceResult getSlaResult() {
        SchedulableEntityInstanceResult schedulableEntityInstanceResult = new GsonBuilder().create().fromJson(message,
                SchedulableEntityInstanceResult.class);
        return schedulableEntityInstanceResult;
    }
}
