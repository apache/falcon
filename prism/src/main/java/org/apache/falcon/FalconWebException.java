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

package org.apache.falcon;

import org.apache.falcon.resource.APIResult;
import org.apache.falcon.resource.InstancesResult;
import org.apache.falcon.resource.InstancesSummaryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Exception for REST APIs.
 */
public class FalconWebException extends WebApplicationException {

    private static final Logger LOG = LoggerFactory.getLogger(FalconWebException.class);

    public static FalconWebException newException(Throwable e, Response.Status status) {
        return newException(getMessage(e), status);
    }

    public static FalconWebException newInstanceException(Throwable e, Response.Status status) {
        return newInstanceException(getMessage(e), status);
    }

    public static FalconWebException newInstanceSummaryException(Throwable e, Response.Status status) {
        String message = getMessage(e);
        LOG.error("Action failed: {}\nError: {}", status, message);
        APIResult result = new InstancesSummaryResult(APIResult.Status.FAILED, message);
        return new FalconWebException(Response.status(status).entity(result).type(MediaType.TEXT_XML_TYPE).build());
    }

    public static FalconWebException newException(APIResult result, Response.Status status) {
        LOG.error("Action failed: {}\nError: {}", status, result.getMessage());
        return new FalconWebException(Response.status(status).
                entity(result).type(MediaType.TEXT_XML_TYPE).build());
    }

    public static FalconWebException newException(String message, Response.Status status) {
        LOG.error("Action failed: {}\nError: {}", status, message);
        APIResult result = new APIResult(APIResult.Status.FAILED, message);
        return new FalconWebException(Response.status(status).
                entity(result).type(MediaType.TEXT_XML_TYPE).build());
    }

    public static FalconWebException newInstanceException(String message, Response.Status status) {
        LOG.error("Action failed: {}\nError: {}", status, message);
        APIResult result = new InstancesResult(APIResult.Status.FAILED, message);
        return new FalconWebException(Response.status(status).entity(result).type(MediaType.TEXT_XML_TYPE).build());
    }

    private static String getMessage(Throwable e) {
        StringWriter errors = new StringWriter();
        e.printStackTrace(new PrintWriter(errors));
        return errors.toString();
    }

    public FalconWebException(Response response) {
        super(response);
    }
}
