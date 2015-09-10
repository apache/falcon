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
import org.apache.falcon.resource.InstanceDependencyResult;
import org.apache.falcon.resource.InstancesResult;
import org.apache.falcon.resource.InstancesSummaryResult;
import org.apache.falcon.resource.TriageResult;
import org.apache.hadoop.security.authorize.AuthorizationException;
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

    public static FalconWebException newException(Throwable e,
                                                  Response.Status status) {
        if (e instanceof AuthorizationException) {
            status = Response.Status.FORBIDDEN;
        }

        return newException(e.getMessage(), status);
    }

    public static FalconWebException newTriageResultException(Throwable e, Response.Status status) {
        String message = getMessage(e);
        LOG.error("Triage failed: {}\nError: {}", status, message);
        APIResult result = new TriageResult(APIResult.Status.FAILED, message);
        return new FalconWebException(e, Response.status(status).entity(result).type(MediaType.TEXT_XML_TYPE).build());
    }

    public static FalconWebException newInstanceSummaryException(Throwable e, Response.Status status) {
        String message = getMessage(e);
        LOG.error("Action failed: {}\nError: {}", status, message);
        APIResult result = new InstancesSummaryResult(APIResult.Status.FAILED, message);
        return new FalconWebException(e, Response.status(status).entity(result).type(MediaType.TEXT_XML_TYPE).build());
    }

    public static FalconWebException newInstanceDependencyResult(Throwable e, Response.Status status) {
        return newInstanceDependencyResult(getMessage(e), status);
    }

    public static FalconWebException newInstanceDependencyResult(String message, Response.Status status) {
        LOG.error("Action failed: {}\nError: {}", status, message);
        APIResult result = new InstanceDependencyResult(APIResult.Status.FAILED, message);
        return new FalconWebException(new Exception(message),
                Response.status(status).entity(result).type(MediaType.TEXT_XML_TYPE).build());
    }

    public static FalconWebException newException(String message, Response.Status status) {
        APIResult result = new APIResult(APIResult.Status.FAILED, message);
        return newException(result, status);
    }

    public static FalconWebException newException(APIResult result, Response.Status status) {
        LOG.error("Action failed: {}\nError: {}", status, result.getMessage());
        return new FalconWebException(new FalconException(result.getMessage()),
                Response.status(status).entity(result).type(MediaType.TEXT_XML_TYPE).build());
    }

    public static FalconWebException newInstanceException(String message, Response.Status status) {
        return newInstanceException(new FalconException(message), status);
    }

    public static FalconWebException newInstanceException(Throwable e, Response.Status status) {
        LOG.error("Action failed: {}\nError: {}", status, e.getMessage());
        APIResult result = new InstancesResult(APIResult.Status.FAILED, e.getMessage());
        return new FalconWebException(e, Response.status(status).entity(result).type(MediaType.TEXT_XML_TYPE).build());
    }

    public static FalconWebException newMetadataResourceException(String message, Response.Status status) {
        LOG.error("Action failed: {}\nError: {}", status, message);
        // Using MediaType.TEXT_PLAIN for newMetadataResourceException to ensure backward compatibility.
        return new FalconWebException(new Exception(message),
                Response.status(status).entity(message).type(MediaType.TEXT_PLAIN).build());
    }

    private static String getMessage(Throwable e) {
        StringWriter errors = new StringWriter();
        e.printStackTrace(new PrintWriter(errors));
        return errors.toString();
    }

    public FalconWebException(Throwable e, Response response) {
        super(e, response);
    }
}
