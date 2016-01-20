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

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.falcon.resource.APIResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exception for REST APIs.
 */
public class FalconWebException extends WebApplicationException {

    private static final Logger LOG = LoggerFactory.getLogger(FalconWebException.class);

    public static FalconWebException newMetadataResourceException(String message, Response.Status status) {
        LOG.error("Action failed: {}\nError: {}", status, message);
        // Using MediaType.TEXT_PLAIN for newMetadataResourceException to ensure backward compatibility.
        return new FalconWebException(new Exception(message),
                Response.status(status).entity(message).type(MediaType.TEXT_PLAIN).build());
    }

    public static FalconWebException newAPIException(Throwable throwable) {
        return newAPIException(throwable, Response.Status.BAD_REQUEST);
    }

    public static FalconWebException newAPIException(Throwable throwable, Response.Status status) {
        String message = getMessage(throwable);
        return newAPIException(message, status);
    }

    public static FalconWebException newAPIException(String message) {
        return newAPIException(message, Response.Status.BAD_REQUEST);
    }

    public static FalconWebException newAPIException(String message, Response.Status status) {
        Response response = Response.status(status)
                .entity(new APIResult(APIResult.Status.FAILED, message))
                .type(MediaType.TEXT_XML_TYPE)
                .build();
        return new FalconWebException(response);
    }

    private static String getMessage(Throwable e) {
        if (e instanceof FalconWebException) {
            return ((APIResult)((FalconWebException) e).getResponse().getEntity()).getMessage();
        }
        return e.getCause() == null ? e.getMessage() : e.getMessage() + "\nCausedBy: " + e.getCause().getMessage();
    }

    public FalconWebException(Response response) {
        super(response);
    }

    public FalconWebException(Throwable e, Response response) {
        super(e, response);
    }
}
