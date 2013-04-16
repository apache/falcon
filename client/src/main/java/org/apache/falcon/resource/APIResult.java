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

package org.apache.falcon.resource;

import org.apache.log4j.NDC;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.StringWriter;
import java.util.UUID;

/**
 * APIResult is the output returned by all the APIs; status-SUCCEEDED or FAILED
 * message- detailed message
 */
@XmlRootElement(name = "result")
@XmlAccessorType(XmlAccessType.FIELD)
public class APIResult {

    private Status status;

    private String message;

    private String requestId;

    private static final JAXBContext jc;

    static {
        try {
            jc = JAXBContext.newInstance(APIResult.class);
        } catch (JAXBException e) {
            throw new RuntimeException(e);
        }
    }

    public static enum Status {
        SUCCEEDED, PARTIAL, FAILED
    }

    public APIResult(Status status, String message) {
        super();
        this.status = status;
        this.message = message;
        requestId = NDC.peek();
        try {
            UUID.fromString(requestId);
        } catch (IllegalArgumentException e) {
            requestId = null;
        }
    }

    protected APIResult() {
        // private default constructor for JAXB
    }

    public Status getStatus() {
        return status;
    }

    public String getMessage() {
        return message;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String reqId) {
        this.requestId = reqId;
    }

    @Override
    public String toString() {
        try {
            StringWriter stringWriter = new StringWriter();
            Marshaller marshaller = jc.createMarshaller();
            marshaller.marshal(this, stringWriter);
            return stringWriter.toString();
        } catch (JAXBException e) {
            return e.getMessage();
        }
    }
}
