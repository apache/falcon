/*
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

package org.apache.ivory.client;

import java.io.InputStream;
import com.sun.jersey.api.client.ClientResponse;

/**
 * Exception thrown by IvoryClient
 */
public class IvoryCLIException extends Exception {

    public IvoryCLIException(String msg) {
        super(msg);
    }

    public IvoryCLIException(String msg, Throwable throwable) {
        super(msg, throwable);
    }

	public IvoryCLIException(ClientResponse clientResponse) {
		super(convertStreamToString(clientResponse.getEntityInputStream()));
	}
	
	public static String convertStreamToString(InputStream is) {
	    try {
	        return new java.util.Scanner(is).useDelimiter("\\A").next();
	    } catch (java.util.NoSuchElementException e) {
	        return "";
	    }
	}

}
