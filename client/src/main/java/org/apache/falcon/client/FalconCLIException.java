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

package org.apache.falcon.client;

import com.sun.jersey.api.client.ClientResponse;
import org.apache.falcon.resource.APIResult;

import java.io.IOException;
import java.io.InputStream;

/**
 * Exception thrown by FalconClient.
 * This was converted to RuntimeException in FALCON-1609.
 * Reasons:
 *  Ultimate goal of switching from current CLI to spring shell based CLI
 *  Spring Shell doesn't work well with unchecked Exceptions
 *  The exception currently only gets surfaced in CLI, and in code existing catch clauses will still work.
 */
public class FalconCLIException extends RuntimeException{

    private static final int MB = 1024 * 1024;

    public FalconCLIException(String msg) {
        super(msg);
    }

    public FalconCLIException(Throwable e) {
        super(e);
    }

    public FalconCLIException(String msg, Throwable throwable) {
        super(msg, throwable);
    }

    public static FalconCLIException fromReponse(ClientResponse clientResponse) {
        ClientResponse.Status status = clientResponse.getClientResponseStatus();
        String statusValue = status.toString();
        String message = "";
        if (status == ClientResponse.Status.BAD_REQUEST) {
            clientResponse.bufferEntity();
            InputStream in = clientResponse.getEntityInputStream();
            try {
                in.mark(MB);
                message = clientResponse.getEntity(APIResult.class).getMessage();
            } catch (Throwable th) {
                byte[] data = new byte[MB];
                try {
                    in.reset();
                    int len = in.read(data);
                    message = new String(data, 0, len);
                } catch (IOException e) {
                    message = e.getMessage();
                }
            }
        }
        return new FalconCLIException(statusValue + ";" + message);
    }
}
