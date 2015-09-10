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

package org.apache.falcon.resource.metadata;

import com.tinkerpop.blueprints.Graph;
import org.apache.falcon.FalconWebException;
import org.apache.falcon.metadata.MetadataMappingService;
import org.apache.falcon.service.FalconService;
import org.apache.falcon.service.Services;

import javax.ws.rs.core.Response;
import java.util.Set;

/**
 * A base class for managing Metadata operations.
]*/
public abstract class AbstractMetadataResource {
    public static final String RESULTS = "results";
    public static final String TOTAL_SIZE = "totalSize";

    protected MetadataMappingService service;

    public AbstractMetadataResource() {
        FalconService falconService = getService(MetadataMappingService.SERVICE_NAME);
        service = (falconService != null) ? (MetadataMappingService)falconService : null;
    }

    private static FalconService getService(String serviceName) {
        return (Services.get().isRegistered(serviceName))
                ? Services.get().getService(serviceName) : null;
    }

    protected Graph getGraph() {
        checkIfMetadataMappingServiceIsEnabled();
        return service.getGraph();
    }

    protected Set<String> getVertexIndexedKeys() {
        checkIfMetadataMappingServiceIsEnabled();
        return service.getVertexIndexedKeys();
    }

    protected Set<String> getEdgeIndexedKeys() {
        checkIfMetadataMappingServiceIsEnabled();
        return service.getEdgeIndexedKeys();
    }

    private void checkIfMetadataMappingServiceIsEnabled() {
        if (service == null) {
            throw FalconWebException.newMetadataResourceException(
                    "Lineage " + MetadataMappingService.SERVICE_NAME + " is not enabled.", Response.Status.NOT_FOUND);
        }
    }
}
