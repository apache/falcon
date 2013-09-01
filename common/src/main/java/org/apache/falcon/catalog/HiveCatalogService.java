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

package org.apache.falcon.catalog;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.apache.falcon.FalconException;
import org.apache.falcon.security.CurrentUser;
import org.apache.log4j.Logger;

import javax.ws.rs.core.MediaType;

/**
 * An implementation of CatalogService that uses Hive Meta Store (HCatalog)
 * as the backing Catalog registry.
 */
public class HiveCatalogService extends AbstractCatalogService {

    private static final Logger LOG = Logger.getLogger(HiveCatalogService.class);

    @Override
    public boolean isAlive(String catalogBaseUrl) throws FalconException {
        LOG.info("Checking if the service is alive for: " + catalogBaseUrl);

        Client client = Client.create();
        WebResource service = client.resource(catalogBaseUrl);
        ClientResponse response = service.path("status")
                .accept(MediaType.APPLICATION_JSON)
                .head();
                // .get(ClientResponse.class);    // todo this isnt working

        if (LOG.isDebugEnabled() && response.getStatus() != 200) {
            LOG.debug("Output from Server .... \n" + response.getEntity(String.class));
        }

        return response.getStatus() == 200;
    }

    @Override
    public boolean tableExists(String catalogUrl, String database, String tableName)
        throws FalconException {
        LOG.info("Checking if the table exists: " + tableName);

        Client client = Client.create();
        WebResource service = client.resource(catalogUrl);

        ClientResponse response = service.path("ddl/database/").path(database)
                .path("/table").path(tableName)
                .queryParam("user.name", CurrentUser.getUser())
                .accept(MediaType.APPLICATION_JSON)
                .get(ClientResponse.class);

        if (LOG.isDebugEnabled() && response.getStatus() != 200) {
            LOG.debug("Output from Server .... \n" + response.getEntity(String.class));
        }

        return response.getStatus() == 200;
    }
}
