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
package org.apache.falcon.ambari.view;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.inject.Singleton;

import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.apache.ambari.view.URLStreamProvider;
import org.apache.ambari.view.ViewContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a class used to bridge the communication between the falcon-ui and
 * the falcon API executing inside ambari.
 */

@Singleton
public class FalconProxyImpersonator {
    private static final Logger LOG = LoggerFactory
            .getLogger(FalconProxyImpersonator.class);
    private static final String GET_METHOD = "GET";
    private static final String POST_METHOD = "POST";
    private static final String DELETE_METHOD = "DELETE";

    private static final String FALCON_ERROR = "<result><status>FAILED</status>";
    private static final String[] FORCE_JSON_RESPONSE = { "/entities/list/", "admin/version", };

    public static final String VIEW_KERBEROS_PRINCIPAL = "view.kerberos.principal";
    public static final String VIEW_KERBEROS_PRINCIPAL_KEYTAB = "view.kerberos.principal.keytab";

    private ViewContext viewContext;

    /**
     * Constructor to get the default viewcontext.
     * @param viewContext
     */
    @Inject
    public FalconProxyImpersonator(ViewContext viewContext) {
        this.viewContext = viewContext;
    }

    /**
     * Method to set the ambari user.
     * @param headers
     * @param ui
     * @return
     */
    @GET
    @Path("/")
    public Response setUser(@Context HttpHeaders headers, @Context UriInfo ui) {
        try {
            String userName = viewContext.getUsername();
            return Response.ok(userName).type(getResponseType(userName))
                    .build();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(ex.toString()).build();
        }
    }

    /**
     * Method to attend all the GET calls.
     * @param headers
     * @param ui
     * @return
     */
    @GET
    @Path("/{path: .*}")
    public Response getUsage(@Context HttpHeaders headers, @Context UriInfo ui) {
        try {
            String serviceURI = buildURI(ui);
            return consumeService(headers, serviceURI, GET_METHOD, null);
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(ex.toString()).build();
        }
    }

    /**
     * Method to attend all the POST calls.
     * @param xml
     * @param headers
     * @param ui
     * @return
     * @throws IOException
     */
    @POST
    @Path("/{path: .*}")
    public Response handlePost(String xml, @Context HttpHeaders headers,
            @Context UriInfo ui) throws IOException {
        try {
            String serviceURI = buildURI(ui);
            return consumeService(headers, serviceURI, POST_METHOD, xml);
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(ex.toString()).build();
        }
    }

    /**
     * Method to attend all the DELETE calls.
     * @param headers
     * @param ui
     * @return
     * @throws IOException
     */
    @DELETE
    @Path("/{path: .*}")
    public Response handleDelete(@Context HttpHeaders headers,
            @Context UriInfo ui) throws IOException {
        try {
            String serviceURI = buildURI(ui);
            return consumeService(headers, serviceURI, DELETE_METHOD, null);
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(ex.toString()).build();
        }
    }

    /**
     * Method set the parametters and cast them to a String.
     * @param ui
     * @return
     */
    private String buildURI(UriInfo ui) {
        String serviceURI = getFalconURL();
        serviceURI += getUIURI(ui);
        StringBuilder urlBuilder = new StringBuilder(serviceURI);
        MultivaluedMap<String, String> parameters = ui.getQueryParameters();
        boolean firstEntry = true;
        for (Map.Entry<String, List<String>> entry : parameters.entrySet()) {
            if (firstEntry) {
                urlBuilder.append("?");
            } else {
                urlBuilder.append("&");
            }
            boolean firstVal = true;
            for (String val : entry.getValue()) {
                urlBuilder.append(firstVal ? "" : "&").append(entry.getKey())
                        .append("=").append(val);
                firstVal = false;
            }
            firstEntry = false;
        }
        return urlBuilder.toString();
    }

    private String getUIURI(UriInfo uriInfo) {
        String uriPath = uriInfo.getAbsolutePath().getPath();
        int index = uriPath.indexOf("proxy/") + 5;
        return uriPath.substring(index);
    }

    /**
     * Method to consume the API from the URLStreamProvider.
     * @param headers
     * @param urlToRead
     * @param method
     * @param xml
     * @return
     * @throws Exception
     */
    private Response consumeService(HttpHeaders headers, String urlToRead,
            String method, String xml) throws Exception {
        Response response;
        URLStreamProvider streamProvider = viewContext.getURLStreamProvider();
        Map<String, String> newHeaders = getHeaders(headers);
        newHeaders.put("user.name", viewContext.getUsername());

        if (checkForceJsonRepsonse(urlToRead, newHeaders)) {
            newHeaders.put("Accept", MediaType.APPLICATION_JSON);
        }
        LOG.info(String.format("Falcon Url[%s]", urlToRead));
        InputStream stream = null;
        if (isSecurityEnabled()) {
            stream = streamProvider.readAsCurrent(urlToRead, method, xml,
                    newHeaders);
        } else {
            stream = streamProvider
                    .readFrom(urlToRead, method, xml, newHeaders);
        }

        String sresponse = getStringFromInputStream(stream);

        if (sresponse.contains(FALCON_ERROR)
                || sresponse.contains(Response.Status.BAD_REQUEST.name())) {
            response = Response.status(Response.Status.BAD_REQUEST)
                    .entity(sresponse).type(MediaType.TEXT_PLAIN).build();
        } else {
            return Response.status(Response.Status.OK).entity(sresponse)
                    .type(getResponseType(sresponse)).build();
        }

        return response;
    }

    private boolean isSecurityEnabled() {
        return !"simple".equals(viewContext.getProperties().get(
                "falcon.authentication.type"));
    }

    private String getFalconURL() {
        String falconUri = "";
        if (viewContext.getCluster() != null) {
            String tlsEnabled = viewContext.getCluster().getConfigurationValue(
                    "falcon_startup.properties", "falcon.enableTLS");
            String scheme = Boolean.parseBoolean(tlsEnabled) ? "https" : "http";
            String falconHost = viewContext.getCluster()
                    .getHostsForServiceComponent("FALCON", "FALCON_SERVER")
                    .get(0);
            String falconPort = viewContext.getCluster().getConfigurationValue(
                    "falcon-env", "falcon_port");
            falconUri = scheme + "://" + falconHost + ":" + falconPort;

        } else {
            falconUri = viewContext.getProperties().get("falcon.service.uri");
        }
        LOG.info("Falcon URI==" + falconUri);
        return falconUri;

    }

    /**
     * Method to read the response and send it to the front.
     * @param is
     * @return
     */
    private String getStringFromInputStream(InputStream is) {

        BufferedReader br = null;
        StringBuilder sb = new StringBuilder();

        String line;
        try {
            br = new BufferedReader(new InputStreamReader(is));
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    LOG.error(e.getMessage(), e);
                }
            }
        }
        return sb.toString();
    }

    /**
     * Method to cast the response type.
     * @param response
     * @return
     */
    private String getResponseType(String response) {
        if (response.startsWith("{")) {
            return MediaType.TEXT_PLAIN;
        } else if (response.startsWith("<")) {
            return MediaType.TEXT_XML;
        } else {
            return MediaType.TEXT_PLAIN;
        }
    }

    private boolean checkForceJsonRepsonse(String urlToRead,
            Map<String, String> headers) throws Exception {
        for (int i = 0; i < FORCE_JSON_RESPONSE.length; i++) {
            if (urlToRead.contains(FORCE_JSON_RESPONSE[i])) {
                return true;
            }
        }
        return false;
    }

    private Map<String, String> getHeaders(HttpHeaders headers) {
        MultivaluedMap<String, String> requestHeaders = headers
                .getRequestHeaders();
        Set<Entry<String, List<String>>> headerEntrySet = requestHeaders
                .entrySet();
        HashMap<String, String> headersMap = new HashMap<String, String>();
        for (Entry<String, List<String>> headerEntry : headerEntrySet) {
            String key = headerEntry.getKey();
            List<String> values = headerEntry.getValue();
            headersMap.put(key, strJoin(values, ","));
        }
        return headersMap;
    }

    // TODO use one of libraries.
    private String strJoin(List<String> strings, String separator) {

        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0, il = strings.size(); i < il; i++) {
            if (i > 0) {
                stringBuilder.append(separator);
            }
            stringBuilder.append(strings.get(i));
        }
        return stringBuilder.toString();
    }

}
