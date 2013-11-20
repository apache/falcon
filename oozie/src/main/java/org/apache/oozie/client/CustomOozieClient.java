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

package org.apache.oozie.client;

import org.apache.falcon.security.CurrentUser;
import org.apache.falcon.util.RuntimeProperties;
import org.apache.log4j.Logger;
import org.apache.oozie.client.rest.RestConstants;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Wrapped Oozie Client.
 */
public class CustomOozieClient extends OozieClient {

    private static final Logger LOG = Logger.getLogger(CustomOozieClient.class);
    private static final Map<String, String> NONE = new HashMap<String, String>();

    public CustomOozieClient(String oozieUrl) {
        super(oozieUrl);
    }

    public Properties getConfiguration() throws OozieClientException {
        return (new OozieConfiguration(RestConstants.ADMIN_CONFIG_RESOURCE)).call();
    }

    public Properties getProperties() throws OozieClientException {
        return (new OozieConfiguration(RestConstants.ADMIN_JAVA_SYS_PROPS_RESOURCE)).call();
    }

    @Override
    protected HttpURLConnection createConnection(URL url, String method) throws IOException, OozieClientException {
        String strUrl = url.toString();
        if (!strUrl.contains(OozieClient.USER_NAME)) { // decorate the url with the user in request
            String paramSeparator = (strUrl.contains("?")) ? "&" : "?";
            strUrl += paramSeparator + OozieClient.USER_NAME + "=" + CurrentUser.getUser();
            url = new URL(strUrl);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Decorated url with user info: " + url);
            }
        }

        HttpURLConnection conn = super.createConnection(url, method);

        int connectTimeout = Integer.valueOf(RuntimeProperties.get().getProperty("oozie.connect.timeout", "1000"));
        conn.setConnectTimeout(connectTimeout);

        int readTimeout = Integer.valueOf(RuntimeProperties.get().getProperty("oozie.read.timeout", "45000"));
        conn.setReadTimeout(readTimeout);

        return conn;
    }

    private class OozieConfiguration extends ClientCallable<Properties> {

        public OozieConfiguration(String resource) {
            super("GET", RestConstants.ADMIN, resource, NONE);
        }

        @Override
        protected Properties call(HttpURLConnection conn) throws IOException, OozieClientException {
            conn.setRequestProperty("content-type", RestConstants.XML_CONTENT_TYPE);
            if ((conn.getResponseCode() == HttpURLConnection.HTTP_OK)) {
                Reader reader = new InputStreamReader(conn.getInputStream(), "UTF_8");
                JSONObject json = (JSONObject) JSONValue.parse(reader);
                Properties props = new Properties();
                props.putAll(json);
                return props;
            } else {
                handleError(conn);
                return null;
            }
        }
    }
}
