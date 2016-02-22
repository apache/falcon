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

package org.apache.falcon.util;

import org.apache.commons.io.IOUtils;
import org.apache.falcon.FalconException;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;

/**
 * Utility class to get the Hadoop Queue names by querying resource manager.
 */
public final class HadoopQueueUtil {

    private static final Logger LOG = LoggerFactory.getLogger(HadoopQueueUtil.class);

    private HadoopQueueUtil() {
        // make the constructor private
    }

    /**
     * Uses Resource Manager REST API to get the hadoop scheduler info.
     *
     * @param rmBaseUrlStr
     * @return JSON string representing hadoop Scheduler Info
     * @throws FalconException
     */

    public static String getHadoopClusterSchedulerInfo(String rmBaseUrlStr) throws FalconException {
        KerberosAuthenticator kAUTHENTICATOR = new KerberosAuthenticator();
        AuthenticatedURL.Token authenticationToken = new AuthenticatedURL.Token();
        String rmSchedulerInfoURL = rmBaseUrlStr;
        if (!rmSchedulerInfoURL.endsWith("/")) {
            rmSchedulerInfoURL += "/";
        }
        rmSchedulerInfoURL += "ws/v1/cluster/scheduler";
        HttpURLConnection conn = null;
        BufferedReader reader = null;

        try {
            URL url = new URL(rmSchedulerInfoURL);
            conn = new AuthenticatedURL(kAUTHENTICATOR).openConnection(url, authenticationToken);
            reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            StringBuilder jsonResponse =  new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                jsonResponse.append(line);
            }
            return jsonResponse.toString();
        } catch (Exception ex) {
            throw new RuntimeException("Could not authenticate, " + ex.getMessage(), ex);
        } finally {
            IOUtils.closeQuietly(reader);
            if (conn != null) {
                conn.disconnect();
            }
        }

    }

    /**
     *
     *
     * @param jsonResult
     * @param qNames
     * @return
     * @throws JSONException
     */

    public static Set<String> getHadoopClusterQueueNamesHelper(String jsonResult, Set<String> qNames)
        throws JSONException {
        String qJson = extractRootQueuesElement(jsonResult);
        LOG.debug("Extracted Queue JSON - {}", qJson);
        JSONObject jObject = new JSONObject(qJson);
        LOG.debug("Parsing Json result done");
        JSONObject queues = jObject.getJSONObject("queues");
        jsonParseForHadoopQueueNames(queues, qNames);
        return qNames;
    }

    /**
     * Recursively parses JSON hadoop cluster scheduler info and returns all the sub queue names in the output
     * parameter.
     *
     * @param queues JSON document queues element
     * @param qNames Output parameter that will have all hadoop cluster queue names
     * @throws JSONException
     *
     */
    public static void jsonParseForHadoopQueueNames(JSONObject queues, Set<String> qNames) throws JSONException {
        JSONArray qs = queues.getJSONArray("queue");
        for(int i=0; i<qs.length(); i++) {
            JSONObject q = qs.getJSONObject(i);
            qNames.add(q.getString("queueName"));

            if ((q.isNull("type"))
                    || (!q.getString("type").equalsIgnoreCase("capacitySchedulerLeafQueueInfo"))) {
                jsonParseForHadoopQueueNames(q.getJSONObject("queues"), qNames);
            }
        }
    }
    /**
     * Parse the hadoop cluster scheduler info to extract JSON element 'queues'.
     *
     * NOTE: the JSON returned by Resource Manager REST API is not well formed
     * and trying to parse the entire returned document results in parse exception
     * using latest JSON parsers.
     *
     * @param json
     * @return
     */

    public static String extractRootQueuesElement(String json) {
        int start = json.indexOf("\"queues\":");
        int i = start;
        while(json.charAt(i) != '{') {
            i++;
        }
        i++;
        int count = 1;
        while (count != 0) {
            if (json.charAt(i) == '{') {
                count++;
            } else if (json.charAt(i) == '}') {
                count--;
            }
            i++;
        }
        return "{" + json.substring(start, i) + "}";
    }

    /**
     * Retrieves scheduler info JSON from the resource manager and extracts hadoop cluster queue names into
     * a set of strings.
     *
     * @param rmBaseUrlStr
     * @return
     * @throws FalconException
     */

    public static Set<String> getHadoopClusterQueueNames(String rmBaseUrlStr) throws FalconException {
        String jsonResult = getHadoopClusterSchedulerInfo(rmBaseUrlStr);
        LOG.debug("Scheduler Info Result : {} ", jsonResult);
        Set<String> qNames = new HashSet<>();
        try {
            return getHadoopClusterQueueNamesHelper(jsonResult, qNames);
        } catch(JSONException jex) {
            throw new FalconException(jex);
        }
    }
}
