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

package org.apache.falcon.regression.core.util;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.jcraft.jsch.JSchException;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.regression.Entities.ClusterMerlin;
import org.apache.falcon.regression.Entities.FeedMerlin;
import org.apache.falcon.regression.Entities.ProcessMerlin;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.helpers.entity.AbstractEntityHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.supportClasses.JmsMessageConsumer;
import org.apache.falcon.request.BaseRequest;
import org.apache.falcon.request.RequestKeys;
import org.apache.falcon.resource.APIResult;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.http.HttpResponse;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.Assert;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

/**
 * util methods used across test.
 */
public final class Util {

    private Util() {
        throw new AssertionError("Instantiating utility class...");
    }
    private static final Logger LOGGER = Logger.getLogger(Util.class);

    /**
     * Sends request without data and user.
     */
    public static ServiceResponse sendRequest(String url, String method)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return sendRequest(url, method, null, null);
    }

    /**
     * Sends api request without data.
     */
    public static ServiceResponse sendRequest(String url, String method, String user)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return sendRequest(url, method, null, user);
    }

    /**
     * Sends api requests.
     * @param url target url
     * @param method request method
     * @param data data to be places in body of request
     * @param user user to be used to send request
     * @return api response
     * @throws IOException
     * @throws URISyntaxException
     * @throws AuthenticationException
     */
    public static ServiceResponse sendRequest(String url, String method, String data, String user)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        BaseRequest request = new BaseRequest(url, method, user, data);
        request.addHeader(RequestKeys.CONTENT_TYPE_HEADER, RequestKeys.XML_CONTENT_TYPE);
        HttpResponse response = request.run();
        return new ServiceResponse(response);
    }

    /**
     * @param data string data
     * @return is data should be considered as XMl
     */
    private static boolean isXML(String data) {
        return data != null && data.trim().length() > 0 && data.trim().startsWith("<");
    }

    /**
     * Converts service response to api result form.
     * @param response service response
     * @return api result
     * @throws JAXBException
     */
    public static APIResult parseResponse(ServiceResponse response) throws JAXBException {
        if (!isXML(response.getMessage())) {
            return new APIResult(APIResult.Status.FAILED, response.getMessage());
        }
        JAXBContext jc = JAXBContext.newInstance(APIResult.class);
        Unmarshaller u = jc.createUnmarshaller();
        if (response.getMessage().contains("requestId")) {
            return  (APIResult) u
                .unmarshal(new InputSource(new StringReader(response.getMessage())));
        } else {
            return new APIResult(response.getCode() == 200
                ? APIResult.Status.SUCCEEDED : APIResult.Status.FAILED, response.getMessage());
        }
    }

    /**
     * Lists all directories contained in a store by sub-path.
     * @param helper cluster where store is present
     * @param subPath sub-path
     * @return list of all directories in the sub-path
     * @throws IOException
     * @throws JSchException
     */
    public static List<String> getStoreInfo(AbstractEntityHelper helper, String subPath)
        throws IOException, JSchException {
        if (helper.getStoreLocation().startsWith("hdfs:")) {
            return HadoopUtil.getAllFilesHDFS(helper.getHadoopFS(),
                new Path(helper.getStoreLocation() + subPath));
        } else {
            return ExecUtil.runRemoteScriptAsSudo(helper.getQaHost(), helper.getUsername(),
                    helper.getPassword(), "ls " + helper.getStoreLocation()  + subPath,
                    helper.getUsername(), helper.getIdentityFile());
        }
    }

    /**
     * @param data entity definition
     * @return entity name
     */
    public static String readEntityName(String data) {
        if (data.contains("uri:falcon:feed")) {
            return new FeedMerlin(data).getName();
        } else if (data.contains("uri:falcon:process")) {
            return new ProcessMerlin(data).getName();
        } else {
            return new ClusterMerlin(data).getName();
        }
    }

    /**
     * Retrieves all hadoop data directories from a specific data path.
     * @param fs filesystem
     * @param feed feed definition
     * @param dir specific directory
     * @return all
     * @throws IOException
     */
    public static List<String> getHadoopDataFromDir(FileSystem fs, String feed, String dir)
        throws IOException {
        List<String> finalResult = new ArrayList<>();
        String feedPath = new FeedMerlin(feed).getFeedPath(LocationType.DATA);
        int depth = feedPath.split(dir)[1].split("/").length - 1;
        List<Path> results = HadoopUtil.getAllDirsRecursivelyHDFS(fs, new Path(dir), depth);
        for (Path result : results) {
            int pathDepth = result.toString().split(dir)[1].split("/").length - 1;
            if (pathDepth == depth) {
                finalResult.add(result.toString().split(dir)[1]);
            }
        }
        return finalResult;
    }

    /**
     * Finds first folder within a date range.
     * @param startTime start date
     * @param endTime end date
     * @param folderList list of folders which are under analysis
     * @return first matching folder or null if not present in a list
     */
    public static String findFolderBetweenGivenTimeStamps(DateTime startTime, DateTime endTime,
                                                          List<String> folderList) {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy/MM/dd/HH/mm");
        for (String folder : folderList) {
            if (folder.compareTo(formatter.print(startTime)) >= 0
                    &&
                folder.compareTo(formatter.print(endTime)) <= 0) {
                return folder;
            }
        }
        return null;
    }

    public static List<String> getInstanceFinishTimes(ColoHelper coloHelper, String workflowId)
        throws IOException, JSchException {
        List<String> raw = ExecUtil.runRemoteScriptAsSudo(coloHelper.getProcessHelper()
                .getQaHost(), coloHelper.getProcessHelper().getUsername(),
            coloHelper.getProcessHelper().getPassword(),
            "cat /var/log/falcon/application.* | grep \"" + workflowId + "\" | grep "
                    + "\"Received\" | awk '{print $2}'",
            coloHelper.getProcessHelper().getUsername(),
            coloHelper.getProcessHelper().getIdentityFile()
        );
        List<String> finalList = new ArrayList<>();
        for (String line : raw) {
            finalList.add(line.split(",")[0]);
        }
        return finalList;
    }

    public static List<String> getInstanceRetryTimes(ColoHelper coloHelper, String workflowId)
        throws IOException, JSchException {
        List<String> raw = ExecUtil.runRemoteScriptAsSudo(coloHelper.getProcessHelper()
                .getQaHost(), coloHelper.getProcessHelper().getUsername(),
            coloHelper.getProcessHelper().getPassword(),
            "cat /var/log/falcon/application.* | grep \"" + workflowId + "\" | grep "
                    +
                "\"Retrying attempt\" | awk '{print $2}'",
            coloHelper.getProcessHelper().getUsername(),
            coloHelper.getProcessHelper().getIdentityFile()
        );
        List<String> finalList = new ArrayList<>();
        for (String line : raw) {
            finalList.add(line.split(",")[0]);
        }
        return finalList;
    }

    /**
     * Shuts down falcon server on a given host using sudo credentials.
     * @param helper given host
     * @throws IOException
     * @throws JSchException
     */
    public static void shutDownService(AbstractEntityHelper helper)
        throws IOException, JSchException {
        ExecUtil.runRemoteScriptAsSudo(helper.getQaHost(), helper.getUsername(),
            helper.getPassword(), helper.getServiceStopCmd(),
            helper.getServiceUser(), helper.getIdentityFile());
        TimeUtil.sleepSeconds(10);
    }

    /**
     * Start falcon server on a given host using sudo credentials and checks if it succeeds.
     * @param helper given host
     * @throws IOException
     * @throws JSchException
     * @throws AuthenticationException
     * @throws URISyntaxException
     */
    public static void startService(AbstractEntityHelper helper)
        throws IOException, JSchException, AuthenticationException, URISyntaxException,
            InterruptedException {
        ExecUtil.runRemoteScriptAsSudo(helper.getQaHost(), helper.getUsername(),
            helper.getPassword(), helper.getServiceStartCmd(), helper.getServiceUser(),
            helper.getIdentityFile());
        int statusCode = 0;
        for (int tries = 20; tries > 0; tries--) {
            try {
                statusCode = Util.sendRequest(helper.getHostname(), "get").getCode();
            } catch (IOException e) {
                LOGGER.info(e.getMessage());
            }
            if (statusCode == 200) {
                return;
            }
            TimeUtil.sleepSeconds(5);
        }
        throw new RuntimeException("Service on" + helper.getHostname() + " did not start!");
    }

    /**
     * Stops and starts falcon service for a given host using sudo credentials.
     * @param helper given host
     * @throws IOException
     * @throws JSchException
     * @throws AuthenticationException
     * @throws URISyntaxException
     */
    public static void restartService(AbstractEntityHelper helper)
        throws IOException, JSchException, AuthenticationException, URISyntaxException,
        InterruptedException {
        LOGGER.info("restarting service for: " + helper.getQaHost());
        shutDownService(helper);
        startService(helper);
    }

    /**
     * Prints JMSConsumer messages content.
     * @param messageConsumer the source JMSConsumer
     * @throws JMSException
     */
    public static void printMessageData(JmsMessageConsumer messageConsumer) throws JMSException {
        LOGGER.info("dumping all queue data:");
        for (MapMessage mapMessage : messageConsumer.getReceivedMessages()) {
            StringBuilder stringBuilder = new StringBuilder();
            final Enumeration mapNames = mapMessage.getMapNames();
            while (mapNames.hasMoreElements()) {
                final String propName = mapNames.nextElement().toString();
                final String propValue = mapMessage.getString(propName);
                stringBuilder.append(propName).append('=').append(propValue).append(' ');
            }
            LOGGER.info(stringBuilder);
        }
    }

    /**
     * Get entity type according to its definition.
     * @param entity entity which is under analysis
     * @return entity type
     */
    public static EntityType getEntityType(String entity) {
        if (entity.contains("uri:falcon:process:0.1")) {
            return EntityType.PROCESS;
        } else if (entity.contains("uri:falcon:cluster:0.1")) {
            return EntityType.CLUSTER;
        } else if (entity.contains("uri:falcon:feed:0.1")) {
            return EntityType.FEED;
        }
        return null;
    }

    /**
     * Compares two definitions.
     * @param server1 server where 1st definition is stored
     * @param server2 server where 2nd definition is stored
     * @param entity entity which is under analysis
     * @return are definitions identical
     */
    public static boolean isDefinitionSame(ColoHelper server1, ColoHelper server2,
                                           String entity)
        throws URISyntaxException, IOException, AuthenticationException, JAXBException,
        SAXException, InterruptedException {
        return XmlUtil.isIdentical(getEntityDefinition(server1, entity, true),
            getEntityDefinition(server2, entity, true));
    }

    /**
     * enums used for instance api.
     */
    public enum URLS {
        LIST_URL("/api/entities/list"),
        SUBMIT_URL("/api/entities/submit"),
        GET_ENTITY_DEFINITION("/api/entities/definition"),
        DEPENDENCIES("/api/entities/dependencies"),
        DELETE_URL("/api/entities/delete"),
        SCHEDULE_URL("/api/entities/schedule"),
        VALIDATE_URL("/api/entities/validate"),
        SUSPEND_URL("/api/entities/suspend"),
        RESUME_URL("/api/entities/resume"),
        UPDATE("/api/entities/update"),
        STATUS_URL("/api/entities/status"),
        ENTITY_SUMMARY("/api/entities/summary"),
        SUBMIT_AND_SCHEDULE_URL("/api/entities/submitAndSchedule"),
        INSTANCE_RUNNING("/api/instance/running"),
        INSTANCE_STATUS("/api/instance/status"),
        INSTANCE_KILL("/api/instance/kill"),
        INSTANCE_RESUME("/api/instance/resume"),
        INSTANCE_SUSPEND("/api/instance/suspend"),
        INSTANCE_RERUN("/api/instance/rerun"),
        INSTANCE_SUMMARY("/api/instance/summary"),
        INSTANCE_PARAMS("/api/instance/params"),
        INSTANCE_LIST("/api/instance/list"),
        INSTANCE_LISTING("/api/instance/listing"),
        INSTANCE_LOGS("/api/instance/logs"),
        TOUCH_URL("/api/entities/touch");

        private final String url;

        URLS(String url) {
            this.url = url;
        }

        public String getValue() {
            return this.url;
        }
    }

    /**
     * @param pathString whole path.
     * @return path to basic data folder
     */
    public static String getPathPrefix(String pathString) {
        return pathString.substring(0, pathString.indexOf('$'));
    }

    /**
     * @param path whole path.
     * @return file name which is retrieved from a path
     */
    public static String getFileNameFromPath(String path) {
        return path.substring(path.lastIndexOf('/') + 1, path.length());
    }

    /**
     * Defines request type according to request url.
     * @param url request url
     * @return request type
     */
    public static String getMethodType(String url) {
        List<String> postList = new ArrayList<>();
        postList.add("/entities/validate");
        postList.add("/entities/submit");
        postList.add("/entities/submitAndSchedule");
        postList.add("/entities/suspend");
        postList.add("/entities/resume");
        postList.add("/instance/kill");
        postList.add("/instance/suspend");
        postList.add("/instance/resume");
        postList.add("/instance/rerun");
        for (String item : postList) {
            if (url.toLowerCase().contains(item)) {
                return "post";
            }
        }
        List<String> deleteList = new ArrayList<>();
        deleteList.add("/entities/delete");
        for (String item : deleteList) {
            if (url.toLowerCase().contains(item)) {
                return "delete";
            }
        }
        return "get";
    }

    /**
     * Prints xml in readable form.
     * @param xmlString xmlString
     * @return formatted xmlString
     */
    public static String prettyPrintXml(final String xmlString) {
        if (xmlString == null) {
            return null;
        }
        try {
            Source xmlInput = new StreamSource(new StringReader(xmlString));
            StringWriter stringWriter = new StringWriter();
            StreamResult xmlOutput = new StreamResult(stringWriter);
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            transformerFactory.setAttribute("indent-number", "2");
            Transformer transformer = transformerFactory.newTransformer();
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            transformer.transform(xmlInput, xmlOutput);
            return xmlOutput.getWriter().toString();
        } catch (TransformerConfigurationException e) {
            return xmlString;
        } catch (TransformerException e) {
            return xmlString;
        }
    }

    /**
     * Converts json string to readable form.
     * @param jsonString json string
     * @return formatted string
     */
    public static String prettyPrintJson(final String jsonString) {
        if (jsonString == null) {
            return null;
        }
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        JsonElement json = new JsonParser().parse(jsonString);
        return gson.toJson(json);
    }

    /**
     * Prints xml or json in pretty and readable format.
     * @param str xml or json string
     * @return converted xml or json
     */
    public static String prettyPrintXmlOrJson(final String str) {
        if (str == null) {
            return null;
        }
        String cleanStr = str.trim();
        //taken from http://stackoverflow.com/questions/7256142/way-to-quickly-check-if-string-is-xml-or-json-in-c-sharp
        if (cleanStr.startsWith("{") || cleanStr.startsWith("[")) {
            return prettyPrintJson(cleanStr);
        }
        if (cleanStr.startsWith("<")) {
            return prettyPrintXml(cleanStr);
        }
        LOGGER.warn("The string does not seem to be either json or xml: " + cleanStr);
        return str;
    }

    /**
     * Tries to get entity definition.
     * @param cluster cluster where definition is stored
     * @param entity entity for which definition is required
     * @param shouldReturn should the definition be successfully retrieved or not
     * @return entity definition
     */
    public static String getEntityDefinition(ColoHelper cluster,
                                             String entity,
                                             boolean shouldReturn) throws
            JAXBException,
            IOException, URISyntaxException, AuthenticationException, InterruptedException {
        EntityType type = getEntityType(entity);
        AbstractEntityHelper helper;
        if (EntityType.PROCESS == type) {
            helper = cluster.getProcessHelper();
        } else if (EntityType.FEED == type) {
            helper = cluster.getFeedHelper();
        } else {
            helper = cluster.getClusterHelper();
        }
        ServiceResponse response = helper.getEntityDefinition(entity);
        if (shouldReturn) {
            AssertUtil.assertSucceeded(response);
        } else {
            AssertUtil.assertFailed(response);
        }
        String result = response.getMessage();
        Assert.assertNotNull(result);
        return result;
    }
}
