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

import org.apache.commons.codec.CharEncoding;
import org.apache.falcon.client.FalconCLIException;
import org.apache.falcon.client.FalconExtensionConstants;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.extensions.ExtensionBuilder;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

/**
 * Handler class that is responsible for preparing Extension entities.
 */
public final class ExtensionHandler {

    public static final Logger LOG = LoggerFactory.getLogger(ExtensionHandler.class);
    private static final String UTF_8 = CharEncoding.UTF_8;
    private static final String TMP_BASE_DIR = String.format("file://%s", System.getProperty("java.io.tmpdir"));
    private static final String LOCATION = "location";
    private static final String TYPE = "type";
    private static final String NAME = "extensionName";
    private static final String EXTENSION_BUILDER_INTERFACE_SERVICE_FILE =
            "META-INF/services/org.apache.falcon.extensions.ExtensionBuilder";

    private ExtensionHandler(){}

    private List<Entity> getEntities(ClassLoader extensionClassloader, String extensionName, String jobName,
                                     InputStream configStream) throws IOException, FalconException {
        Thread.currentThread().setContextClassLoader(extensionClassloader);

        ServiceLoader<ExtensionBuilder> extensionBuilders = ServiceLoader.load(ExtensionBuilder.class);

        List<Class<? extends ExtensionBuilder>> result = new ArrayList<>();

        for (ExtensionBuilder extensionBuilder : extensionBuilders) {
            result.add(extensionBuilder.getClass());
        }

        if (result.isEmpty()) {
            throw new FalconException("Extension Implementation not found in the package of : " + extensionName);
        } else if (result.size() > 1) {
            throw new FalconException("Found more than one extension Implementation in the package of : "
                    + extensionName);
        }

        ExtensionBuilder extensionBuilder = null;
        try {
            @SuppressWarnings("unchecked")
            Class<ExtensionBuilder> clazz = (Class<ExtensionBuilder>) extensionClassloader
                    .loadClass(result.get(0).getCanonicalName());
            extensionBuilder = clazz.newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new FalconCLIException("Failed to instantiate extension implementation " + extensionName, e);
        }

        extensionBuilder.validateExtensionConfig(extensionName, configStream);

        return extensionBuilder.getEntities(jobName, configStream);
    }

    public static List<Entity> loadAndPrepare(String extensionName, String jobName, InputStream configStream,
                                              String extensionBuildLocation)
        throws IOException, FalconException, URISyntaxException {
        String stagePath = createStagePath(extensionName, jobName);
        List<URL> urls = ExtensionHandler.copyExtensionPackage(extensionBuildLocation, stagePath);

        List<Entity> entities = prepare(extensionName, jobName, configStream, urls);
        ExtensionHandler.stageEntities(entities, stagePath);
        return entities;
    }

    public static List<Entity> prepare(String extensionName, String jobName, InputStream configStream, List<URL> urls)
        throws IOException, FalconException {
        ClassLoader extensionClassLoader = ExtensionClassLoader.load(urls);
        if (extensionClassLoader.getResourceAsStream(EXTENSION_BUILDER_INTERFACE_SERVICE_FILE) == null) {
            throw new FalconCLIException("The extension build time jars do not contain "
                    + EXTENSION_BUILDER_INTERFACE_SERVICE_FILE);
        }
        ExtensionHandler extensionHandler = new ExtensionHandler();

        return extensionHandler.getEntities(extensionClassLoader, extensionName, jobName, configStream);
    }

    // This method is only for debugging, the staged entities can be found in /tmp path.
    private static void stageEntities(List<Entity> entities, String stagePath) {
        File entityFile;
        EntityType type;
        for (Entity entity : entities) {
            type = entity.getEntityType();
            OutputStream out;
            try {
                entityFile = new File(new Path(stagePath + File.separator + entity.getEntityType().toString() + "_"
                    + URLEncoder.encode(entity.getName(), UTF_8)).toUri().toURL().getPath());
                if (!entityFile.createNewFile()) {
                    LOG.debug("Not able to stage the entities in the tmp path");
                    return;
                }
                out = new FileOutputStream(entityFile);
                type.getMarshaller().marshal(entity, out);
                LOG.debug("Staged configuration {}/{}", type, entity.getName());
                out.close();
            } catch (Exception e) {
                LOG.error("Unable to serialize the entity object {}/{}", type, entity.getName(), e);
            }
        }
    }

    private static String createStagePath(String extensionName, String jobName) {
        String stagePath = TMP_BASE_DIR + File.separator + extensionName + File.separator + jobName
                + File.separator + System.currentTimeMillis()/1000;
        File tmpPath = new File(stagePath);
        if (tmpPath.mkdir()) {
            throw new FalconCLIException("Failed to create stage directory" + tmpPath.toString());
        }
        return stagePath;
    }

    private static List<URL> copyExtensionPackage(String extensionBuildUrl, String stagePath)
        throws IOException, FalconException, URISyntaxException {

        FileSystem fs = HadoopClientFactory.get().createProxiedFileSystem(new URI(extensionBuildUrl));
        Path libsPath = new Path(extensionBuildUrl, FalconExtensionConstants.LIBS);
        Path buildLibsPath = new Path(libsPath, FalconExtensionConstants.BUILD);
        Path localStagePath = new Path(stagePath);
        Path localBuildLibsPath = new Path(localStagePath, FalconExtensionConstants.LIBS);
        LOG.info("Copying build time libs from {} to {}", buildLibsPath, localBuildLibsPath);
        fs.copyToLocalFile(buildLibsPath, localBuildLibsPath);

        Path resourcesPath = new Path(extensionBuildUrl, FalconExtensionConstants.RESOURCES);
        Path buildResourcesPath = new Path(resourcesPath, FalconExtensionConstants.BUILD);
        Path localBuildResourcesPath = new Path(localStagePath, FalconExtensionConstants.RESOURCES);
        LOG.info("Copying build time resources from {} to {}", buildLibsPath, localBuildResourcesPath);
        fs.copyToLocalFile(buildResourcesPath, localBuildResourcesPath);

        List<URL> urls = new ArrayList<>();
        urls.addAll(getFilesInPath(localBuildLibsPath.toUri().toURL()));
        urls.add(localBuildResourcesPath.toUri().toURL());
        return urls;
    }

    static List<URL> getFilesInPath(URL fileURL) throws MalformedURLException {
        List<URL> urls = new ArrayList<>();

        File file = new File(fileURL.getPath());
        if (file.isDirectory()) {
            File[] files = file.listFiles();

            if (files != null) {
                for (File innerFile : files) {
                    if (innerFile.isFile()) {
                        urls.add(innerFile.toURI().toURL());
                    }
                }
            }

            if (!fileURL.toString().endsWith("/")) {
                fileURL = new URL(fileURL.toString() + "/");
            }
        }

        urls.add(fileURL);
        return urls;
    }


    public static String getExtensionLocation(String extensionName, JSONObject extensionDetailJson) {
        String extensionBuildPath;
        try {
            extensionBuildPath = extensionDetailJson.get(LOCATION).toString();
        } catch (JSONException e) {
            throw new FalconCLIException("Failed to get extension location for the given extension:" + extensionName,
                    e);
        }
        return extensionBuildPath;
    }

    public static  String getExtensionType(String extensionName, JSONObject extensionDetailJson) {
        String extensionType;
        try {
            extensionType = extensionDetailJson.get(TYPE).toString();
        } catch (JSONException e) {
            throw new FalconCLIException("Failed to get extension type for the given extension:" + extensionName, e);
        }
        return extensionType;
    }

    public static  String getExtensionName(String jobName, JSONObject extensionJobDetailJson) {
        String extensionType;
        try {
            extensionType = extensionJobDetailJson.get(NAME).toString();
        } catch (JSONException e) {
            throw new FalconCLIException("Failed to get extension name for the given extension job:" + jobName, e);
        }
        return extensionType;
    }
}
