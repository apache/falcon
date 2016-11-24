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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

/**
 * Helper methods to prepare Extension entities.
 */
public final class ExtensionUtil {
    private ExtensionUtil() {}

    public static final Logger LOG = LoggerFactory.getLogger(ExtensionUtil.class);
    private static final String UTF_8 = CharEncoding.UTF_8;
    private static final String TMP_BASE_DIR = String.format("file://%s", System.getProperty("java.io.tmpdir"));

    public static List<Entity> getEntities(ClassLoader extensionClassloader, String extensionName, String jobName,
                                           InputStream configStream) throws IOException, FalconException {
        ClassLoader previousClassLoader = Thread.currentThread().getContextClassLoader();
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
            Class<ExtensionBuilder> clazz = (Class<ExtensionBuilder>) extensionClassloader
                    .loadClass(result.get(0).getCanonicalName());
            extensionBuilder = clazz.newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new FalconCLIException("Failed to instantiate extension implementation " + extensionName, e);
        }

        extensionBuilder.validateExtensionConfig(extensionName, configStream);
        List<Entity> entities = extensionBuilder.getEntities(jobName, configStream);

        Thread.currentThread().setContextClassLoader(previousClassLoader);
        return entities;
    }

    public static List<Entity> loadAndPrepare(String extensionName, String jobName, InputStream configStream,
                                              String extensionBuildLocation) throws IOException, FalconException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        String stagePath = createStagePath(extensionName, jobName);
        List<URL> urls = ExtensionUtil.copyExtensionPackage(extensionBuildLocation, fs, stagePath);

        List<Entity> entities = prepare(extensionName, jobName, configStream, urls);
        ExtensionUtil.stageEntities(entities, stagePath);
        return entities;
    }

    public static List<Entity> prepare(String extensionName, String jobName, InputStream configStream, List<URL> urls)
        throws IOException, FalconException {
        ClassLoader extensionClassLoader = ExtensionClassLoader.load(urls);
        return ExtensionUtil.getEntities(extensionClassLoader, extensionName, jobName, configStream);
    }

    // This method is only for debugging, the staged entities can be found in /tmp path.
    public static void stageEntities(List<Entity> entities, String stagePath)
        throws IOException, FalconException {
        File entityFile;
        EntityType type;
        for (Entity entity : entities) {
            type = entity.getEntityType();
            entityFile = new File(stagePath + File.separator + entity.getEntityType().toString() + "_"
                    + URLEncoder.encode(entity.getName(), UTF_8));
            if (!entityFile.createNewFile()) {
                throw new FalconCLIException("Failed to create the file" + entityFile.toString());
            }
            OutputStream out = new FileOutputStream(entityFile);
            try {
                type.getMarshaller().marshal(entity, out);
                LOG.debug("Staged configuration {}/{}", type, entity.getName());
            } catch (JAXBException e) {
                LOG.error("Unable to serialize the entity object {}/{}", type, entity.getName(), e);
                throw new FalconException("Error in staging entity: " + entity.getName() + "to "
                        + entityFile.toString());
            } finally {
                out.close();
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

    public static List<URL> copyExtensionPackage(String extensionBuildUrl, FileSystem fs, String stagePath)
        throws IOException {

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

        Path metaPath = new Path(extensionBuildUrl, FalconExtensionConstants.META_INF);
        Path metaServicesPath = new Path(metaPath, FalconExtensionConstants.SERVICES);
        Path localMetaServicesPath = new Path(localStagePath, FalconExtensionConstants.RESOURCES);
        LOG.info("Copying meta services from {} to {}", metaServicesPath, localMetaServicesPath);
        fs.copyToLocalFile(metaServicesPath, localMetaServicesPath);


        List<URL> urls = new ArrayList<>();
        urls.addAll(getFilesInPath(localBuildLibsPath.toUri().toURL()));
        urls.add(localBuildResourcesPath.toUri().toURL());
        urls.add(localMetaServicesPath.toUri().toURL());
        return urls;
    }

    public static List<URL> getFilesInPath(URL fileURL) throws MalformedURLException {
        List<URL> urls = new ArrayList<>();

        File file = new File(fileURL.getPath());
        if (file.isDirectory()) {
            File[] files = file.listFiles(new FileFilter() {
                @Override
                public boolean accept(File file) {
                    return file.isFile();
                }
            });

            if (files != null) {
                for (File innerFile : files) {
                    urls.add(innerFile.toURI().toURL());
                }
            }

            if (!fileURL.toString().endsWith("/")) {
                fileURL = new URL(fileURL.toString() + "/");
            }
        }

        urls.add(fileURL);
        return urls;
    }
}
