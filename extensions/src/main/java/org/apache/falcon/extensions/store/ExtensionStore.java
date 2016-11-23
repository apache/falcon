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

package org.apache.falcon.extensions.store;

import org.apache.commons.lang.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.extensions.AbstractExtension;
import org.apache.falcon.extensions.ExtensionType;
import org.apache.falcon.extensions.jdbc.ExtensionMetaStore;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.entity.parser.ValidationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IOUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.falcon.util.StartupProperties;
import org.apache.falcon.entity.store.StoreAccessException;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Store for Falcon extensions.
 */
public final class ExtensionStore {

    private static final Logger LOG = LoggerFactory.getLogger(ExtensionStore.class);

    public static ExtensionMetaStore getMetaStore() {
        return metaStore;
    }

    private static ExtensionMetaStore metaStore = new ExtensionMetaStore();
    private FileSystem fs;

    private Path storePath;

    private static final String EXTENSION_PROPERTY_JSON_SUFFIX = "-properties.json";
    private static final String SHORT_DESCRIPTION = "shortDescription";

    // Convention over configuration design paradigm
    private static final String RESOURCES_DIR = "resources";
    private static final String LIBS_DIR = "libs";

    public static final String EXTENSION_STORE_URI = "extension.store.uri";

    private static final ExtensionStore STORE = new ExtensionStore();


    public static ExtensionStore get() {
        return STORE;
    }

    private ExtensionStore() {
        String uri = StartupProperties.get().getProperty(EXTENSION_STORE_URI);
        if (StringUtils.isEmpty(uri)) {
            throw new RuntimeException("Property extension.store.uri not set in startup properties."
                    + "Please set it to path of extension deployment on HDFS. Extension store init failed");
        }
        storePath = new Path(uri);
        fs = initializeFileSystem();
        initializeDbTable();
    }

    private void initializeDbTable() {
        try{
            metaStore.deleteExtensionsOfType(ExtensionType.TRUSTED);
            List<String> extensions = getExtensions();
            for (String extension : extensions) {
                ExtensionType extensionType = AbstractExtension.isExtensionTrusted(extension)
                        ? ExtensionType.TRUSTED : ExtensionType.CUSTOM;
                String description = getShortDescription(extension);
                String recipeName = extension;
                String location = storePath.toString() + '/' + extension;
                metaStore.storeExtensionMetadataBean(recipeName, location, extensionType, description);
            }
        } catch (FalconException e){
            LOG.error("Exception in ExtensionStore:", e);
            throw new RuntimeException(e);
        }

    }

    private String getShortDescription(final String extensionName) throws FalconException {
        String content = getResource(extensionName, extensionName.toLowerCase()
                + EXTENSION_PROPERTY_JSON_SUFFIX);
        String description;
        try {
            JSONObject jsonObject = new JSONObject(content);
            description = (String) jsonObject.get(SHORT_DESCRIPTION);
        } catch (JSONException e) {
            throw new FalconException(e);
        }
        return description;
    }

    private FileSystem initializeFileSystem() {
        try {
            FileSystem fileSystem =
                    HadoopClientFactory.get().createFalconFileSystem(storePath.toUri());
            if (!fileSystem.exists(storePath)) {
                LOG.info("Creating extension store directory: {}", storePath);
                // set permissions so config store dir is owned by falcon alone
                HadoopClientFactory.mkdirs(fileSystem, storePath, HadoopClientFactory.ALL_PERMISSION);
            }
            return fileSystem;
        } catch (Exception e) {
            throw new RuntimeException("Unable to bring up extension store for path: " + storePath, e);
        }
    }

    public Map<String, String> getExtensionArtifacts(final String extensionName) throws StoreAccessException {
        Map<String, String> extensionFileMap = new HashMap<>();
        try {
            Path extensionPath = new Path(storePath, extensionName.toLowerCase());
            RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(extensionPath, true);

            if (!fileStatusListIterator.hasNext()) {
                throw new StoreAccessException(new Exception(" For extension " + extensionName
                        + " there are no artifacts at the extension store path " + storePath));
            }
            while (fileStatusListIterator.hasNext()) {
                LocatedFileStatus fileStatus = fileStatusListIterator.next();
                Path filePath = Path.getPathWithoutSchemeAndAuthority(fileStatus.getPath());
                extensionFileMap.put(filePath.getName(), filePath.toString());
            }
        } catch (IOException e) {
            throw new StoreAccessException(e);
        }
        return extensionFileMap;
    }

    public Map<String, String> getExtensionResources(final String extensionName) throws StoreAccessException {
        Map<String, String> extensionFileMap = new HashMap<>();
        try {
            Path extensionPath = new Path(storePath, extensionName.toLowerCase());

            Path resourcesPath = null;
            FileStatus[] files = fs.listStatus(extensionPath);

            for (FileStatus fileStatus : files) {
                if (fileStatus.getPath().getName().equalsIgnoreCase(RESOURCES_DIR)) {
                    resourcesPath = fileStatus.getPath();
                    break;
                }
            }

            if (resourcesPath == null) {
                throw new StoreAccessException(new Exception(" For extension " + extensionName
                        + " there is no " + RESOURCES_DIR + "at the extension store path " + storePath));
            }
            RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(resourcesPath, true);
            while (fileStatusListIterator.hasNext()) {
                LocatedFileStatus fileStatus = fileStatusListIterator.next();
                Path filePath = Path.getPathWithoutSchemeAndAuthority(fileStatus.getPath());
                extensionFileMap.put(filePath.getName(), filePath.toString());
            }
        } catch (IOException e) {
            throw new StoreAccessException(e);
        }
        return extensionFileMap;
    }

    public String getExtensionLibPath(final String extensionName) throws StoreAccessException {
        try {
            Path extensionPath = new Path(storePath, extensionName.toLowerCase());

            Path libsPath = null;
            FileStatus[] files = fs.listStatus(extensionPath);

            for (FileStatus fileStatus : files) {
                if (fileStatus.getPath().getName().equalsIgnoreCase(LIBS_DIR)) {
                    libsPath = Path.getPathWithoutSchemeAndAuthority(fileStatus.getPath());
                    break;
                }
            }

            if (libsPath == null) {
                LOG.info("For extension " + extensionName + " there is no "
                        + LIBS_DIR + "at the extension store path " + extensionPath);
                return null;
            } else {
                return libsPath.toString();
            }
        } catch (IOException e) {
            throw new StoreAccessException(e);
        }
    }

    public String getExtensionResource(final String resourcePath) throws StoreAccessException {
        if (StringUtils.isBlank(resourcePath)) {
            throw new StoreAccessException(new Exception("Resource path cannot be null or empty"));
        }

        try {
            Path resourceFile = new Path(resourcePath);

            ByteArrayOutputStream writer = new ByteArrayOutputStream();
            InputStream data = fs.open(resourceFile);
            IOUtils.copyBytes(data, writer, fs.getConf(), true);
            return writer.toString();
        } catch (IOException e) {
            throw new StoreAccessException(e);
        }
    }

    public List<String> getExtensions() throws StoreAccessException {
        List<String> extesnionList = new ArrayList<>();
        try {
            FileStatus[] fileStatuses = fs.listStatus(storePath);

            for (FileStatus fileStatus : fileStatuses) {
                if (fileStatus.isDirectory()) {
                    Path filePath = Path.getPathWithoutSchemeAndAuthority(fileStatus.getPath());
                    extesnionList.add(filePath.getName());
                }
            }
        } catch (IOException e) {
            throw new StoreAccessException(e);
        }
        return extesnionList;
    }
    public String deleteExtensionMetadata(final String extensionName) throws ValidationException{
        ExtensionType extensionType = AbstractExtension.isExtensionTrusted(extensionName) ? ExtensionType.TRUSTED
                : ExtensionType.CUSTOM;
        if (extensionType.equals(ExtensionType.TRUSTED)){
            throw new ValidationException(extensionName + " is trusted cannot be deleted.");
        }
        if (metaStore.checkIfExtensionExists(extensionName)) {
            metaStore.deleteExtensionMetadata(extensionName);
            return "Deleted extension:" + extensionName;
        }else {
            return "Extension:" + extensionName + " is not registered with Falcon.";
        }
    }

    public String registerExtensionMetadata(final String extensionName, final String path, final String description)
        throws URISyntaxException, FalconException {
        Configuration conf = new Configuration();
        URI uri = new URI(path);
        conf.set("fs.default.name", uri.getScheme() + "://" + uri.getAuthority());
        FileSystem fileSystem =  HadoopClientFactory.get().createFalconFileSystem(uri);
        try {
            fileSystem.listStatus(new Path(uri.getPath() + "/README"));
        } catch (IOException e){
            LOG.error("Exception in registerExtensionMetadata:", e);
            throw new ValidationException("README file is not present in the " + path);
        }
        PathFilter filter=new PathFilter(){
            public boolean accept(Path file){
                return file.getName().endsWith(".jar");
            }
        };
        FileStatus[] jarStatus;
        try {
            jarStatus = fileSystem.listStatus(new Path(uri.getPath() + "/libs/build"), filter);
            if (jarStatus.length <=0) {
                throw new ValidationException("Jars are not present in the " + uri.getPath() + "libs/build.");
            }
        } catch (IOException e){
            LOG.error("Exception in registerExtensionMetadata:", e);
            throw new ValidationException("Jars are not present in the " + uri.getPath() + "libs/build.");
        }
        FileStatus[] propStatus;
        try{
            propStatus = fileSystem.listStatus(new Path(uri.getPath() + "/META"));
            if (propStatus.length <=0){
                throw new ValidationException("No properties file is not present in the " + uri.getPath() + "/META"
                        + " structure.");
            }
        } catch (IOException e){
            LOG.error("Exception in registerExtensionMetadata:", e);
            throw new ValidationException("Directory is not present in the " + uri.getPath() + "/META"
                    + " structure.");
        }

        if (!metaStore.checkIfExtensionExists(extensionName)){
            metaStore.storeExtensionMetadataBean(extensionName, path, ExtensionType.CUSTOM, description);
        }else{
            throw new ValidationException(extensionName + " already exsists.");
        }
        return "Extension :" + extensionName + " registered succesfully.";
    }

    public String getResource(final String extensionName, final String resourceName) throws StoreAccessException {
        Map<String, String> resources = getExtensionArtifacts(extensionName);
        if (resources.isEmpty()) {
            throw new StoreAccessException(new Exception("No extension resources found for " + extensionName));
        }

        return getExtensionResource(resources.get(resourceName));
    }

    public Path getExtensionStorePath() {
        return storePath;
    }

    public boolean isExtensionStoreInitialized() {
        return (storePath != null);
    }

}
