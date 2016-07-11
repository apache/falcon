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

import org.apache.commons.io.FileUtils;
import org.apache.falcon.extensions.AbstractExtension;
import org.apache.falcon.extensions.ExtensionService;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.util.StartupProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.AfterClass;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;

/**
 *  Abstract class to setup extension store.
*/
public class AbstractTestExtensionStore {

    protected String extensionStorePath;
    protected ExtensionStore store;
    private FileSystem fileSystem;

    public void initExtensionStore() throws Exception {
        initExtensionStore(this.getClass());
    }

    public void initExtensionStore(Class resourceClass) throws Exception {
        new ExtensionService().init();
        store = ExtensionService.getExtensionStore();
        fileSystem = HadoopClientFactory.get().createFalconFileSystem(new Configuration(true));
        extensionStorePath = new URI(StartupProperties.get().getProperty(ExtensionStore.EXTENSION_STORE_URI)).getPath();
        extensionStoreSetup(resourceClass);
    }

    private void extensionStoreSetup(Class resourceClass) throws IOException {
        List<AbstractExtension> extensions = AbstractExtension.getExtensions();
        for (AbstractExtension extension : extensions) {
            String extensionName = extension.getName().toLowerCase();
            Path extensionPath = new Path(extensionStorePath, extensionName);
            Path libPath = new Path(extensionPath, "libs");
            Path resourcesPath = new Path(extensionPath, "resources");
            HadoopClientFactory.mkdirs(fileSystem, extensionPath,
                    HadoopClientFactory.READ_EXECUTE_PERMISSION);
            HadoopClientFactory.mkdirs(fileSystem, new Path(extensionPath, new Path("README")),
                    HadoopClientFactory.READ_EXECUTE_PERMISSION);

            HadoopClientFactory.mkdirs(fileSystem, libPath,
                    HadoopClientFactory.READ_EXECUTE_PERMISSION);
            HadoopClientFactory.mkdirs(fileSystem, new Path(libPath, "build"),
                    HadoopClientFactory.READ_EXECUTE_PERMISSION);
            HadoopClientFactory.mkdirs(fileSystem, new Path(libPath, "runtime"),
                    HadoopClientFactory.READ_EXECUTE_PERMISSION);

            HadoopClientFactory.mkdirs(fileSystem, resourcesPath,
                    HadoopClientFactory.READ_EXECUTE_PERMISSION);
            HadoopClientFactory.mkdirs(fileSystem, new Path(resourcesPath, "build"),
                    HadoopClientFactory.READ_EXECUTE_PERMISSION);
            Path runTimeResourcePath = new Path(resourcesPath, "runtime");
            HadoopClientFactory.mkdirs(fileSystem, runTimeResourcePath,
                    HadoopClientFactory.READ_EXECUTE_PERMISSION);

            fileSystem.create(new Path(runTimeResourcePath, extensionName + "-workflow.xml"));
            Path dstFile = new Path(runTimeResourcePath, extensionName + "-template.xml");
            fileSystem.create(dstFile);
            String srcFile = extensionName + "-template.xml";
            fileSystem.copyFromLocalFile(new Path(getAbsolutePath(resourceClass, srcFile)), dstFile);
        }

    }

    private String getAbsolutePath(Class resourceClass, String fileName) {
        return resourceClass.getResource("/" + fileName).getPath();
    }


    @AfterClass
    public void cleanUp() throws Exception {
        FileUtils.deleteDirectory(new File(extensionStorePath));
    }
}
