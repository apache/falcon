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

import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * Test Class for validating Extension Class Loader.
 */
public class ExtensionClassLoaderTest {
    public static final String JARS_DIR = "file:///" + System.getProperty("user.dir") + "/src/test/resources";

    @Test
    public void testManuallyLoadedClass() throws Exception{

        List<URL> urls = new ArrayList<>();

        urls.addAll(ExtensionHandler.getFilesInPath(new Path(JARS_DIR).toUri().toURL()));

        ClassLoader loader = ExtensionClassLoader.load(urls);
        ClassLoader previousClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(loader);
        Class<?> classManuallyLoaded = loader.loadClass("org.apache.falcon.ExtensionExample");

        Object exampleExtension = classManuallyLoaded.newInstance();

        Method methodToString = classManuallyLoaded.getMethod("toString", String.class);

        Thread.currentThread().setContextClassLoader(previousClassLoader);
        Assert.assertEquals("test", methodToString.invoke(exampleExtension, "test"));
    }
}
