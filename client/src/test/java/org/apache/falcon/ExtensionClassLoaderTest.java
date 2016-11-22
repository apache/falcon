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

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;

/**
 * Test Class for validating Extension Class Loader.
 */
public class ExtensionClassLoaderTest {
    public static final String JARS_DIR = "file:///" + System.getProperty("user.dir") + "/src/test/resources";

    @Test
    public void testManuallyLoadedClass() throws Exception{
        File path = new File(JARS_DIR);
        URL[] urls = {new Path(path.toString()).toUri().toURL()};
        ExtensionClassLoader loader = new ExtensionClassLoader(urls, Thread.currentThread().getContextClassLoader());
        Class<?> classManuallyLoaded = loader.loadClass("org.apache.falcon.ExtensionExample");

        Object myBeanInstanceFromReflection = classManuallyLoaded.newInstance();

        Method methodToString = classManuallyLoaded.getMethod("toString", String.class);

        Assert.assertEquals("test", methodToString.invoke(myBeanInstanceFromReflection, "test"));
    }
}
