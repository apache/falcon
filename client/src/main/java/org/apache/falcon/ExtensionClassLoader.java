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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.List;

/**
 * Helper class loader that fetches jars from local disk and loads into JVM.
 */

public class ExtensionClassLoader extends URLClassLoader{

    public static final Logger LOG = LoggerFactory.getLogger(ExtensionClassLoader.class);

    public ExtensionClassLoader(URL[] urls, ClassLoader parent) {
        super(urls, parent);
    }

    public static ClassLoader load(final List<URL> urls) throws IOException {
        final ClassLoader parentClassLoader = ExtensionClassLoader.class.getClassLoader();
        ClassLoader extensionClassLoader = java.security.AccessController.doPrivileged(
                new java.security.PrivilegedAction<ExtensionClassLoader>() {
                    @Override
                    public ExtensionClassLoader run() {
                        return new ExtensionClassLoader(urls.toArray(new URL[urls.size()]), parentClassLoader);
                    }
                }
        );
        LOG.info("Created a new ExtensionClassLoader using classpath = {}", Arrays.toString(urls.toArray()));
        return extensionClassLoader;
    }

    @Override
    protected void addURL(URL url) {
        super.addURL(url);
    }

}
