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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Helper class loader that fetches jars from HDFS location and loads into JVM.
 */

public class HdfsClassLoader extends URLClassLoader {

    private static final Logger LOG = LoggerFactory.getLogger(HdfsClassLoader.class);
    private static Map<String, HdfsClassLoader>  classLoaderCache = new ConcurrentHashMap<>();
    private static final Object LOCK = new Object();

    public static ClassLoader load(final String name, final List<String> jarHdfsPath) throws IOException {
        LOG.info("ClassLoader cache size = " + classLoaderCache.size());
        if (classLoaderCache.containsKey(name)) {
            return classLoaderCache.get(name);
        }

        synchronized (LOCK) {
            final URL[] urls = copyHdfsJarFilesToTempDir(name, jarHdfsPath);
            LOG.info("Copied jar files from HDFS to local dir");
            final ClassLoader parentClassLoader = HdfsClassLoader.class.getClassLoader();
            HdfsClassLoader hdfsClassLoader = java.security.AccessController.doPrivileged(
                    new java.security.PrivilegedAction<HdfsClassLoader>() {
                        @Override
                        public HdfsClassLoader run() {
                            return new HdfsClassLoader(name, urls, parentClassLoader);
                        }
                    }
            );
            LOG.info("Created a new HdfsClassLoader for name = {} with parent = {} using classpath = {}",
                    name, parentClassLoader.toString(),  Arrays.toString(jarHdfsPath.toArray()));
            classLoaderCache.put(name, hdfsClassLoader);
            return hdfsClassLoader;
        }
    }

    private final ClassLoader realParent;

    public HdfsClassLoader(String name, URL[] urls, ClassLoader parentClassLoader) {
        // set the 'parent' member to null giving an option for this class loader
        super(urls, null);
        this.realParent = parentClassLoader;
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve)
        throws ClassNotFoundException {

        // Load through the parent class loader first and then fallback to this class loader.
        try {
            return realParent.loadClass(name);
        } catch (Throwable t) {
            return super.loadClass(name, resolve);
        }
    }

    @Override
    public URL getResource(String name) {
        // This is the same as the jdk's getResource except the parent
        // is taken from the realParent member instead of the parent member.
        URL url = realParent.getResource(name);
        if (url == null) {
            url = findResource(name);
        }
        return url;
    }

    private static URL[] copyHdfsJarFilesToTempDir(String databaseName, List<String> jars) throws IOException {
        List<URL> urls = new ArrayList<URL>();

        final Configuration conf = new Configuration();
        Path localPath = createTempDir(databaseName, conf);

        for (String jar : jars) {
            Path jarPath = new Path(jar);
            final FileSystem fs = jarPath.getFileSystem(conf);
            if (fs.isFile(jarPath) && jarPath.getName().endsWith(".jar")) {
                LOG.info("Copying jarFile = " + jarPath);
                fs.copyToLocalFile(jarPath, localPath);
            }
        }
        urls.addAll(getJarsInPath(localPath.toUri().toURL()));

        return urls.toArray(new URL[urls.size()]);
    }

    private static Path createTempDir(String databaseName, Configuration conf) throws IOException {
        String tmpBaseDir = String.format("file://%s", System.getProperty("java.io.tmpdir"));
        if (StringUtils.isBlank(tmpBaseDir)) {
            tmpBaseDir = "file:///tmp";
        }
        Path localPath = new Path(tmpBaseDir, databaseName);
        localPath.getFileSystem(conf).mkdirs(localPath);
        return localPath;
    }

    private static List<URL> getJarsInPath(URL fileURL) throws MalformedURLException {
        List<URL> urls = new ArrayList<URL>();

        File file = new File(fileURL.getPath());
        if (file.isDirectory()) {
            File[] jarFiles = file.listFiles(new FileFilter() {
                @Override
                public boolean accept(File file) {
                    return file.isFile() && file.getName().endsWith(".jar");
                }
            });

            if (jarFiles != null) {
                for (File jarFile : jarFiles) {
                    urls.add(jarFile.toURI().toURL());
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
