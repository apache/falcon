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
package org.apache.falcon.designer.storage.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.falcon.designer.storage.Storage;
import org.apache.falcon.designer.storage.StorageException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Storage implementation to store in HDFS.
 *
 */
public class HDFSStorage implements Storage {

    private FileSystem fs;
    private String basePath;
    private static final String SEPERATOR = "/";
    private static final String BASEPATH_CONFIG_NAME =
        "falcon.designer.hdfsstorage.defaultpath";

    HDFSStorage(Configuration conf) throws StorageException {
        try {
            this.fs = FileSystem.get(conf);
        } catch (IOException e) {
            throw new StorageException(e);
        }
        this.basePath = conf.get(BASEPATH_CONFIG_NAME);
        if (this.basePath == null || this.basePath.isEmpty()) {
            throw new StorageException(BASEPATH_CONFIG_NAME
                + " cannot be empty");
        }

    }

    @Override
    public InputStream open(String namespace, String entity)
        throws StorageException {
        try {
            return fs.open(new Path(basePath + SEPERATOR + namespace
                + SEPERATOR + entity));

        } catch (IllegalArgumentException e) {
            throw new StorageException(e);
        } catch (IOException e) {
            throw new StorageException(e);
        }

    }

    @Override
    public OutputStream create(String namespace, String entity)
        throws StorageException {
        try {
            return fs.create(new Path(basePath + SEPERATOR + namespace
                + SEPERATOR + entity));
        } catch (IllegalArgumentException e) {
            throw new StorageException(e);
        } catch (IOException e) {
            throw new StorageException(e);
        }

    }

    @Override
    public void delete(String namespace, String entity) throws StorageException {
        try {
            fs.delete(new Path(basePath + SEPERATOR + namespace + SEPERATOR
                + entity), true);
        } catch (IllegalArgumentException e) {
            throw new StorageException(e);
        } catch (IOException e) {
            throw new StorageException(e);
        }

    }

}
