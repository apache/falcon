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

package org.apache.falcon.extensions;

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.extensions.mirroring.hdfs.HdfsMirroringExtension;
import org.apache.falcon.extensions.mirroring.hdfsSnapshot.HdfsSnapshotMirroringExtension;
import org.apache.falcon.extensions.mirroring.hive.HiveMirroringExtension;
import org.apache.falcon.util.ReplicationDistCpOption;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Abstract extension class to be extended by all trusted and custom extensions.
 */
public abstract class AbstractExtension {
    private static final List<String> TRUSTED_EXTENSIONS = Arrays.asList(
            new HdfsMirroringExtension().getName().toUpperCase(),
            new HdfsSnapshotMirroringExtension().getName().toUpperCase(),
            new HiveMirroringExtension().getName().toUpperCase());
    private static List<AbstractExtension> extensions = new ArrayList<>();

    public static List<AbstractExtension> getExtensions() {
        if (extensions.isEmpty()) {
            extensions.add(new HdfsMirroringExtension());
            extensions.add(new HdfsSnapshotMirroringExtension());
            extensions.add(new HiveMirroringExtension());
        }
        return extensions;
    }

    public static boolean isExtensionTrusted(final String extensionName) {
        return TRUSTED_EXTENSIONS.contains(extensionName.toUpperCase());
    }

    /* Name cannot be null */
    public abstract String getName();

    public abstract void validate(final Properties extensionProperties) throws FalconException;

    public abstract Properties getAdditionalProperties(final Properties extensionProperties) throws FalconException;

    public static void addAdditionalDistCPProperties(final Properties extensionProperties,
                                                      final Properties additionalProperties) {
        for (ReplicationDistCpOption distcpOption : ReplicationDistCpOption.values()) {
            if (StringUtils.isBlank(
                    extensionProperties.getProperty(distcpOption.getName()))) {
                additionalProperties.put(distcpOption.getName(), "false");
            }
        }
    }
}

