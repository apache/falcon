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

/**
 * enum for DistCp options.
 */
public enum ReplicationDistCpOption {

    DISTCP_OPTION_OVERWRITE("overwrite"),
    DISTCP_OPTION_IGNORE_ERRORS("ignoreErrors"),
    DISTCP_OPTION_SKIP_CHECKSUM("skipChecksum"),
    DISTCP_OPTION_REMOVE_DELETED_FILES("removeDeletedFiles"),
    DISTCP_OPTION_PRESERVE_BLOCK_SIZE("preserveBlockSize"),
    DISTCP_OPTION_PRESERVE_REPLICATION_NUMBER("preserveReplicationNumber"),
    DISTCP_OPTION_PRESERVE_PERMISSIONS("preservePermission"),
    DISTCP_OPTION_PRESERVE_USER("preserveUser"),
    DISTCP_OPTION_PRESERVE_GROUP("preserveGroup"),
    DISTCP_OPTION_PRESERVE_CHECKSUM_TYPE("preserveChecksumType"),
    DISTCP_OPTION_PRESERVE_ACL("preserveAcl"),
    DISTCP_OPTION_PRESERVE_XATTR("preserveXattr"),
    DISTCP_OPTION_PRESERVE_TIMES("preserveTimes");

    private final String name;

    ReplicationDistCpOption(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
