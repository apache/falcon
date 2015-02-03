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

package org.apache.falcon.regression.Entities;

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.ACL;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.cluster.ClusterLocationType;
import org.apache.falcon.entity.v0.cluster.Interface;
import org.apache.falcon.entity.v0.cluster.Interfaces;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.apache.falcon.entity.v0.cluster.Location;
import org.testng.Assert;

import javax.xml.bind.JAXBException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Class for representing a cluster xml. */
public class ClusterMerlin extends Cluster {

    public ClusterMerlin(String clusterData) {
        final Cluster cluster = (Cluster) TestEntityUtil.fromString(EntityType.CLUSTER,
                clusterData);
        try {
            PropertyUtils.copyProperties(this, cluster);
        } catch (ReflectiveOperationException e) {
            Assert.fail("Can't create ClusterMerlin: " + ExceptionUtils.getStackTrace(e));
        }
    }

    @Override
    public String toString() {
        try {
            StringWriter sw = new StringWriter();
            EntityType.CLUSTER.getMarshaller().marshal(this, sw);
            return sw.toString();
        } catch (JAXBException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Sets unique names for the cluster.
     * @return mapping of old name to new name
     * @param prefix prefix of new name
     */
    public Map<? extends String, ? extends String> setUniqueName(String prefix) {
        final String oldName = getName();
        final String newName = TestEntityUtil.generateUniqueName(prefix, oldName);
        setName(newName);
        final HashMap<String, String> nameMap = new HashMap<String, String>(1);
        nameMap.put(oldName, newName);
        return nameMap;
    }

    /**
     * Set ACL.
     */
    public void setACL(String owner, String group, String permission) {
        ACL acl = new ACL();
        acl.setOwner(owner);
        acl.setGroup(group);
        acl.setPermission(permission);
        this.setACL(acl);
    }

    public void setInterface(Interfacetype interfacetype, String value) {
        final Interfaces interfaces = this.getInterfaces();
        final List<Interface> interfaceList = interfaces.getInterfaces();
        for (final Interface anInterface : interfaceList) {
            if (anInterface.getType() == interfacetype) {
                anInterface.setEndpoint(value);
            }
        }
    }

    public void setWorkingLocationPath(String path) {
        for (Location location : getLocations().getLocations()) {
            if (location.getName() == ClusterLocationType.WORKING) {
                location.setPath(path);
                break;
            }
        }
    }

}
