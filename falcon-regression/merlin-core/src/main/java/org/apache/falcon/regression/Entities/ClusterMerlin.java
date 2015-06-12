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
import org.apache.falcon.entity.v0.cluster.Locations;
import org.apache.falcon.entity.v0.cluster.Properties;
import org.apache.falcon.entity.v0.cluster.Property;
import org.apache.falcon.regression.core.util.Util;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.asserts.SoftAssert;

import javax.xml.bind.JAXBException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Class for representing a cluster xml. */
public class ClusterMerlin extends Cluster {
    private static final Logger LOGGER = Logger.getLogger(ClusterMerlin.class);
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
        final HashMap<String, String> nameMap = new HashMap<>(1);
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

    public String getInterfaceEndpoint(final Interfacetype interfaceType) {
        String value = null;
        for (Interface anInterface : getInterfaces().getInterfaces()) {
            if (anInterface.getType() == interfaceType) {
                value = anInterface.getEndpoint();
            }
        }
        LOGGER.info("Cluster: " + getName() + " interfaceType: " + interfaceType
            + " value:" + value);
        return value;
    }

    public String getProperty(final String propName) {
        String value = null;
        for (Property property : getProperties().getProperties()) {
            if (property.getName().trim().equals(propName.trim())) {
                value = property.getValue();
            }
        }
        LOGGER.info("Cluster: " + getName() + " property: " + propName + " value:" + value);
        return value;
    }

    public String getLocation(final String locationType) {
        String value = null;
        for (Location location : getLocations().getLocations()) {
            if (location.getName().name().trim().equalsIgnoreCase(locationType.trim().toLowerCase())) {
                value = location.getPath();
            }
        }
        LOGGER.info("Cluster: " + getName() + " locationType: " + locationType + " value:" + value);
        return value;
    }

    /**
     * Cleans all properties and returns empty cluster as a draft (as we can't create cluster e.g from empty string).
     */
    public ClusterMerlin getEmptyCluster() {
        ClusterMerlin clusterMerlin = new ClusterMerlin(this.toString());
        clusterMerlin.setName("");
        clusterMerlin.setDescription(null);
        clusterMerlin.setColo(null);
        clusterMerlin.setTags(null);
        clusterMerlin.setInterfaces(new Interfaces());
        clusterMerlin.setLocations(new Locations());
        clusterMerlin.getACL().setGroup("");
        clusterMerlin.getACL().setOwner("");
        clusterMerlin.setProperties(new Properties());
        return clusterMerlin;
    }

    public void addLocation(ClusterLocationType type, String path) {
        Location newLocation = new Location();
        newLocation.setName(type);
        newLocation.setPath(path);
        getLocations().getLocations().add(newLocation);
    }

    public void addProperty(String name, String value) {
        Property property = new Property();
        property.setName(name);
        property.setValue(value);
        getProperties().getProperties().add(property);
    }

    public void addInterface(Interfacetype type, String endpoint, String version) {
        Interface iface = new Interface();
        iface.setType(type);
        iface.setEndpoint(endpoint);
        iface.setVersion(version);
        getInterfaces().getInterfaces().add(iface);
    }

    public void assertEquals(ClusterMerlin cluster) {
        LOGGER.info(String.format("Comparing : source: %n%s%n and cluster: %n%n%s",
            Util.prettyPrintXml(toString()), Util.prettyPrintXml(cluster.toString())));
        SoftAssert softAssert = new SoftAssert();
        softAssert.assertEquals(name, cluster.getName(), "Cluster name is different.");
        softAssert.assertEquals(colo, cluster.getColo(), "Cluster colo is different.");
        softAssert.assertEquals(description, cluster.getDescription(), "Cluster description is different.");
        softAssert.assertEquals(tags, cluster.getTags(), "Cluster tags are different.");
        softAssert.assertTrue(interfacesEqual(interfaces.getInterfaces(), cluster.getInterfaces().getInterfaces()),
            "Cluster interfaces are different");
        softAssert.assertTrue(locationsEqual(locations.getLocations(), cluster.getLocations().getLocations()),
            "Cluster locations are different");
        softAssert.assertEquals(acl.getGroup(), cluster.getACL().getGroup(), "Cluster acl group is different.");
        softAssert.assertEquals(acl.getOwner(), cluster.getACL().getOwner(), "Cluster acl owner is different.");
        softAssert.assertEquals(acl.getPermission(), cluster.getACL().getPermission(),
            "Cluster acl permissions is different.");
        softAssert.assertTrue(propertiesEqual(properties.getProperties(), cluster.getProperties().getProperties()),
            "Cluster properties are different.");
        softAssert.assertAll();
    }

    private static boolean checkEquality(String str1, String str2, String message){
        if (!str1.equals(str2)) {
            LOGGER.info(String.format("Cluster %s are different: %s and %s.", message, str1, str2));
            return false;
        }
        return true;
    }

    private static boolean interfacesEqual(List<Interface> srcInterfaces, List<Interface> trgInterfaces) {
        if (srcInterfaces.size() == trgInterfaces.size()) {
            boolean equality = false;
            for(Interface iface1: srcInterfaces){
                for(Interface iface2 : trgInterfaces) {
                    if (iface2.getType().value().equals(iface1.getType().value())) {
                        equality = checkEquality(iface1.getEndpoint(), iface2.getEndpoint(),
                            iface1.getType().value() + " interface endpoints");
                        equality &= checkEquality(iface1.getVersion(), iface2.getVersion(),
                            iface1.getType().value() + " interface versions");
                    }
                }
            }
            return equality;
        } else {
            return false;
        }
    }

    private static boolean propertiesEqual(List<Property> srcProps, List<Property> trgProps) {
        if (srcProps.size() == trgProps.size()) {
            boolean equality = true;
            for(Property prop1: srcProps){
                for(Property prop2 : trgProps) {
                    if (prop2.getName().equals(prop1.getName())) {
                        equality &= checkEquality(prop1.getValue(), prop2.getValue(),
                            prop1.getName() + " property values");
                    }
                }
            }
            return equality;
        } else {
            return false;
        }
    }

    /**
     * Compares two lists of locations.
     */
    private static boolean locationsEqual(List<Location> srcLocations, List<Location> objLocations) {
        if (srcLocations.size() != objLocations.size()) {
            return false;
        }
    nextType:
        for (ClusterLocationType type : ClusterLocationType.values()) {
            List<Location> locations1 = new ArrayList<>();
            List<Location> locations2 = new ArrayList<>();
            //get locations of the same type
            for (int i = 0; i < srcLocations.size(); i++) {
                if (srcLocations.get(i).getName() == type) {
                    locations1.add(srcLocations.get(i));
                }
                if (objLocations.get(i).getName() == type) {
                    locations2.add(objLocations.get(i));
                }
            }
            //compare locations of the same type. At least 1 match should be present.
            if (locations1.size() != locations2.size()) {
                return false;
            }
            for (Location location1 : locations1) {
                for (Location location2 : locations2) {
                    if (location1.getPath().equals(location2.getPath())) {
                        continue nextType;
                    }
                }
            }
            return false;
        }
        return true;
    }

    public Location getLocation(ClusterLocationType type) {
        List<Location> locationsOfType = new ArrayList<>();
        for(Location location : locations.getLocations()) {
            if (location.getName() == type) {
                locationsOfType.add(location);
            }
        }
        Assert.assertEquals(locationsOfType.size(), 1, "Unexpected number of " + type + " locations in: " + this);
        return locationsOfType.get(0);
    }
    @Override
    public EntityType getEntityType() {
        return EntityType.CLUSTER;
    }

}
