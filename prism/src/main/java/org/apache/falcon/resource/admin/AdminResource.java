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

package org.apache.falcon.resource.admin;

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconWebException;
import org.apache.falcon.security.CurrentUser;
import org.apache.falcon.security.SecurityUtil;
import org.apache.falcon.util.BuildProperties;
import org.apache.falcon.util.DeploymentProperties;
import org.apache.falcon.util.RuntimeProperties;
import org.apache.falcon.util.StartupProperties;
import org.apache.hadoop.util.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Jersey Resource for admin operations.
 */
@Path("admin")
public class AdminResource {
    public static final String SAFEMODE = "safemode";
    private static final Logger LOG = LoggerFactory.getLogger(AdminResource.class);

    /**
     * Get stack trace of the falcon server.
     * @return Stack trace of the server.
     */
    @GET
    @Path("stack")
    @Produces(MediaType.TEXT_PLAIN)
    public String getThreadDump() {
        ThreadGroup topThreadGroup = Thread.currentThread().getThreadGroup();

        while (topThreadGroup.getParent() != null) {
            topThreadGroup = topThreadGroup.getParent();
        }
        Thread[] threads = new Thread[topThreadGroup.activeCount()];

        int nr = topThreadGroup.enumerate(threads);
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < nr; i++) {
            builder.append(threads[i].getName()).append("\nState: ").
                    append(threads[i].getState()).append("\n");
            String stackTrace = StringUtils.join(threads[i].getStackTrace(), "\n");
            builder.append(stackTrace);
        }
        return builder.toString();
    }

    private PropertyList version;

    /**
     * Get version of the falcon server.
     * @return Version of the server.
     */
    @GET
    @Path("version")
    @Produces({MediaType.TEXT_XML, MediaType.APPLICATION_JSON})
    public PropertyList getVersion() {
        if (version == null) {
            List<Property> props = new ArrayList<Property>();

            Property property = new Property();
            property.key = "Version";
            property.value = BuildProperties.get().getProperty("build.version");
            props.add(property);

            property = new Property();
            property.key = "Mode";
            property.value = DeploymentProperties.get().getProperty("deploy.mode");
            props.add(property);

            property = new Property();
            property.key = "Hadoop";
            property.value = VersionInfo.getVersion() + "-r" + VersionInfo.getRevision();
            props.add(property);

            property = new Property();
            property.key = "authentication";
            property.value = StartupProperties.get().getProperty("falcon.authentication.type", "simple");
            props.add(property);

            property = new Property();
            property.key = SAFEMODE;
            property.value = StartupProperties.get().getProperty(StartupProperties.SAFEMODE_PROPERTY, "false");
            props.add(property);

            version = new PropertyList();
            version.properties = props;
        }

        return version;
    }

    /**
     * Set safemode for falcon server.
     *
     * @param mode Set safemode to true/false based on mode.
     * @return Configuration information of the server.
     */
    @GET
    @Path("setSafeMode/{mode}")
    @Produces({MediaType.TEXT_XML, MediaType.APPLICATION_JSON})
    public String setSafeMode(@PathParam("mode") String mode) {
        LOG.info("Setting falcon server safemode property to: {}", mode);
        if ("true".equals(mode) || "false".equals(mode)) {
            StartupProperties.get().setProperty(StartupProperties.SAFEMODE_PROPERTY, mode);
        } else {
            LOG.error("Bad request, Invalid value for setsafemode : {}", mode);
            throw FalconWebException.newAPIException("Invalid value \"" + mode + "\" provided for safemode.",
                    Response.Status.BAD_REQUEST);
        }
        return StartupProperties.get().getProperty(StartupProperties.SAFEMODE_PROPERTY, "false");
    }

    /**
     * Get configuration information of the falcon server.
     * @param type config-type can be build, deploy, startup or runtime
     * @return Configuration information of the server.
     */
    @GET
    @Path("config/{type}")
    @Produces({MediaType.TEXT_XML, MediaType.APPLICATION_JSON})
    public PropertyList getVersion(@PathParam("type") String type) {
        if ("build".equals(type)) {
            return getProperties(BuildProperties.get());
        } else if ("deploy".equals(type)) {
            return getProperties(DeploymentProperties.get());
        } else if ("startup".equals(type)) {
            return getProperties(StartupProperties.get());
        } else if ("runtime".equals(type)) {
            return getProperties(RuntimeProperties.get());
        } else {
            return null;
        }
    }

    private PropertyList getProperties(Properties properties) {
        List<Property> props = new ArrayList<Property>();

        for (Object key : properties.keySet()) {
            Property property = new Property();
            property.key = key.toString();
            property.value = properties.getProperty(key.toString());
            props.add(property);
        }
        PropertyList propertyList = new PropertyList();
        propertyList.properties = props;
        return propertyList;
    }

    //SUSPEND CHECKSTYLE CHECK VisibilityModifierCheck

    /**
     * Class for property.
     */
    @XmlRootElement(name = "property")
    @XmlAccessorType(XmlAccessType.FIELD)
    @edu.umd.cs.findbugs.annotations.SuppressWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class Property {
        public String key;
        public String value;
    }
    //RESUME CHECKSTYLE CHECK VisibilityModifierCheck

    //SUSPEND CHECKSTYLE CHECK VisibilityModifierCheck

    /**
     * Class for list of Properties.
     */
    @XmlRootElement(name = "properties")
    @XmlAccessorType(XmlAccessType.FIELD)
    @edu.umd.cs.findbugs.annotations.SuppressWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class PropertyList {
        public List<Property> properties;
    }
    //RESUME CHECKSTYLE CHECK VisibilityModifierCheck

    @GET
    @Path("clearuser")
    @Produces(MediaType.TEXT_PLAIN)
    public String clearUser(@Context HttpServletResponse response) {
        if (!SecurityUtil.isSecurityEnabled()) {
            Cookie cookie = new Cookie("hadoop.auth", null);
            cookie.setPath("/");
            cookie.setMaxAge(0);
            cookie.setSecure(false);
            response.addCookie(cookie);
        }  // Else,  Do not checkin User, security is handled via Kerberos.
        return "ok";
    }

    @GET
    @Path("getuser")
    @Produces(MediaType.TEXT_PLAIN)
    public String getAuthenticatedUser() {
        String user;
        try {
            if (SecurityUtil.isSecurityEnabled()) {
                user = CurrentUser.getAuthenticatedUser();
            } else {
                user = CurrentUser.getUser();
            }
        } catch (IllegalStateException ile) {
            user = "";
        }
        return user;
    }
}
