package org.apache.falcon.resource.admin;

import org.apache.commons.lang.StringUtils;
import org.apache.falcon.util.BuildProperties;
import org.apache.falcon.util.DeploymentProperties;
import org.apache.falcon.util.RuntimeProperties;
import org.apache.falcon.util.StartupProperties;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@Path("admin")
public class AdminResource {

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

    private String version;

    @GET
    @Path("version")
    @Produces(MediaType.TEXT_PLAIN)
    public String getVersion() {
        if (version == null) {
            version = "{Version:\"" + BuildProperties.get().getProperty("build.version") +
                    "\",Mode:\"" + DeploymentProperties.get().getProperty("deploy.mode") + "\"}";
        }
        return version;
    }

    @GET
    @Path("config/{type}")
    @Produces(MediaType.TEXT_XML)
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

    @XmlRootElement(name = "property")
    @XmlAccessorType(XmlAccessType.FIELD)
    private static class Property {
        public String key;
        public String value;
    }

    @XmlRootElement(name = "properties")
    @XmlAccessorType(XmlAccessType.FIELD)
    private static class PropertyList {
        public List<Property> properties;
    }
}
