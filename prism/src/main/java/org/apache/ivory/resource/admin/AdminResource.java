package org.apache.ivory.resource.admin;

import org.apache.commons.lang.StringUtils;
import org.apache.ivory.util.BuildProperties;
import org.apache.ivory.util.DeploymentProperties;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

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
}
