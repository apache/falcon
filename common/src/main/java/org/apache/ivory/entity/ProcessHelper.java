package org.apache.ivory.entity;

import org.apache.ivory.entity.v0.process.Cluster;
import org.apache.ivory.entity.v0.process.Process;

public class ProcessHelper {
    public static Cluster getCluster(Process process, String clusterName) {
        for(Cluster cluster:process.getClusters().getClusters())
            if(cluster.getName().equals(clusterName))
                return cluster;
        return null;
    }
}
