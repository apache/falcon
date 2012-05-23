package org.apache.ivory.cluster.util;

import java.io.File;

import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.entity.v0.cluster.Interfacetype;
import org.apache.log4j.Logger;


public class StandAloneCluster extends EmbeddedCluster{
    private static final Logger LOG = Logger.getLogger(StandAloneCluster.class);
    
    private StandAloneCluster() {
    }
    
    public static StandAloneCluster newCluster(String clusterFile) throws Exception {
        LOG.debug("Initialising standalone cluster");
        StandAloneCluster cluster = new StandAloneCluster();
        cluster.clusterEntity = (Cluster) EntityType.CLUSTER.getUnmarshaller().unmarshal(new File(clusterFile));
        cluster.getConf().set("fs.default.name", cluster.clusterEntity.getInterfaces().get(Interfacetype.WRITE).getEndpoint());
        LOG.info("Cluster Namenode = " + cluster.getConf().get("fs.default.name"));
        return cluster;
    }
    
    @Override
    public void shutdown() {
        
    }
}
