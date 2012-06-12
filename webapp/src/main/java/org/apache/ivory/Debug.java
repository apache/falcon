package org.apache.ivory;

import org.apache.ivory.client.IvoryClient;
import org.apache.ivory.entity.EntityUtil;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.resource.InstancesResult;
import org.apache.ivory.service.Services;
import org.apache.ivory.workflow.engine.OozieWorkflowEngine;
import org.apache.log4j.Logger;

import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.ByteArrayInputStream;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

public class Debug {
    private static final Logger LOG = Logger.getLogger(Debug.class);

    private static final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm Z");

    public static void main(String[] args) throws Exception {
        String ivoryUrl = args[0];
        String type = args[1];
        String entity = args[2];

        Services.get().register(ConfigurationStore.get());
        ConfigurationStore.get().init();

        IvoryClient client = new IvoryClient(ivoryUrl);
        String[] deps = client.getDependency(type, entity).split("\n");
        for (String line : deps) {
            String[] fields = line.replace("(", "").replace(")", "").split(" ");
            EntityType eType = EntityType.valueOf(fields[0].toUpperCase());
            String xml = client.getDefinition(eType.name().toLowerCase(), fields[1]);
            store(eType, xml);
        }
        String xml = client.getDefinition(type.toLowerCase(), entity);
        System.out.println(xml);
        store(EntityType.valueOf(type.toUpperCase()), xml);

        Entity obj = EntityUtil.getEntity(type, entity);

        OozieWorkflowEngine engine = new OozieWorkflowEngine();
        Date start = formatter.parse("2010-01-02 01:15 UTC");
        Date end = formatter.parse("2010-01-02 01:20 UTC");
        InstancesResult status = engine.killInstances(obj, start, end, new Properties());
        System.out.println(Arrays.toString(status.getInstances()));
    }

    private static void store(EntityType eType, String xml) throws JAXBException, IvoryException {
        Unmarshaller unmarshaller = eType.getUnmarshaller();
        Entity obj = (Entity) unmarshaller.unmarshal(new
                ByteArrayInputStream(xml.getBytes()));
        ConfigurationStore.get().publish(eType, obj);
    }
}
