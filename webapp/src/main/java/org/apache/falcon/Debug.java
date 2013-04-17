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

package org.apache.falcon;

import org.apache.falcon.client.FalconClient;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.security.CurrentUser;
import org.apache.falcon.service.Services;
import org.apache.falcon.util.DeploymentProperties;
import org.apache.falcon.util.StartupProperties;
import org.apache.falcon.workflow.engine.OozieWorkflowEngine;
import org.apache.log4j.Logger;

import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.ByteArrayInputStream;
import java.text.SimpleDateFormat;

public class Debug {
    private static final Logger LOG = Logger.getLogger(Debug.class);

    private static final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm Z");

    public static void main(String[] args) throws Exception {
        String falconUrl = args[0];
        String type = args[1];
        String entity;

        Services.get().register(ConfigurationStore.get());
        ConfigurationStore.get().init();
        CurrentUser.authenticate("testuser");
        FalconClient client = new FalconClient(falconUrl);
        for (int index = 2; index < args.length; index++) {
            entity = args[index];
            String[] deps = client.getDependency(type, entity).split("\n");
            for (String line : deps) {
                String[] fields = line.replace("(", "").replace(")", "").split(" ");
                EntityType eType = EntityType.valueOf(fields[0].toUpperCase());
                if (ConfigurationStore.get().get(eType, fields[1]) != null) {
                    continue;
                }
                String xml = client.getDefinition(eType.name().toLowerCase(), fields[1]);
                System.out.println(xml);
                store(eType, xml);
            }
            String xml = client.getDefinition(type.toLowerCase(), entity);
            System.out.println(xml);
            store(EntityType.valueOf(type.toUpperCase()), xml);
        }

        entity = args[2];
        Entity obj = EntityUtil.getEntity(type, entity);
        Process newEntity = (Process) obj.copy();
        newEntity.setFrequency(Frequency.fromString("minutes(5)"));
        System.out.println("##############OLD ENTITY " + EntityUtil.md5(obj));
        System.out.println("##############NEW ENTITY " + EntityUtil.md5(newEntity));


//        OozieWorkflowEngine engine = new OozieWorkflowEngine();
//        Date start = formatter.parse("2010-01-02 01:05 UTC");
//        Date end = formatter.parse("2010-01-02 01:21 UTC");
//        InstancesResult status = engine.suspendInstances(obj, start, end, new Properties());
//        System.out.println(Arrays.toString(status.getInstances()));
//        AbstractInstanceManager manager = new InstanceManager();
//        InstancesResult result = manager.suspendInstance(new NullServletRequest(), type, entity,
//                "2010-01-02T01:05Z", "2010-01-02T01:21Z", "*");

        DeploymentProperties.get().setProperty("deploy.mode", "standalone");
        StartupProperties.get().setProperty("current.colo", "ua1");
        OozieWorkflowEngine engine = new OozieWorkflowEngine();
        ConfigurationStore.get().initiateUpdate(newEntity);
        engine.update(obj, newEntity, newEntity.getClusters().getClusters().get(0).getName());
        engine.delete(newEntity);
        System.exit(0);
    }

    private static void store(EntityType eType, String xml) throws JAXBException, FalconException {
        Unmarshaller unmarshaller = eType.getUnmarshaller();
        Entity obj = (Entity) unmarshaller.unmarshal(new
                ByteArrayInputStream(xml.getBytes()));
        ConfigurationStore.get().publish(eType, obj);
    }
}
