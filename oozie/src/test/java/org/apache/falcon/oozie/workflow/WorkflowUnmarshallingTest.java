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

package org.apache.falcon.oozie.workflow;

import org.testng.Assert;
import org.testng.annotations.Test;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

/**
 * Test to verify workflow xml unmarshalling.
 */
public class WorkflowUnmarshallingTest {

    @Test
    public void testValidWorkflowUnamrashalling() throws JAXBException, SAXException {
        Unmarshaller unmarshaller = JAXBContext.newInstance(
                org.apache.falcon.oozie.workflow.WORKFLOWAPP.class).createUnmarshaller();
        SchemaFactory schemaFactory = SchemaFactory.newInstance("http://www.w3.org/2001/XMLSchema");
        Schema schema = schemaFactory.newSchema(this.getClass().getResource("/oozie-workflow-0.3.xsd"));
        unmarshaller.setSchema(schema);
        JAXBElement<WORKFLOWAPP> workflowApp = (JAXBElement<WORKFLOWAPP>) unmarshaller.unmarshal(
                WorkflowUnmarshallingTest.class
                        .getResourceAsStream("/oozie/xmls/workflow.xml"));
        WORKFLOWAPP app = workflowApp.getValue();
        Assert.assertEquals(app.getName(), "java-main-wf");
        Assert.assertEquals(((ACTION) app.getDecisionOrForkOrJoin().get(0)).getName(), "java-node");
    }

}
