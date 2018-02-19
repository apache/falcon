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

package org.apache.falcon.oozie.process;

import org.apache.falcon.util.OozieUtils;
import org.testng.annotations.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.URL;

import static org.testng.Assert.assertFalse;

/**
 *  Make sure OozieUtils.unMarshal[*]Action does not throw ClassCastException.
 */
public class ClassCastExceptionTest {


    public static String getXmlFile(URL url) throws IOException {
        BufferedReader rd = new BufferedReader(new InputStreamReader(url.openStream()));
        String inputLine = null;
        StringBuilder builder = new StringBuilder();
        while((inputLine = rd.readLine()) != null) {
            builder.append(inputLine);
        }
        rd.close();
        return builder.toString();
    }

    public static Document convertStringToDocument(String xmlStr) {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder;
        try {
            builder = factory.newDocumentBuilder();
            Document doc = builder.parse(new InputSource(new StringReader(xmlStr)));
            return doc;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Test
    public void testClassCastException() throws IOException {
        String filePath = "/action/process/hive-action.xml";
        URL resource = this.getClass().getResource(filePath);
        Document doc  = convertStringToDocument(getXmlFile(resource));
        NodeList ns = doc.getElementsByTagName("action");
        Node node = ns.item(0);

        org.apache.falcon.oozie.workflow.ACTION wfAction = new org.apache.falcon.oozie.workflow.ACTION();
        wfAction.setAny(node);

        try {
            OozieUtils.unMarshalHiveAction(wfAction);
        } catch (Exception e){
            //expected: UnmarshalException
            assertFalse(e.getClass().equals(ClassCastException.class));
        }

        try {
            OozieUtils.unMarshalSqoopAction(wfAction);
        } catch (Exception e){
            assertFalse(e.getClass().equals(ClassCastException.class));
        }

        try {
            OozieUtils.unMarshalSparkAction(wfAction);
        } catch (Exception e){
            assertFalse(e.getClass().equals(ClassCastException.class));
        }
    }

}
