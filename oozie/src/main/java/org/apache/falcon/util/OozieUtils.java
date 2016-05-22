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
package org.apache.falcon.util;

import org.apache.falcon.oozie.bundle.BUNDLEAPP;
import org.apache.falcon.oozie.coordinator.COORDINATORAPP;
import org.apache.falcon.oozie.hive.ACTION;
import org.apache.falcon.oozie.workflow.CONFIGURATION;
import org.apache.falcon.oozie.workflow.WORKFLOWAPP;
import org.apache.hadoop.conf.Configuration;
import org.apache.xerces.dom.ElementNSImpl;
import org.w3c.dom.Document;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.dom.DOMResult;
import java.io.ByteArrayInputStream;
import java.util.Map;
import java.util.Properties;

/**
 * Help methods relating to oozie configuration.
 */
public final class OozieUtils {
    public static final JAXBContext WORKFLOW_JAXB_CONTEXT;
    public static final JAXBContext ACTION_JAXB_CONTEXT;
    public static final JAXBContext COORD_JAXB_CONTEXT;
    public static final JAXBContext BUNDLE_JAXB_CONTEXT;
    public static final JAXBContext CONFIG_JAXB_CONTEXT;
    protected static final JAXBContext HIVE_ACTION_JAXB_CONTEXT;
    protected static final JAXBContext SQOOP_ACTION_JAXB_CONTEXT;
    protected static final JAXBContext SPARK_ACTION_JAXB_CONTEXT;

    static {
        try {
            WORKFLOW_JAXB_CONTEXT = JAXBContext.newInstance(WORKFLOWAPP.class);
            ACTION_JAXB_CONTEXT = JAXBContext.newInstance(org.apache.falcon.oozie.workflow.ACTION.class);
            COORD_JAXB_CONTEXT = JAXBContext.newInstance(COORDINATORAPP.class);
            BUNDLE_JAXB_CONTEXT = JAXBContext.newInstance(BUNDLEAPP.class);
            CONFIG_JAXB_CONTEXT = JAXBContext.newInstance(CONFIGURATION.class);
            HIVE_ACTION_JAXB_CONTEXT = JAXBContext.newInstance(
                org.apache.falcon.oozie.hive.ACTION.class.getPackage().getName());
            SQOOP_ACTION_JAXB_CONTEXT = JAXBContext.newInstance(
                    org.apache.falcon.oozie.sqoop.ACTION.class.getPackage().getName());
            SPARK_ACTION_JAXB_CONTEXT = JAXBContext.newInstance(
                    org.apache.falcon.oozie.spark.ACTION.class.getPackage().getName());
        } catch (JAXBException e) {
            throw new RuntimeException("Unable to create JAXB context", e);
        }
    }


    private OozieUtils() {}

    public static Properties toProperties(String properties) {
        Configuration conf = new Configuration(false);
        conf.addResource(new ByteArrayInputStream(properties.getBytes()));
        Properties jobprops = new Properties();
        for (Map.Entry<String, String> entry : conf) {
            jobprops.put(entry.getKey(), entry.getValue());
        }
        return jobprops;
    }

    @SuppressWarnings("unchecked")
    public static JAXBElement<ACTION> unMarshalHiveAction(org.apache.falcon.oozie.workflow.ACTION wfAction) {
        try {
            Unmarshaller unmarshaller = HIVE_ACTION_JAXB_CONTEXT.createUnmarshaller();
            unmarshaller.setEventHandler(new javax.xml.bind.helpers.DefaultValidationEventHandler());
            return (JAXBElement<org.apache.falcon.oozie.hive.ACTION>)
                unmarshaller.unmarshal((ElementNSImpl) wfAction.getAny());
        } catch (JAXBException e) {
            throw new RuntimeException("Unable to unmarshall hive action.", e);
        }
    }

    public static  void marshalHiveAction(org.apache.falcon.oozie.workflow.ACTION wfAction,
        JAXBElement<org.apache.falcon.oozie.hive.ACTION> actionjaxbElement) {
        try {
            DOMResult hiveActionDOM = new DOMResult();
            Marshaller marshaller = HIVE_ACTION_JAXB_CONTEXT.createMarshaller();
            marshaller.marshal(actionjaxbElement, hiveActionDOM);
            wfAction.setAny(((Document) hiveActionDOM.getNode()).getDocumentElement());
        } catch (JAXBException e) {
            throw new RuntimeException("Unable to marshall hive action.", e);
        }
    }

    @SuppressWarnings("unchecked")
    public static JAXBElement<org.apache.falcon.oozie.sqoop.ACTION> unMarshalSqoopAction(
        org.apache.falcon.oozie.workflow.ACTION wfAction) {
        try {
            Unmarshaller unmarshaller = SQOOP_ACTION_JAXB_CONTEXT.createUnmarshaller();
            unmarshaller.setEventHandler(new javax.xml.bind.helpers.DefaultValidationEventHandler());
            return (JAXBElement<org.apache.falcon.oozie.sqoop.ACTION>)
                    unmarshaller.unmarshal((ElementNSImpl) wfAction.getAny());
        } catch (JAXBException e) {
            throw new RuntimeException("Unable to unmarshall sqoop action.", e);
        }
    }

    public static  void marshalSqoopAction(org.apache.falcon.oozie.workflow.ACTION wfAction,
                                          JAXBElement<org.apache.falcon.oozie.sqoop.ACTION> actionjaxbElement) {
        try {
            DOMResult hiveActionDOM = new DOMResult();
            Marshaller marshaller = SQOOP_ACTION_JAXB_CONTEXT.createMarshaller();
            marshaller.marshal(actionjaxbElement, hiveActionDOM);
            wfAction.setAny(((Document) hiveActionDOM.getNode()).getDocumentElement());
        } catch (JAXBException e) {
            throw new RuntimeException("Unable to marshall sqoop action.", e);
        }
    }

    public static JAXBElement<org.apache.falcon.oozie.spark.ACTION> unMarshalSparkAction(
            org.apache.falcon.oozie.workflow.ACTION wfAction) {
        try {
            Unmarshaller unmarshaller = SPARK_ACTION_JAXB_CONTEXT.createUnmarshaller();
            unmarshaller.setEventHandler(new javax.xml.bind.helpers.DefaultValidationEventHandler());
            return (JAXBElement<org.apache.falcon.oozie.spark.ACTION>)
                    unmarshaller.unmarshal((ElementNSImpl) wfAction.getAny());
        } catch (JAXBException e) {
            throw new RuntimeException("Unable to unmarshall spark action.", e);
        }
    }

    public static  void marshalSparkAction(org.apache.falcon.oozie.workflow.ACTION wfAction,
                                          JAXBElement<org.apache.falcon.oozie.spark.ACTION> actionjaxbElement) {
        try {
            DOMResult sparkActionDOM = new DOMResult();
            Marshaller marshaller = SPARK_ACTION_JAXB_CONTEXT.createMarshaller();
            marshaller.marshal(actionjaxbElement, sparkActionDOM);
            wfAction.setAny(((Document) sparkActionDOM.getNode()).getDocumentElement());
        } catch (JAXBException e) {
            throw new RuntimeException("Unable to marshall spark action.", e);
        }
    }
}
