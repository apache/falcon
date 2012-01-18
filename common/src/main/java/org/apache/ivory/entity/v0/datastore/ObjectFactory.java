/*******************************************************************************
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
 ******************************************************************************/
package org.apache.ivory.entity.v0.datastore;

import javax.xml.bind.JAXBElement;
import javax.xml.bind.annotation.XmlElementDecl;
import javax.xml.bind.annotation.XmlRegistry;
import javax.xml.namespace.QName;

/**
 * This object contains factory methods for each Java content interface and Java element interface
 * generated in the org.apache.ivory.entity.v0.datastore package.
 * <p>
 * An ObjectFactory allows you to programatically construct new instances of the Java representation
 * for XML content. The Java representation of XML content can consist of schema derived interfaces
 * and classes representing the binding of schema type definitions, element declarations and model
 * groups. Factory methods for each of these are provided in this class.
 * 
 */
@XmlRegistry
public class ObjectFactory {

  private final static QName _Argument_QNAME = new QName("uri:ivory:datastore:0.1", "Argument");

  private final static QName _Comments_QNAME = new QName("uri:ivory:datastore:0.1", "Comments");

  /**
   * Create a new ObjectFactory that can be used to create new instances of schema derived classes
   * for package: org.apache.ivory.entity.v0.datastore
   * 
   */
  public ObjectFactory() {
  }

  /**
   * Create an instance of {@link Datastore }
   * 
   */
  public Datastore createDatastore() {
    return new Datastore();
  }

  /**
   * Create an instance of {@link Proxy }
   * 
   */
  public Proxy createProxy() {
    return new Proxy();
  }

  /**
   * Create an instance of {@link Interfaces }
   * 
   */
  public Interfaces createInterfaces() {
    return new Interfaces();
  }

  /**
   * Create an instance of {@link Sink }
   * 
   */
  public Sink createSink() {
    return new Sink();
  }

  /**
   * Create an instance of {@link Source }
   * 
   */
  public Source createSource() {
    return new Source();
  }

  /**
   * Create an instance of {@link Configuration }
   * 
   */
  public Configuration createConfiguration() {
    return new Configuration();
  }

  /**
   * Create an instance of {@link Interface }
   * 
   */
  public Interface createInterface() {
    return new Interface();
  }

  /**
   * Create an instance of {@link Property }
   * 
   */
  public Property createProperty() {
    return new Property();
  }

  /**
   * Create an instance of {@link Option }
   * 
   */
  public Option createOption() {
    return new Option();
  }

  /**
   * Create an instance of {@link Options }
   * 
   */
  public Options createOptions() {
    return new Options();
  }

  /**
   * Create an instance of {@link Command }
   * 
   */
  public Command createCommand() {
    return new Command();
  }

  /**
   * Create an instance of {@link JAXBElement }{@code <}{@link String }{@code >}
   * 
   */
  @XmlElementDecl(namespace = "uri:ivory:datastore:0.1", name = "Argument")
  public JAXBElement<String> createArgument(String value) {
    return new JAXBElement<String>(_Argument_QNAME, String.class, null, value);
  }

  /**
   * Create an instance of {@link JAXBElement }{@code <}{@link String }{@code >}
   * 
   */
  @XmlElementDecl(namespace = "uri:ivory:datastore:0.1", name = "Comments")
  public JAXBElement<String> createComments(String value) {
    return new JAXBElement<String>(_Comments_QNAME, String.class, null, value);
  }

}
