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
package org.apache.ivory.entity.v0.dataset;

import javax.xml.bind.JAXBElement;
import javax.xml.bind.annotation.XmlElementDecl;
import javax.xml.bind.annotation.XmlRegistry;
import javax.xml.namespace.QName;

/**
 * This object contains factory methods for each Java content interface and Java element interface
 * generated in the org.apache.ivory.entity.v0.dataset package.
 * <p>
 * An ObjectFactory allows you to programatically construct new instances of the Java representation
 * for XML content. The Java representation of XML content can consist of schema derived interfaces
 * and classes representing the binding of schema type definitions, element declarations and model
 * groups. Factory methods for each of these are provided in this class.
 * 
 */
@XmlRegistry
public class ObjectFactory {

  private final static QName _Comments_QNAME = new QName("uri:ivory:dataset:0.1", "Comments");

  private final static QName _Tags_QNAME = new QName("uri:ivory:dataset:0.1", "Tags");

  private final static QName _Status_QNAME = new QName("uri:ivory:dataset:0.1", "Status");

  private final static QName _IgnorePolicies_QNAME = new QName("uri:ivory:dataset:0.1",
      "IgnorePolicies");

  private final static QName _AuthScheme_QNAME = new QName("uri:ivory:dataset:0.1", "AuthScheme");

  private final static QName _Type_QNAME = new QName("uri:ivory:dataset:0.1", "Type");

  private final static QName _OutputFormat_QNAME = new QName("uri:ivory:dataset:0.1",
      "OutputFormat");

  private final static QName _CredentialProvider_QNAME = new QName("uri:ivory:dataset:0.1",
      "CredentialProvider");

  private final static QName _PartitionKey_QNAME = new QName("uri:ivory:dataset:0.1",
      "PartitionKey");

  private final static QName _SortKey_QNAME = new QName("uri:ivory:dataset:0.1", "SortKey");

  private final static QName _Priority_QNAME = new QName("uri:ivory:dataset:0.1", "Priority");

  private final static QName _InputFormat_QNAME = new QName("uri:ivory:dataset:0.1", "InputFormat");

  /**
   * Create a new ObjectFactory that can be used to create new instances of schema derived classes
   * for package: org.apache.ivory.entity.v0.dataset
   * 
   */
  public ObjectFactory() {
  }

  /**
   * Create an instance of {@link Paths }
   * 
   */
  public Paths createPaths() {
    return new Paths();
  }

  /**
   * Create an instance of {@link Consumers }
   * 
   */
  public Consumers createConsumers() {
    return new Consumers();
  }

  /**
   * Create an instance of {@link DateRange }
   * 
   */
  public DateRange createDateRange() {
    return new DateRange();
  }

  /**
   * Create an instance of {@link Partition }
   * 
   */
  public Partition createPartition() {
    return new Partition();
  }

  /**
   * Create an instance of {@link ACL }
   * 
   */
  public ACL createACL() {
    return new ACL();
  }

  /**
   * Create an instance of {@link Authentication }
   * 
   */
  public Authentication createAuthentication() {
    return new Authentication();
  }

  /**
   * Create an instance of {@link Consumer }
   * 
   */
  public Consumer createConsumer() {
    return new Consumer();
  }

  /**
   * Create an instance of {@link Copy }
   * 
   */
  public Copy createCopy() {
    return new Copy();
  }

  /**
   * Create an instance of {@link Source }
   * 
   */
  public Source createSource() {
    return new Source();
  }

  /**
   * Create an instance of {@link Owner }
   * 
   */
  public Owner createOwner() {
    return new Owner();
  }

  /**
   * Create an instance of {@link Export }
   * 
   */
  public Export createExport() {
    return new Export();
  }

  /**
   * Create an instance of {@link Load }
   * 
   */
  public Load createLoad() {
    return new Load();
  }

  /**
   * Create an instance of {@link Path }
   * 
   */
  public Path createPath() {
    return new Path();
  }

  /**
   * Create an instance of {@link Policies }
   * 
   */
  public Policies createPolicies() {
    return new Policies();
  }

  /**
   * Create an instance of {@link DataFlow }
   * 
   */
  public DataFlow createDataFlow() {
    return new DataFlow();
  }

  /**
   * Create an instance of {@link Replicate }
   * 
   */
  public Replicate createReplicate() {
    return new Replicate();
  }

  /**
   * Create an instance of {@link Attribute }
   * 
   */
  public Attribute createAttribute() {
    return new Attribute();
  }

  /**
   * Create an instance of {@link MetaInfo }
   * 
   */
  public MetaInfo createMetaInfo() {
    return new MetaInfo();
  }

  /**
   * Create an instance of {@link Datastore }
   * 
   */
  public Datastore createDatastore() {
    return new Datastore();
  }

  /**
   * Create an instance of {@link Configuration }
   * 
   */
  public Configuration createConfiguration() {
    return new Configuration();
  }

  /**
   * Create an instance of {@link CustomCode }
   * 
   */
  public CustomCode createCustomCode() {
    return new CustomCode();
  }

  /**
   * Create an instance of {@link Schema }
   * 
   */
  public Schema createSchema() {
    return new Schema();
  }

  /**
   * Create an instance of {@link Discovery }
   * 
   */
  public Discovery createDiscovery() {
    return new Discovery();
  }

  /**
   * Create an instance of {@link Import }
   * 
   */
  public Import createImport() {
    return new Import();
  }

  /**
   * Create an instance of {@link Property }
   * 
   */
  public Property createProperty() {
    return new Property();
  }

  /**
   * Create an instance of {@link Policy }
   * 
   */
  public Policy createPolicy() {
    return new Policy();
  }

  /**
   * Create an instance of {@link SourceDataDefinition }
   * 
   */
  public SourceDataDefinition createSourceDataDefinition() {
    return new SourceDataDefinition();
  }

  /**
   * Create an instance of {@link SubPartition }
   * 
   */
  public SubPartition createSubPartition() {
    return new SubPartition();
  }

  /**
   * Create an instance of {@link Target }
   * 
   */
  public Target createTarget() {
    return new Target();
  }

  /**
   * Create an instance of {@link Dataset }
   * 
   */
  public Dataset createDataset() {
    return new Dataset();
  }

  /**
   * Create an instance of {@link Defaults }
   * 
   */
  public Defaults createDefaults() {
    return new Defaults();
  }

  /**
   * Create an instance of {@link Transform }
   * 
   */
  public Transform createTransform() {
    return new Transform();
  }

  /**
   * Create an instance of {@link Parameters }
   * 
   */
  public Parameters createParameters() {
    return new Parameters();
  }

  /**
   * Create an instance of {@link JAXBElement }{@code <}{@link String }{@code >}
   * 
   */
  @XmlElementDecl(namespace = "uri:ivory:dataset:0.1", name = "Comments")
  public JAXBElement<String> createComments(String value) {
    return new JAXBElement<String>(_Comments_QNAME, String.class, null, value);
  }

  /**
   * Create an instance of {@link JAXBElement }{@code <}{@link String }{@code >}
   * 
   */
  @XmlElementDecl(namespace = "uri:ivory:dataset:0.1", name = "Tags")
  public JAXBElement<String> createTags(String value) {
    return new JAXBElement<String>(_Tags_QNAME, String.class, null, value);
  }

  /**
   * Create an instance of {@link JAXBElement }{@code <}{@link String }{@code >}
   * 
   */
  @XmlElementDecl(namespace = "uri:ivory:dataset:0.1", name = "Status")
  public JAXBElement<String> createStatus(String value) {
    return new JAXBElement<String>(_Status_QNAME, String.class, null, value);
  }

  /**
   * Create an instance of {@link JAXBElement }{@code <}{@link String }{@code >}
   * 
   */
  @XmlElementDecl(namespace = "uri:ivory:dataset:0.1", name = "IgnorePolicies")
  public JAXBElement<String> createIgnorePolicy(String value) {
    return new JAXBElement<String>(_IgnorePolicies_QNAME, String.class, null, value);
  }

  /**
   * Create an instance of {@link JAXBElement }{@code <}{@link String }{@code >}
   * 
   */
  @XmlElementDecl(namespace = "uri:ivory:dataset:0.1", name = "AuthScheme")
  public JAXBElement<String> createAuthScheme(String value) {
    return new JAXBElement<String>(_AuthScheme_QNAME, String.class, null, value);
  }

  /**
   * Create an instance of {@link JAXBElement }{@code <}{@link String }{@code >}
   * 
   */
  @XmlElementDecl(namespace = "uri:ivory:dataset:0.1", name = "Type")
  public JAXBElement<String> createType(String value) {
    return new JAXBElement<String>(_Type_QNAME, String.class, null, value);
  }

  /**
   * Create an instance of {@link JAXBElement }{@code <}{@link String }{@code >}
   * 
   */
  @XmlElementDecl(namespace = "uri:ivory:dataset:0.1", name = "OutputFormat")
  public JAXBElement<String> createOutputFormat(String value) {
    return new JAXBElement<String>(_OutputFormat_QNAME, String.class, null, value);
  }

  /**
   * Create an instance of {@link JAXBElement }{@code <}{@link String }{@code >}
   * 
   */
  @XmlElementDecl(namespace = "uri:ivory:dataset:0.1", name = "CredentialProvider")
  public JAXBElement<String> createCredentialProvider(String value) {
    return new JAXBElement<String>(_CredentialProvider_QNAME, String.class, null, value);
  }

  /**
   * Create an instance of {@link JAXBElement }{@code <}{@link String }{@code >}
   * 
   */
  @XmlElementDecl(namespace = "uri:ivory:dataset:0.1", name = "PartitionKey")
  public JAXBElement<String> createPartitionKey(String value) {
    return new JAXBElement<String>(_PartitionKey_QNAME, String.class, null, value);
  }

  /**
   * Create an instance of {@link JAXBElement }{@code <}{@link String }{@code >}
   * 
   */
  @XmlElementDecl(namespace = "uri:ivory:dataset:0.1", name = "SortKey")
  public JAXBElement<String> createSortKey(String value) {
    return new JAXBElement<String>(_SortKey_QNAME, String.class, null, value);
  }

  /**
   * Create an instance of {@link JAXBElement }{@code <}{@link String }{@code >}
   * 
   */
  @XmlElementDecl(namespace = "uri:ivory:dataset:0.1", name = "Priority")
  public JAXBElement<String> createPriority(String value) {
    return new JAXBElement<String>(_Priority_QNAME, String.class, null, value);
  }

  /**
   * Create an instance of {@link JAXBElement }{@code <}{@link String }{@code >}
   * 
   */
  @XmlElementDecl(namespace = "uri:ivory:dataset:0.1", name = "InputFormat")
  public JAXBElement<String> createInputFormat(String value) {
    return new JAXBElement<String>(_InputFormat_QNAME, String.class, null, value);
  }

}
