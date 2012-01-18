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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlSchemaType;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import org.apache.ivory.entity.v0.datastore.adapter.OptionsMapAdapter;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(propOrder = { "arguments", "options" })
@XmlRootElement(name = "Command")
public class Command {

  @XmlAttribute(required = true)
  protected String name;

  @XmlAttribute(required = true)
  @XmlSchemaType(name = "anyURI")
  protected String url;

  @XmlAttribute
  protected String command;

  @XmlElementWrapper(name = "Arguments")
  @XmlElement(name = "Argument")
  protected List<String> arguments;

  @XmlJavaTypeAdapter(OptionsMapAdapter.class)
  @XmlElement(name = "Options")
  protected Map<String, String> options;

  @XmlTransient
  protected Map<String, String> headers;

  @XmlTransient
  protected InputStream inputStream;

  @XmlTransient
  protected long inputStreamLength;

  public String getName() {
    return name;
  }

  public void setName(String value) {
    name = value;
  }

  public String getUrl() {
    return url;
  }

  public void setUrl(String value) {
    url = value;
  }

  public String getCommand() {
    return command;
  }

  public void setCommand(String value) {
    command = value;
  }

  public List<String> getArguments() {
    return arguments;
  }

  public void setArguments(List<String> arguments) {
    this.arguments = arguments;
  }

  public Map<String, String> getOptions() {
    return options;
  }

  public void setOptions(Map<String, String> value) {
    options = value;
  }

  public Map<String, String> getHeaders() {
    return headers;
  }

  public void setHeaders(Map<String, String> headers) {
    this.headers = headers;
  }

  public InputStream getInputStream() {
    return inputStream;
  }

  public void setInputStream(InputStream inputStream) {
    this.inputStream = inputStream;
  }

  public long getInputStreamLength() {
    return inputStreamLength;
  }

  public void setInputStreamLength(long inputStreamLength) {
    this.inputStreamLength = inputStreamLength;
  }

  public void readFields(DataInput in) throws IOException {
    name = in.readUTF();
    url = in.readUTF();
    command = in.readUTF();

    int numArguments = in.readInt();
    if (numArguments != 0) {
      arguments = new ArrayList<String>();
      for (int i = 0; i < numArguments; i++) {
        arguments.add(in.readUTF());
      }
    }

    int numOptions = in.readInt();
    if (numOptions != 0) {
      options = new HashMap<String, String>();
      for (int i = 0; i < numOptions; i++) {
        options.put(in.readUTF(), in.readUTF());
      }
    }

  }

  public void write(DataOutput out) throws IOException {
    out.writeUTF(name);
    out.writeUTF(url);
    out.writeUTF(command);

    int numArguments = (arguments == null) ? 0 : arguments.size();
    out.writeInt(numArguments);
    if (arguments != null) {
      for (String arg : arguments) {
        out.writeUTF(arg);
      }
    }

    int numOptions = (options == null) ? 0 : options.size();
    out.writeInt(numOptions);
    if (options != null) {
      for (Entry<String, String> entry : options.entrySet()) {
        out.writeUTF(entry.getKey());
        out.writeUTF(entry.getValue());
      }
    }
  }
}
