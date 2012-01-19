/*
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

package org.apache.ivory.entity.v0;

import java.io.StringWriter;

import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import org.apache.ivory.Util;

public abstract class Entity {

  public abstract String getName();

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!o.getClass().equals(this.getClass())) return false;

    Entity entity = (Entity) o;

    String name = getName();
    if (name != null ? !name.equals(entity.getName()) : entity.getName() != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    String clazz = this.getClass().getName();

    String name = getName();
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + clazz.hashCode();
    return result;
  }

	@Override
	public String toString() {
		try {
			StringWriter stringWriter = new StringWriter();
			Marshaller marshaller = Util.getMarshaller(this.getClass());
			marshaller.marshal(this, stringWriter);
			return stringWriter.toString();
		} catch (JAXBException e) {
			return e.getMessage();
		}
	}
}
