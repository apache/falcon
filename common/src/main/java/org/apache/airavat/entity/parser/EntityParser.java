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

package org.apache.airavat.entity.parser;

import org.apache.airavat.entity.Entity;
import org.apache.airavat.entity.EntityType;
import org.apache.log4j.Logger;

public abstract class EntityParser<T extends Entity> {

  private static Logger LOG = Logger.getLogger(EntityParser.class);

  private EntityType entityType;

  protected EntityParser(EntityType entityType) {
    this.entityType = entityType;
  }

  public EntityType getEntityType() {
    return entityType;
  }

  public Entity parse(String xml) {
    if (validateSchema(xml)) {
      T entity = doParse(xml);
      applyValidations(entity);
    }
    return null;
  }

  private boolean validateSchema(String xml) {
    //TODO use getEntityType to fetch xsd for validation
    return true;
  }

  protected abstract T doParse(String xml);

  protected abstract void applyValidations(T entity);

}
