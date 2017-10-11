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
package org.apache.falcon.persistence;

import org.apache.openjpa.persistence.jdbc.Index;

import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import java.util.List;
//SUSPEND CHECKSTYLE CHECK  LineLengthCheck
/**
 * Entity object which will be stored in Data Base.
 */
@Entity
@NamedQueries({
        @NamedQuery(name = PersistenceConstants.GET_ENTITY, query = "select OBJECT(a) from EntityBean a where a.id = :id"),
        @NamedQuery(name = PersistenceConstants.GET_ENTITY_FOR_STATE, query = "select OBJECT(a) from EntityBean a where a.state = :state"),
        @NamedQuery(name = PersistenceConstants.UPDATE_ENTITY, query = "update EntityBean a set a.state = :state, a.name = :name, a.type = :type, a.properties = :properties where a.id = :id"),
        @NamedQuery(name = PersistenceConstants.GET_ENTITIES_FOR_TYPE, query = "select OBJECT(a) from EntityBean a where a.type = :type"),
        @NamedQuery(name = PersistenceConstants.GET_ENTITIES, query = "select OBJECT(a) from EntityBean a"),
        @NamedQuery(name = PersistenceConstants.DELETE_ENTITY, query = "delete from EntityBean a where a.id = :id"),
        @NamedQuery(name = PersistenceConstants.DELETE_ENTITIES, query = "delete from EntityBean")})
//RESUME CHECKSTYLE CHECK  LineLengthCheck
@Table(name = "ENTITIES")
public class EntityBean {
    @NotNull
    @Id
    private String id;

    @Basic
    @NotNull
    @Column(name = "name")
    private String name;


    @Basic
    @Index
    @NotNull
    @Column(name = "type")
    private String type;

    @Basic
    @Index
    @NotNull
    @Column(name = "current_state")
    private String state;

    @Column(name = "properties")
    @Lob
    private byte[] properties;

    @OneToMany(cascade= CascadeType.REMOVE, mappedBy="entityBean")
    private List<InstanceBean> instanceBeans;

    public EntityBean() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public List<InstanceBean> getInstanceBeans() {
        return instanceBeans;
    }

    public void setInstanceBeans(List<InstanceBean> instanceBeans) {
        this.instanceBeans = instanceBeans;
    }

    public byte[] getProperties() {
        return properties;
    }

    public void setProperties(byte[] properties) {
        this.properties = properties;
    }
}

