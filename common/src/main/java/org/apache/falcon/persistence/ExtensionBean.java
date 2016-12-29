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

import org.apache.falcon.extensions.ExtensionStatus;
import org.apache.falcon.extensions.ExtensionType;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import java.util.Date;


//SUSPEND CHECKSTYLE CHECK LineLengthCheck
/**
 * Table to store extensions.
 */

@Table(name = "EXTENSIONS")
@Entity
@NamedQueries({
        @NamedQuery(name = PersistenceConstants.GET_ALL_EXTENSIONS, query = "select OBJECT(a) from ExtensionBean a "),
        @NamedQuery(name = PersistenceConstants.DELETE_EXTENSIONS_OF_TYPE, query = "delete from ExtensionBean a where a.extensionType = :extensionType "),
        @NamedQuery(name = PersistenceConstants.DELETE_EXTENSION, query = "delete from ExtensionBean a where a.extensionName = :extensionName "),
        @NamedQuery(name = PersistenceConstants.GET_EXTENSION, query = "select OBJECT(a) from ExtensionBean a where a.extensionName = :extensionName"),
        @NamedQuery(name = PersistenceConstants.CHANGE_EXTENSION_STATUS, query = "update ExtensionBean a set a.status = :extensionStatus where a.extensionName = :extensionName")
})
//RESUME CHECKSTYLE CHECK  LineLengthCheck
public class ExtensionBean {
    @Basic
    @NotNull
    @Id
    @Column(name = "extension_name")
    private String extensionName;

    @Basic
    @NotNull
    @Column(name = "extension_type")
    @Enumerated(EnumType.STRING)
    private ExtensionType extensionType;

    @Basic
    @Column(name = "description")
    private String description;

    @Basic
    @NotNull
    @Column(name = "location")
    private String location;

    @Basic
    @NotNull
    @Column(name = "creation_time")
    private Date creationTime;

    @Basic
    @NotNull
    @Column(name = "extension_owner")
    private String extensionOwner;

    @Basic
    @NotNull
    @Column(name = "status")
    @Enumerated(EnumType.STRING)
    private ExtensionStatus status;

    public ExtensionType getExtensionType() {
        return extensionType;
    }

    public void setExtensionType(ExtensionType extensionType) {
        this.extensionType = extensionType;
    }

    public Date getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(Date creationTime) {
        this.creationTime = creationTime;
    }

    public String getExtensionName() {
        return extensionName;
    }

    public void setExtensionName(String extensionName) {
        this.extensionName = extensionName;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getExtensionOwner() {
        return extensionOwner;
    }

    public void setExtensionOwner(String extensionOwner) {
        this.extensionOwner = extensionOwner;
    }

    public ExtensionStatus getStatus() {
        return status;
    }

    public void setStatus(ExtensionStatus status) {
        this.status = status;
    }

}
