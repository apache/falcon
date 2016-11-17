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

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import java.util.Date;


//SUSPEND CHECKSTYLE CHECK LineLengthCheck
/**
 * Table to store extension metadata.
 */

@Table(name = "EXTENSION_METADATA")
@Entity
@NamedQueries({
        @NamedQuery(name = PersistenceConstants.GET_ALL_EXTENSIONS, query = "select OBJECT(a) from ExtensionMetadataBean a "),
        @NamedQuery(name = PersistenceConstants.GET_EXTENSION_LOCATION, query = "select a.location from ExtensionMetadataBean a where a.extensionName = :extensionName"),
        @NamedQuery(name = PersistenceConstants.DELETE_EXTENSIONS_OF_TYPE, query = "delete from ExtensionMetadataBean a where a.extensionType = :extensionType ")
})
//RESUME CHECKSTYLE CHECK  LineLengthCheck
public class ExtensionMetadataBean {
    @Basic
    @NotNull
    @Id
    @Column(name = "extension_name")
    private String extensionName;


    @Basic
    @NotNull
    @Column(name = "extension_type")
    private String extensionType;

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

    public String getExtensionType() {
        return extensionType;
    }

    public void setExtensionType(String extensionType) {
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
}
