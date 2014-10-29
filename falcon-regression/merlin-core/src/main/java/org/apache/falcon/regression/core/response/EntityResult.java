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

package org.apache.falcon.regression.core.response;

import org.apache.commons.lang.StringUtils;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

/** Class for api result of one entity. */
@XmlRootElement(name = "entity")
@XmlAccessorType(XmlAccessType.FIELD)
public class EntityResult {
    //SUSPEND CHECKSTYLE CHECK VisibilityModifierCheck
    @XmlElement
    public String type;
    @XmlElement
    public String name;
    @XmlElement
    public String status;
    @XmlElementWrapper(name = "list")
    public List<String> tag;
    @XmlElementWrapper(name = "list")
    public List<String> pipelines;
    //RESUME CHECKSTYLE CHECK VisibilityModifierCheck

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        String outString = "(" + type + ") " + name;
        if (StringUtils.isNotEmpty(status)) {
            outString += "(" + status + ")";
        }

        if (tag != null && !tag.isEmpty()) {
            outString += " - " + tag.toString();
        }

        if (pipelines != null && !pipelines.isEmpty()) {
            outString += " - " + pipelines.toString();
        }
        outString += "\n";
        return outString;
    }
}
