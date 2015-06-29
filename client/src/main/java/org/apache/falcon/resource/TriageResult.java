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

package org.apache.falcon.resource;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Resut for instance triage.
 */
@XmlRootElement(name = "result")
@XmlAccessorType(XmlAccessType.FIELD)
@edu.umd.cs.findbugs.annotations.SuppressWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
public class TriageResult extends APIResult {

    @XmlElement(name = "triageGraphs")
    private LineageGraphResult[] triageGraphs;

    //For JAXB
    private TriageResult() {
        super();
    }

    public TriageResult(Status status, String message) {
        super(status, message);
    }



    public LineageGraphResult[] getTriageGraphs() {
        return triageGraphs;
    }

    public void setTriageGraphs(LineageGraphResult[] triageGraphs) {
        this.triageGraphs = triageGraphs;
    }


    @Override
    public Object[] getCollection() {
        return getTriageGraphs();
    }


    @Override
    public void setCollection(Object[] items) {
        if (items == null) {
            setTriageGraphs(new LineageGraphResult[0]);
        } else {
            LineageGraphResult[] graphs = new LineageGraphResult[items.length];
            for (int index = 0; index < items.length; index++) {
                graphs[index] = (LineageGraphResult)items[index];
            }
            setTriageGraphs(graphs);
        }
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();
        if (triageGraphs != null) {
            for (LineageGraphResult graph : triageGraphs) {
                buffer.append(graph.getDotNotation());
                buffer.append("\n\n");
            }
        }
        return buffer.toString();
    }
}
